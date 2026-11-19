"""Persistent versioned key-value store used as a replicated shared-state abstraction."""
import json
import os
import sqlite3
import threading
import time


class SharedStateStore:
    def __init__(self, path):
        self.path = path
        self.lock = threading.RLock()
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with self._connect() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS shared_values (
                key TEXT PRIMARY KEY, value_json TEXT, version INTEGER NOT NULL,
                deleted INTEGER NOT NULL DEFAULT 0, updated_at REAL NOT NULL
            )""")
            db.execute("""CREATE TABLE IF NOT EXISTS shared_pending (
                tx_id TEXT PRIMARY KEY, key TEXT NOT NULL, value_json TEXT,
                expected_version INTEGER NOT NULL, new_version INTEGER NOT NULL,
                deleted INTEGER NOT NULL, updated_at REAL NOT NULL
            )""")
            db.execute("CREATE INDEX IF NOT EXISTS pending_key ON shared_pending(key)")
            db.execute("""CREATE TABLE IF NOT EXISTS shared_committed (
                tx_id TEXT PRIMARY KEY, key TEXT NOT NULL, version INTEGER NOT NULL,
                committed_at REAL NOT NULL
            )""")

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=10, isolation_level="DEFERRED")
        db.row_factory = sqlite3.Row
        return db

    @staticmethod
    def _encode(value):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _row(row):
        if row is None:
            return {"version": 0, "value": None, "found": False, "deleted": False, "updated_at": None}
        deleted = bool(row["deleted"])
        return {"version": row["version"], "value": None if deleted else json.loads(row["value_json"]),
                "found": not deleted, "deleted": deleted, "updated_at": row["updated_at"]}

    def get(self, key):
        with self.lock, self._connect() as db:
            row = db.execute("SELECT * FROM shared_values WHERE key=?", (key,)).fetchone()
        return {"key": key, **self._row(row)}

    def prepare(self, tx_id, key, value, expected_version, deleted=False):
        encoded = None if deleted else self._encode(value)
        with self.lock, self._connect() as db:
            existing = db.execute("SELECT * FROM shared_pending WHERE tx_id=?", (tx_id,)).fetchone()
            if existing:
                same = (existing["key"] == key and existing["value_json"] == encoded and
                        existing["expected_version"] == expected_version and
                        bool(existing["deleted"]) == bool(deleted))
                if not same:
                    raise ValueError("transaction ID is already used for a different operation")
                return {"status": "PREPARED", "new_version": existing["new_version"]}
            committed = db.execute("SELECT version FROM shared_committed WHERE tx_id=?", (tx_id,)).fetchone()
            if committed:
                return {"status": "COMMITTED", "new_version": committed["version"]}
            current = db.execute("SELECT version FROM shared_values WHERE key=?", (key,)).fetchone()
            actual_version = current["version"] if current else 0
            if actual_version != int(expected_version):
                raise RuntimeError(f"version conflict: expected {expected_version}, found {actual_version}")
            conflict = db.execute("SELECT tx_id FROM shared_pending WHERE key=? LIMIT 1", (key,)).fetchone()
            if conflict:
                raise RuntimeError("key has another pending write")
            new_version = actual_version + 1
            db.execute("INSERT INTO shared_pending VALUES (?,?,?,?,?,?,?)",
                       (tx_id, key, encoded, actual_version, new_version, int(deleted), time.time()))
        return {"status": "PREPARED", "new_version": new_version}

    def commit(self, tx_id):
        with self.lock, self._connect() as db:
            committed = db.execute("SELECT key,version FROM shared_committed WHERE tx_id=?", (tx_id,)).fetchone()
            if committed:
                current = db.execute("SELECT * FROM shared_values WHERE key=?", (committed["key"],)).fetchone()
                return {"status": "COMMITTED", "key": committed["key"],
                        **self._row(current), "idempotent": True}
            pending = db.execute("SELECT * FROM shared_pending WHERE tx_id=?", (tx_id,)).fetchone()
            if pending is None:
                raise KeyError("unknown prepared transaction")
            current = db.execute("SELECT version FROM shared_values WHERE key=?", (pending["key"],)).fetchone()
            actual_version = current["version"] if current else 0
            if actual_version != pending["expected_version"]:
                raise RuntimeError("prepared state changed before commit")
            db.execute("""INSERT INTO shared_values(key,value_json,version,deleted,updated_at)
                VALUES (?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,
                version=excluded.version,deleted=excluded.deleted,updated_at=excluded.updated_at""",
                       (pending["key"], pending["value_json"], pending["new_version"],
                        pending["deleted"], time.time()))
            db.execute("INSERT INTO shared_committed VALUES (?,?,?,?)",
                       (tx_id, pending["key"], pending["new_version"], time.time()))
            db.execute("DELETE FROM shared_pending WHERE tx_id=?", (tx_id,))
            row = db.execute("SELECT * FROM shared_values WHERE key=?", (pending["key"],)).fetchone()
        return {"status": "COMMITTED", "key": pending["key"], **self._row(row)}

    def abort(self, tx_id):
        with self.lock, self._connect() as db:
            db.execute("DELETE FROM shared_pending WHERE tx_id=?", (tx_id,))
        return {"status": "ABORTED", "tx_id": tx_id}

    def install(self, key, value, version, deleted=False, updated_at=None):
        """Install a versioned replica/read-repair record if it is newer locally."""
        encoded = None if deleted else self._encode(value)
        with self.lock, self._connect() as db:
            current = db.execute("SELECT * FROM shared_values WHERE key=?", (key,)).fetchone()
            current_version = current["version"] if current else 0
            if int(version) < current_version:
                return {"status": "STALE_IGNORED", "version": current_version}
            if int(version) == current_version and current is not None:
                if current["value_json"] != encoded or bool(current["deleted"]) != bool(deleted):
                    raise RuntimeError("same-version replica value mismatch")
                return {"status": "CURRENT", "version": current_version}
            db.execute("""INSERT INTO shared_values(key,value_json,version,deleted,updated_at)
                VALUES (?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,
                version=excluded.version,deleted=excluded.deleted,updated_at=excluded.updated_at""",
                       (key, encoded, int(version), int(deleted), updated_at or time.time()))
        return {"status": "INSTALLED", "version": int(version)}

    def list_values(self, prefix=""):
        with self.lock, self._connect() as db:
            rows = db.execute("SELECT * FROM shared_values WHERE key LIKE ? ORDER BY key",
                              (prefix + "%",)).fetchall()
        return [{"key": row["key"], **self._row(row)} for row in rows]
