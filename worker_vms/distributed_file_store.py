"""Persistent versioned object/file store used by edge and core workers."""
import hashlib
import os
import sqlite3
import threading
import time


class DistributedFileStore:
    def __init__(self, path):
        self.path = path
        self.lock = threading.RLock()
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with self._connect() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS files (
                path TEXT PRIMARY KEY, content BLOB, checksum TEXT NOT NULL,
                version INTEGER NOT NULL, deleted INTEGER NOT NULL DEFAULT 0,
                updated_at REAL NOT NULL
            )""")
            db.execute("""CREATE TABLE IF NOT EXISTS file_pending (
                tx_id TEXT PRIMARY KEY, path TEXT NOT NULL, content BLOB,
                checksum TEXT NOT NULL, expected_version INTEGER NOT NULL,
                new_version INTEGER NOT NULL, deleted INTEGER NOT NULL,
                updated_at REAL NOT NULL
            )""")
            db.execute("CREATE INDEX IF NOT EXISTS file_pending_path ON file_pending(path)")
            db.execute("""CREATE TABLE IF NOT EXISTS file_committed (
                tx_id TEXT PRIMARY KEY, path TEXT NOT NULL, version INTEGER NOT NULL,
                committed_at REAL NOT NULL
            )""")

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=15)
        db.row_factory = sqlite3.Row
        return db

    @staticmethod
    def checksum(content):
        return hashlib.sha256(content).hexdigest()

    def get(self, path):
        with self.lock, self._connect() as db:
            row = db.execute("SELECT * FROM files WHERE path=?", (path,)).fetchone()
        if row is None:
            return {"path": path, "content": b"", "checksum": self.checksum(b""),
                    "version": 0, "deleted": False, "found": False, "updated_at": None}
        return {"path": path, "content": bytes(row["content"] or b""),
                "checksum": row["checksum"], "version": row["version"],
                "deleted": bool(row["deleted"]), "found": not bool(row["deleted"]),
                "updated_at": row["updated_at"]}

    def prepare(self, tx_id, path, content, expected_version, deleted=False):
        content = b"" if deleted else bytes(content)
        checksum = self.checksum(content)
        with self.lock, self._connect() as db:
            pending = db.execute("SELECT * FROM file_pending WHERE tx_id=?", (tx_id,)).fetchone()
            if pending:
                if (pending["path"], bytes(pending["content"] or b""), pending["expected_version"],
                        bool(pending["deleted"])) != (path, content, int(expected_version), bool(deleted)):
                    raise ValueError("transaction ID is already used for a different file write")
                return {"status": "PREPARED", "new_version": pending["new_version"],
                        "checksum": pending["checksum"]}
            committed = db.execute("SELECT version FROM file_committed WHERE tx_id=?", (tx_id,)).fetchone()
            if committed:
                return {"status": "COMMITTED", "new_version": committed["version"]}
            current = db.execute("SELECT version FROM files WHERE path=?", (path,)).fetchone()
            actual = current["version"] if current else 0
            if actual != int(expected_version):
                raise RuntimeError(f"file version conflict: expected {expected_version}, found {actual}")
            conflict = db.execute("SELECT tx_id FROM file_pending WHERE path=? LIMIT 1", (path,)).fetchone()
            if conflict:
                raise RuntimeError("file has another pending write")
            version = actual + 1
            db.execute("INSERT INTO file_pending VALUES (?,?,?,?,?,?,?,?)",
                       (tx_id, path, sqlite3.Binary(content), checksum, actual, version,
                        int(deleted), time.time()))
        return {"status": "PREPARED", "new_version": version, "checksum": checksum}

    def commit(self, tx_id):
        with self.lock, self._connect() as db:
            committed = db.execute("SELECT path,version FROM file_committed WHERE tx_id=?", (tx_id,)).fetchone()
            if committed:
                current = self.get(committed["path"])
                return {"status": "COMMITTED", **current, "idempotent": True}
            pending = db.execute("SELECT * FROM file_pending WHERE tx_id=?", (tx_id,)).fetchone()
            if pending is None:
                raise KeyError("unknown prepared file transaction")
            current = db.execute("SELECT version FROM files WHERE path=?", (pending["path"],)).fetchone()
            actual = current["version"] if current else 0
            if actual != pending["expected_version"]:
                raise RuntimeError("prepared file changed before commit")
            db.execute("""INSERT INTO files(path,content,checksum,version,deleted,updated_at)
                VALUES (?,?,?,?,?,?) ON CONFLICT(path) DO UPDATE SET content=excluded.content,
                checksum=excluded.checksum,version=excluded.version,deleted=excluded.deleted,
                updated_at=excluded.updated_at""",
                       (pending["path"], pending["content"], pending["checksum"],
                        pending["new_version"], pending["deleted"], time.time()))
            db.execute("INSERT INTO file_committed VALUES (?,?,?,?)",
                       (tx_id, pending["path"], pending["new_version"], time.time()))
            db.execute("DELETE FROM file_pending WHERE tx_id=?", (tx_id,))
        return {"status": "COMMITTED", **self.get(pending["path"])}

    def abort(self, tx_id):
        with self.lock, self._connect() as db:
            db.execute("DELETE FROM file_pending WHERE tx_id=?", (tx_id,))
        return {"status": "ABORTED", "tx_id": tx_id}

    def install(self, path, content, version, deleted=False, checksum=None, updated_at=None):
        content = b"" if deleted else bytes(content)
        actual_checksum = self.checksum(content)
        if checksum and checksum != actual_checksum:
            raise ValueError("file checksum does not match content")
        with self.lock, self._connect() as db:
            current = db.execute("SELECT version,content,checksum,deleted FROM files WHERE path=?", (path,)).fetchone()
            current_version = current["version"] if current else 0
            if int(version) < current_version:
                return {"status": "STALE_IGNORED", "version": current_version}
            if int(version) == current_version and current is not None:
                if (bytes(current["content"] or b""), current["checksum"], bool(current["deleted"])) != \
                        (content, actual_checksum, bool(deleted)):
                    raise RuntimeError("same-version file replica mismatch")
                return {"status": "CURRENT", "version": current_version}
            db.execute("""INSERT INTO files(path,content,checksum,version,deleted,updated_at)
                VALUES (?,?,?,?,?,?) ON CONFLICT(path) DO UPDATE SET content=excluded.content,
                checksum=excluded.checksum,version=excluded.version,deleted=excluded.deleted,
                updated_at=excluded.updated_at""",
                       (path, sqlite3.Binary(content), actual_checksum, int(version), int(deleted),
                        updated_at or time.time()))
        return {"status": "INSTALLED", "version": int(version)}

    def list_files(self, prefix=""):
        with self.lock, self._connect() as db:
            rows = db.execute("SELECT path,checksum,version,deleted,updated_at,length(content) AS size "
                              "FROM files WHERE path LIKE ? ORDER BY path", (prefix + "%",)).fetchall()
        return [dict(row) for row in rows]
