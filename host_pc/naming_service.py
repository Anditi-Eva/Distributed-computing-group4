"""SQLite-backed hierarchical naming registry for the distributed slice gateway."""
import os
import re
import sqlite3
import threading
import time


PREFIX_TABLE = {
    "worker/": "worker",
    "service/": "service",
    "slice/": "slice",
    "process/": "process",
    "file/": "file",
}
_NAME_PART = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]*$")


def normalize_name(name):
    value = str(name or "").strip().strip("/")
    if value.startswith("urn:dcg4:process:"):
        value = "process/" + value.rsplit(":", 1)[-1]
    parts = value.split("/") if value else []
    if not parts or any(not _NAME_PART.fullmatch(part) for part in parts):
        raise ValueError("Names must be slash-separated alphanumeric, dot, dash, or underscore components")
    return "/".join(parts)


def namespace_for(name):
    for prefix in sorted(PREFIX_TABLE, key=len, reverse=True):
        if name.startswith(prefix):
            return PREFIX_TABLE[prefix]
    return "custom"


class NamingService:
    """Authoritative name server, with static worker names and dynamic resource bindings."""

    def __init__(self, db_path, static_workers):
        self.db_path = db_path
        self.static_workers = dict(static_workers)
        self.lock = threading.RLock()
        parent = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(parent, exist_ok=True)
        with self._connect() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS names (
                name TEXT PRIMARY KEY,
                resource_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                logical_uri TEXT NOT NULL,
                owner_node TEXT,
                state TEXT NOT NULL DEFAULT 'active',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )""")
            db.execute("CREATE INDEX IF NOT EXISTS names_resource_id ON names(resource_id)")
            db.execute("CREATE INDEX IF NOT EXISTS names_prefix ON names(name)")

    def _connect(self):
        db = sqlite3.connect(self.db_path, timeout=10)
        db.row_factory = sqlite3.Row
        return db

    def register_many(self, records):
        now = time.time()
        normalized = []
        seen = set()
        for record in records:
            name = normalize_name(record["name"])
            if name in seen:
                raise ValueError(f"duplicate name in request: {name}")
            seen.add(name)
            resource_id = str(record["resource_id"])
            kind = str(record.get("kind") or namespace_for(name))
            logical_uri = str(record.get("logical_uri") or f"/api/processes/{resource_id}")
            normalized.append((name, resource_id, kind, logical_uri,
                               record.get("owner_node"), "active", now, now))
        with self.lock, self._connect() as db:
            db.executemany("""INSERT INTO names
                (name,resource_id,kind,logical_uri,owner_node,state,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?)""", normalized)
        return [{"name": name, "resource_id": resource_id, "kind": kind,
                 "logical_uri": logical_uri, "owner_node": owner, "status": state,
                 "updated_at": now}
                for name, resource_id, kind, logical_uri, owner, state, _created, _updated in normalized]

    def resolve(self, name):
        normalized = normalize_name(name)
        if normalized.startswith("worker/"):
            worker_name = normalized.split("/", 1)[1]
            if worker_name in self.static_workers:
                return {"name": normalized, "kind": "worker", "resource_id": worker_name,
                        "logical_uri": f"/api/nodes/{worker_name}", "status": "static"}
        with self.lock, self._connect() as db:
            row = db.execute("SELECT * FROM names WHERE name=?", (normalized,)).fetchone()
        if row is None:
            return None
        return {"name": row["name"], "kind": row["kind"], "resource_id": row["resource_id"],
                "logical_uri": row["logical_uri"], "owner_node": row["owner_node"],
                "status": row["state"], "updated_at": row["updated_at"]}

    def update_owner(self, resource_id, owner_node, state="active"):
        with self.lock, self._connect() as db:
            db.execute("UPDATE names SET owner_node=?,state=?,updated_at=? WHERE resource_id=?",
                       (owner_node, state, time.time(), str(resource_id)))

    def set_state(self, resource_id, state):
        with self.lock, self._connect() as db:
            db.execute("UPDATE names SET state=?,updated_at=? WHERE resource_id=?",
                       (state, time.time(), str(resource_id)))

    def remove_resource(self, resource_id):
        with self.lock, self._connect() as db:
            db.execute("DELETE FROM names WHERE resource_id=?", (str(resource_id),))

    def list_names(self, prefix=""):
        normalized = normalize_name(prefix) if prefix else ""
        escaped = normalized.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        with self.lock, self._connect() as db:
            rows = db.execute("SELECT name,resource_id,kind,logical_uri,state FROM names "
                              "WHERE name LIKE ? ESCAPE '\\' ORDER BY name", (escaped + "%",)).fetchall()
        return [dict(row) for row in rows]

    def delete_prefix(self, prefix):
        normalized = normalize_name(prefix)
        escaped = normalized.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        with self.lock, self._connect() as db:
            cursor = db.execute("DELETE FROM names WHERE name LIKE ? ESCAPE '\\'", (escaped + "%",))
            return cursor.rowcount
