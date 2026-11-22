"""Milestone 9/10 PROPOSED naming layer.

A write-through LRU + TTL cache in front of the baseline SQLite name server
(`naming_service.NamingService`). The baseline file is not modified, so the
Baseline vs Proposed comparison measures only what this module adds:

1. Resolution cache: hot names are answered from memory (O(1) dict lookup)
   instead of opening SQLite and running a query on every resolve.
2. Write-through coherence: this gateway is the only writer of its registry,
   so every write (register / owner change / state change / delete) updates or
   invalidates the cached entry in the same call. The TTL is a safety net, not
   the coherence mechanism.
3. Connection reuse: one SQLite connection per thread instead of one per call.
4. Dynamic worker leases: `worker/<name>` resolves for workers that registered
   at runtime, not only for the static map configured at start-up.
"""
from collections import OrderedDict
import sqlite3
import threading
import time

from naming_service import NamingService, normalize_name


class CachedNamingService(NamingService):
    def __init__(self, db_path, static_workers, maxsize=50000, ttl_s=60.0):
        self._tls = threading.local()
        super().__init__(db_path, static_workers)
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=NORMAL")
        self.maxsize = int(maxsize)
        self.ttl_s = float(ttl_s)
        self._cache = OrderedDict()   # name -> (expires_at, record)
        self._by_resource = {}        # resource_id -> set(names)
        self._cache_lock = threading.Lock()
        self.hits = self.misses = self.evictions = 0
        self.dynamic_workers = {}     # worker name -> base URL (leases managed by the gateway)

    # ------------------------------------------------------------ connection reuse
    def _connect(self):
        db = getattr(self._tls, "db", None)
        if db is None:
            db = sqlite3.connect(self.db_path, timeout=10)
            db.row_factory = sqlite3.Row
            self._tls.db = db
        return db

    # ------------------------------------------------------------------- cache core
    def _put(self, name, record):
        with self._cache_lock:
            self._cache[name] = (time.monotonic() + self.ttl_s, dict(record))
            self._cache.move_to_end(name)
            self._by_resource.setdefault(str(record.get("resource_id")), set()).add(name)
            while len(self._cache) > self.maxsize:
                old_name, (_exp, old) = self._cache.popitem(last=False)
                self._by_resource.get(str(old.get("resource_id")), set()).discard(old_name)
                self.evictions += 1

    def _drop(self, name):
        entry = self._cache.pop(name, None)
        if entry:
            self._by_resource.get(str(entry[1].get("resource_id")), set()).discard(name)

    def _patch_resource(self, resource_id, **changes):
        with self._cache_lock:
            for name in list(self._by_resource.get(str(resource_id), ())):
                entry = self._cache.get(name)
                if entry:
                    entry[1].update(changes)

    # --------------------------------------------------------------- read path
    def resolve(self, name, bypass_cache=False):
        normalized = normalize_name(name)
        if normalized.startswith("worker/"):
            worker = normalized.split("/", 1)[1]
            if worker in self.dynamic_workers:
                return {"name": normalized, "kind": "worker", "resource_id": worker,
                        "logical_uri": f"/api/nodes/{worker}", "status": "leased"}
            return super().resolve(normalized)
        if not bypass_cache:
            with self._cache_lock:
                entry = self._cache.get(normalized)
                if entry is not None and entry[0] > time.monotonic():
                    self._cache.move_to_end(normalized)
                    self.hits += 1
                    return dict(entry[1])
                if entry is not None:
                    self._drop(normalized)          # expired
                self.misses += 1
        record = super().resolve(normalized)
        if record is not None:
            self._put(normalized, record)
        return record

    # -------------------------------------------------------------- write path
    def register_many(self, records):
        created = super().register_many(records)
        for row in created:
            self._put(row["name"], row)
        return created

    def update_owner(self, resource_id, owner_node, state="active"):
        super().update_owner(resource_id, owner_node, state)
        self._patch_resource(resource_id, owner_node=owner_node, status=state, updated_at=time.time())

    def set_state(self, resource_id, state):
        super().set_state(resource_id, state)
        self._patch_resource(resource_id, status=state, updated_at=time.time())

    def remove_resource(self, resource_id):
        super().remove_resource(resource_id)
        with self._cache_lock:
            for name in list(self._by_resource.pop(str(resource_id), ())):
                self._cache.pop(name, None)

    def delete_prefix(self, prefix):
        removed = super().delete_prefix(prefix)
        normalized = normalize_name(prefix)
        with self._cache_lock:
            for name in [n for n in self._cache if n.startswith(normalized)]:
                self._drop(name)
        return removed

    def cache_stats(self):
        with self._cache_lock:
            total = self.hits + self.misses
            return {"entries": len(self._cache), "maxsize": self.maxsize, "ttl_s": self.ttl_s,
                    "hits": self.hits, "misses": self.misses, "evictions": self.evictions,
                    "hit_rate": round(self.hits / total, 4) if total else 0.0}
