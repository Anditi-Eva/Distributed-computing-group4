"""Milestone 9/10 PROPOSED logical gateway (Baseline = transparency_gateway.py on :5100).

Same client API as the baseline (/api/slices, /api/processes, /api/names...), so the
same experiment drives both. Four engineering changes, each measured in
results/milestone9_10_proposed/RESULTS.md:

1. Proactive failure detector (M9 failure transparency).
   A heartbeat thread probes every worker each DELTA seconds. After K consecutive
   missed probes the node is SUSPECTED and its standby replicas are promoted on the
   survivors immediately, without waiting for a client to ask.
       detection bound:  T_detect <= DELTA + K * max(DELTA, TAU)   (TAU = probe timeout)
   (first probe starts within DELTA of the fault; each of the K missed probes lasts
   at most max(DELTA, TAU) because a tick waits for every probe to finish.)
   The baseline only notices a failure inside a client request (reactive).

2. Cached membership view + parallel fan-out (M9 overhead).
   Placement reads the heartbeat view instead of probing every worker per request,
   and the primary submit and the standby copy are sent in parallel, so admission
   latency is max(submit, replicate) instead of probe + replicate + submit + lookup.

3. Direct-owner routing (M10 name resolution).
   The name table stores the owner node. Locate/terminate go to the owner (and its
   standby) only: O(1) messages instead of an O(N) broadcast; broadcast is kept as
   the fallback when the owner hint is missing or wrong.

4. Cached naming + dynamic membership (M10 scalability).
   cached_naming.CachedNamingService answers hot names from memory; workers can be
   added at runtime via POST /api/workers/register (lease), no gateway restart.

Failure model: crash-stop and omission (hung) workers. The gateway itself is still a
single instance (see Limitations in RESULTS.md).
"""
from concurrent.futures import ThreadPoolExecutor
import os
import sqlite3
import threading
import time
import uuid

import requests
from requests.adapters import HTTPAdapter
from flask import Flask, jsonify, request

from cached_naming import CachedNamingService
from naming_service import PREFIX_TABLE

TIMEOUT = float(os.getenv("TRANSPARENCY_TIMEOUT", "1.5"))       # same as the baseline
DELTA = float(os.getenv("HEARTBEAT_INTERVAL", "0.25"))          # heartbeat period (s)
K_MISSES = int(os.getenv("SUSPECT_AFTER_MISSES", "2"))          # misses before suspicion
TAU = float(os.getenv("PROBE_TIMEOUT", "0.3"))                  # probe timeout (s)
LEASE_S = float(os.getenv("WORKER_LEASE_S", "10"))              # dynamic worker lease


def _parse_nodes(raw):
    nodes = {}
    for entry in (raw or "").split(","):
        if "=" in entry:
            name, url = entry.split("=", 1)
            nodes[name.strip()] = url.strip().rstrip("/")
    return nodes


STATIC_NODES = _parse_nodes(os.getenv(
    "TRANSPARENCY_NODES", "edge=http://127.0.0.1:5001,core=http://127.0.0.1:5002"))
nameserver = CachedNamingService(os.getenv("NAMING_DB", "/tmp/proposed_naming.sqlite3"), STATIC_NODES)
app = Flask(__name__)
pool = ThreadPoolExecutor(max_workers=32)
_tls = threading.local()


def _session():
    """One pooled keep-alive HTTP session per thread (the baseline opens a new connection per call)."""
    session = getattr(_tls, "session", None)
    if session is None:
        session = requests.Session()
        session.mount("http://", HTTPAdapter(pool_connections=16, pool_maxsize=32))
        _tls.session = session
    return session


def _call(method, url, timeout=TIMEOUT, **kwargs):
    counter = getattr(_tls, "messages", None)
    if counter is not None:
        counter[0] += 1
    return _session().request(method, url, timeout=timeout, **kwargs)


def _spawn(fn, *args, **kwargs):
    """Run fn on the shared pool while still counting its worker messages for this request."""
    counter = getattr(_tls, "messages", None)

    def run():
        _tls.messages = counter
        try:
            return fn(*args, **kwargs)
        finally:
            _tls.messages = None
    return pool.submit(run)


def _pmap(fn, items):
    return [f.result() for f in [_spawn(fn, item) for item in items]]


# =============================================================== failure detector
class Membership:
    """Heartbeat failure detector and cached membership view."""

    def __init__(self, static_nodes):
        self.lock = threading.RLock()
        self.nodes = {}            # name -> url
        self.dynamic = {}          # name -> lease seconds
        self.view = {}             # name -> state
        self.events = []           # failure / recovery timeline
        self.moved = {}            # failed node -> {pid: new owner}
        for name, url in static_nodes.items():
            self.add(name, url)

    def add(self, name, url, lease_s=None):
        with self.lock:
            self.nodes[name] = url.rstrip("/")
            if lease_s is not None:
                self.dynamic[name] = float(lease_s)
            self.view.setdefault(name, {"alive": True, "misses": 0, "status": {},
                                        "last_ok": time.time(), "suspected_at": None})

    def remove(self, name):
        with self.lock:
            self.nodes.pop(name, None)
            self.dynamic.pop(name, None)
            self.view.pop(name, None)
        nameserver.dynamic_workers.pop(name, None)

    def alive(self):
        with self.lock:
            return [(n, self.nodes[n], dict(self.view[n]["status"]))
                    for n in self.nodes if self.view[n]["alive"]]

    def is_alive(self, name):
        with self.lock:
            return name in self.nodes and self.view[name]["alive"]

    def url(self, name):
        with self.lock:
            return self.nodes.get(name)

    def reserve(self, name, cpu, memory):
        """Optimistic local accounting so a burst of creates between heartbeats still spreads out."""
        with self.lock:
            status = self.view.get(name, {}).get("status")
            if status:
                status["available_cpu"] = status.get("available_cpu", 0) - cpu
                status["available_memory"] = status.get("available_memory", 0) - memory
                status["queue_length"] = status.get("queue_length", 0) + 1

    def _probe(self, name, url):
        try:
            response = _session().get(f"{url}/api/status", timeout=TAU, headers={"X-Probe": "heartbeat"})
            return name, (response.json() if response.ok else None)
        except (requests.RequestException, ValueError):
            return name, None

    def tick(self):
        with self.lock:
            targets = list(self.nodes.items())
        for name, status in pool.map(lambda item: self._probe(*item), targets):
            failed = recovered = False
            with self.lock:
                state = self.view.get(name)
                if state is None:
                    continue
                now = time.time()
                if status is not None:
                    state.update(status=status, misses=0, last_ok=now)
                    if not state["alive"]:
                        state.update(alive=True, suspected_at=None)
                        recovered = True
                else:
                    state["misses"] += 1
                    if state["alive"] and state["misses"] >= K_MISSES:
                        state.update(alive=False, suspected_at=now)
                        failed = True
                    lease = self.dynamic.get(name)
                    if lease is not None and not state["alive"] and now - state["last_ok"] > lease:
                        self.events.append({"event": "lease_expired", "node": name, "at": now})
                        threading.Thread(target=self.remove, args=(name,), daemon=True).start()
            if failed:
                _on_failure(name)
            if recovered:
                _on_recovery(name)

    def run(self):
        while True:
            started = time.monotonic()
            try:
                self.tick()
            except Exception as exc:          # the detector must never die
                print(f"[proposed] heartbeat error: {exc}")
            time.sleep(max(0.0, DELTA - (time.monotonic() - started)))


members = Membership(STATIC_NODES)
ownership = {}          # resource_id -> {"owner": node, "standby": node | None}
ownership_lock = threading.Lock()
metrics = {"direct_owner_hits": 0, "broadcast_fallbacks": 0, "proactive_failovers": 0,
           "promoted_processes": 0, "fenced_duplicates": 0, "reprotected_replicas": 0}
metrics_lock = threading.Lock()


def _bump(key, amount=1):
    with metrics_lock:
        metrics[key] += amount


def _set_owner(resource_id, owner, standby=None, state="running"):
    with ownership_lock:
        ownership[resource_id] = {"owner": owner, "standby": standby}
    nameserver.update_owner(resource_id, owner, state)


def _on_failure(dead):
    """Promote the failed node's standby copies on the survivors immediately."""
    started = time.perf_counter()
    promoted = {}
    for name, base, _status in members.alive():
        try:
            response = _call("POST", f"{base}/api/replicas/promote", json={"failed_node": dead})
            if response.ok:
                for pid in response.json().get("promoted", []):
                    promoted[pid] = name
        except requests.RequestException:
            continue
    for pid, new_owner in promoted.items():
        _set_owner(pid, new_owner, standby=None)
    with members.lock:
        members.moved.setdefault(dead, {}).update(promoted)
        members.events.append({"event": "suspected_and_failed_over", "node": dead, "at": time.time(),
                               "promoted": sorted(promoted), "promote_ms":
                               round((time.perf_counter() - started) * 1000, 2)})
    _bump("proactive_failovers")
    _bump("promoted_processes", len(promoted))


def _on_recovery(name):
    """A suspected node answered again: fence its stale copies, then restore redundancy."""
    base = members.url(name)
    with members.lock:
        moved = members.moved.pop(name, {})
    fenced = reprotected = 0
    for pid, new_owner in moved.items():    # a hung (not crashed) node may still run old copies
        try:
            # Demote, not delete: the stale copy stops and becomes the new owner's standby.
            # (A crashed node restarts with empty memory, so this returns 404 there.)
            if _call("POST", f"{base}/api/process/{pid}/demote", json={"primary": new_owner}).ok:
                fenced += 1
                with ownership_lock:
                    if pid in ownership:
                        ownership[pid]["standby"] = name
                        reprotected += 1
        except requests.RequestException:
            pass
    with ownership_lock:
        unprotected = [(pid, row["owner"]) for pid, row in ownership.items()
                       if row["standby"] is None and row["owner"] != name]
    for pid, owner in unprotected:
        owner_url = members.url(owner)
        if not owner_url or not members.is_alive(owner):
            continue
        try:
            row = _call("GET", f"{owner_url}/api/process/{pid}").json()
            if row.get("status") != "running":
                continue
            copy = {k: v for k, v in row["process"].items() if not k.startswith("replica_")}
            if _call("POST", f"{base}/api/replica", json={"process": copy, "primary": owner}).ok:
                with ownership_lock:
                    ownership[pid]["standby"] = name
                reprotected += 1
        except (requests.RequestException, ValueError, KeyError):
            continue
    with members.lock:
        members.events.append({"event": "recovered", "node": name, "at": time.time(),
                               "fenced_duplicates": fenced, "reprotected_replicas": reprotected})
    _bump("fenced_duplicates", fenced)
    _bump("reprotected_replicas", reprotected)


# =============================================================== locate / create
def _broadcast(resource_id, unreachable=None):
    """Fallback: ask every live worker, prefer an active copy, promote a standby if needed.

    `unreachable` is an owner that just failed a direct call inside this request but that the
    heartbeat has not suspected yet; it is skipped (no second timeout) and treated as failed.
    """
    _bump("broadcast_fallbacks")
    nodes = [n for n in members.alive() if n[0] != unreachable]

    def ask(item):
        name, base, _s = item
        try:
            response = _call("GET", f"{base}/api/process/{resource_id}")
            return name, (response.json() if response.ok else None)
        except (requests.RequestException, ValueError):
            return name, None

    answers = [(n, row) for n, row in _pmap(ask, nodes) if row]
    active = next(((n, row) for n, row in answers if row.get("status") != "replicated"), None)
    if active is None and answers:
        holder = answers[0][0]
        record = ownership.get(resource_id, {})
        owner = record.get("owner")
        if owner and (owner == unreachable or not members.is_alive(owner)):
            try:
                promoted = _call("POST", f"{members.url(holder)}/api/replicas/promote",
                                 json={"failed_node": owner}).json().get("promoted", [])
            except (requests.RequestException, ValueError):
                promoted = []
            with members.lock:                 # remember the move so the old owner is fenced later
                members.moved.setdefault(owner, {}).update({pid: holder for pid in promoted})
            name, row = ask((holder, members.url(holder), None))
            if row and row.get("status") != "replicated":
                active = (name, row)
    chosen = active or (answers[0] if answers else None)
    if chosen:
        standby = next((n for n, row in answers if row.get("status") == "replicated" and n != chosen[0]), None)
        _set_owner(resource_id, chosen[0], standby, chosen[1].get("status", "running"))
    return chosen


def _locate(resource_id, owner_hint=None):
    """Direct-owner routing: one message to the recorded owner; broadcast only on a miss."""
    with ownership_lock:
        owner = (ownership.get(resource_id) or {}).get("owner") or owner_hint
    unreachable = None
    if owner and members.is_alive(owner):
        try:
            response = _call("GET", f"{members.url(owner)}/api/process/{resource_id}")
            if response.ok:
                row = response.json()
                if row.get("status") != "replicated":
                    _bump("direct_owner_hits")
                    return (owner, row), "direct_owner"
        except (requests.RequestException, ValueError):
            unreachable = owner
    return _broadcast(resource_id, unreachable), "broadcast_fallback"


def _create_process(payload):
    started = time.perf_counter()
    process = dict(payload)
    process.setdefault("pid", str(uuid.uuid4()))
    missing = [f for f in ("name", "cpu_needed", "memory_needed", "remaining_burst") if f not in process]
    if missing:
        return {"error": f"Missing fields: {', '.join(missing)}"}, 400
    pid = str(process["pid"])
    worker_process = {k: process[k] for k in ("pid", "name", "cpu_needed", "memory_needed",
                                              "remaining_burst", "arrival_time", "start_time")
                      if k in process}
    slice_type = str(process.get("slice_type", "generic")).lower().replace(" ", "-")
    aliases = [f"process/{pid}", f"slice/{slice_type}/{pid}"]
    if process.get("logical_name"):
        logical = str(process["logical_name"])
        logical = logical if "/" in logical else f"process/{logical}"
        if logical not in aliases:
            aliases.append(logical)

    t = time.perf_counter()
    try:
        nameserver.register_many([{"name": a, "resource_id": pid, "kind": "process"} for a in aliases])
    except (ValueError, sqlite3.IntegrityError) as exc:
        return {"error": f"Name registration failed: {exc}"}, 409
    naming_ms = (time.perf_counter() - t) * 1000

    t = time.perf_counter()
    nodes = [n for n in members.alive()
             if n[2].get("available_cpu", 0) >= process["cpu_needed"]
             and n[2].get("available_memory", 0) >= process["memory_needed"]] or members.alive()
    if not nodes:
        nameserver.remove_resource(pid)
        return {"error": "No worker is reachable"}, 503
    nodes.sort(key=lambda n: (n[2].get("queue_length", 0), -n[2].get("available_cpu", 0)))
    primary = nodes[0]
    standby = nodes[1] if len(nodes) > 1 else None
    members.reserve(primary[0], process["cpu_needed"], process["memory_needed"])
    schedule_ms = (time.perf_counter() - t) * 1000

    t = time.perf_counter()
    submit_f = _spawn(_call, "POST", f"{primary[1]}/api/submit", json=worker_process)
    replica_f = _spawn(_call, "POST", f"{standby[1]}/api/replica",
                            json={"process": worker_process, "primary": primary[0]}) if standby else None
    try:
        submitted = submit_f.result()
    except requests.RequestException:
        submitted = None
    replica_ok = False
    if replica_f is not None:
        try:
            replica_ok = replica_f.result().ok
        except requests.RequestException:
            replica_ok = False
    coordinate_ms = (time.perf_counter() - t) * 1000

    t = time.perf_counter()
    message = (submitted.json().get("message", "") if submitted is not None and submitted.ok else "")
    if submitted is not None and submitted.ok and not message.startswith("Offloaded"):
        _set_owner(pid, primary[0], standby[0] if replica_ok else None)
        located = True
    elif submitted is not None and submitted.ok:
        located = _broadcast(pid) is not None          # worker overflow moved it; find it once
    else:
        located = False
        if standby and replica_ok:                     # primary failed during admission
            try:
                _call("POST", f"{standby[1]}/api/replicas/promote", json={"failed_node": primary[0]})
                located = _broadcast(pid) is not None
            except requests.RequestException:
                pass
    locate_ms = (time.perf_counter() - t) * 1000
    if not located:
        nameserver.remove_resource(pid)
        return {"error": "No worker could admit the process"}, 503
    return {
        "status": "accepted", "resource_id": pid, "urn": f"urn:dcg4:process:{pid}",
        "name": aliases[0], "aliases": aliases, "logical_uri": f"/api/processes/{aliases[0]}",
        "replica_count": 1 if replica_ok else 0,
        "replication": "synchronous" if replica_ok else "degraded",
        "lifecycle": ["created", "named", "located", "scheduled", "coordinated", "accepted"],
        "phase_ms": {"name_resolution_registration": naming_ms, "schedule": schedule_ms,
                     "coordinate": coordinate_ms, "locate": locate_ms,
                     "total": (time.perf_counter() - started) * 1000},
    }, 201


# ======================================================================= HTTP API
@app.before_request
def _start_message_count():
    _tls.messages = [0]


@app.after_request
def _report_message_count(response):
    response.headers["X-Worker-Messages"] = str((getattr(_tls, "messages", None) or [0])[0])
    _tls.messages = None
    return response


@app.get("/health")
def health():
    return jsonify({"status": "up", "service": "proposed-logical-gateway",
                    "heartbeat": {"interval_s": DELTA, "suspect_after_misses": K_MISSES,
                                  "probe_timeout_s": TAU,
                                  "detection_bound_s": round(DELTA + K_MISSES * max(DELTA, TAU), 3)}})


@app.get("/api/metrics")
def gateway_metrics():
    with metrics_lock:
        counters = dict(metrics)
    return jsonify({"counters": counters, "cache": nameserver.cache_stats()})


@app.get("/api/membership")
def membership():
    with members.lock:
        view = {n: {"alive": s["alive"], "misses": s["misses"], "dynamic": n in members.dynamic,
                    "suspected_at": s["suspected_at"], "last_ok": s["last_ok"]}
                for n, s in members.view.items()}
        events = list(members.events[-50:])
    return jsonify({"nodes": view, "events": events})


@app.get("/api/nodes")
def logical_capacity():
    nodes = members.alive()
    return jsonify({"available_nodes": len(nodes), "total_nodes": len(members.nodes),
                    "available_cpu": sum(n[2].get("available_cpu", 0) for n in nodes),
                    "available_memory": sum(n[2].get("available_memory", 0) for n in nodes),
                    "logical_resource": "distributed-worker-pool"}), (200 if nodes else 503)


@app.get("/api/nodes/<node_name>")
def logical_node_status(node_name):
    if not members.url(node_name):
        return jsonify({"status": "not_found", "name": f"worker/{node_name}"}), 404
    alive = members.is_alive(node_name)
    return jsonify({"name": f"worker/{node_name}", "status": "available" if alive else "suspected",
                    "location": {"uri": f"/api/nodes/{node_name}", "scheme": "gateway-logical"}}), \
        (200 if alive else 503)


@app.post("/api/workers/register")
def register_worker():
    data = request.get_json(silent=True) or {}
    name, url = data.get("name"), data.get("url")
    if not name or not url:
        return jsonify({"error": "name and url are required"}), 400
    lease = float(data.get("lease_s", LEASE_S))
    members.add(str(name), str(url), lease_s=lease)
    nameserver.dynamic_workers[str(name)] = str(url)
    return jsonify({"status": "registered", "name": f"worker/{name}", "lease_s": lease}), 201


@app.delete("/api/workers/<name>")
def deregister_worker(name):
    members.remove(name)
    return jsonify({"status": "deregistered", "name": f"worker/{name}"})


@app.post("/api/slices")
@app.post("/api/processes")
def create_managed_process():
    process = request.get_json(silent=True)
    if not isinstance(process, dict):
        return jsonify({"status": "error", "message": "JSON process object required"}), 400
    result, code = _create_process(process)
    return jsonify(result), code


@app.get("/api/slices/<resource_id>")
def get_slice(resource_id):
    found, mode = _locate(resource_id)
    if not found:
        return jsonify({"status": "not_found", "resource_id": resource_id}), 404
    _owner, row = found
    response = jsonify({"resource_id": resource_id, "status": row.get("status"),
                        "process": row.get("process")})
    response.headers["X-Routing"] = mode           # routing mode, never the physical location
    return response


@app.post("/api/names/register")
def register_name():
    data = request.get_json(silent=True) or {}
    records = data.get("records") or [data]
    if not records or any(not r.get("name") or not r.get("resource_id") for r in records):
        return jsonify({"status": "error", "message": "name and resource_id are required"}), 400
    try:
        result = nameserver.register_many(records)
    except (ValueError, sqlite3.IntegrityError) as exc:
        return jsonify({"status": "conflict", "message": str(exc)}), 409
    if data.get("include_records", len(result) == 1):
        return jsonify({"status": "registered", "records": [
            {k: v for k, v in row.items() if k != "owner_node"} for row in result]}), 201
    return jsonify({"status": "registered", "count": len(result)}), 201


def _public(record, status):
    return {"name": record["name"], "kind": record["kind"], "resource_id": record["resource_id"],
            "status": status, "location": {"uri": record["logical_uri"], "scheme": "gateway-logical"}}


@app.get("/api/names/resolve/<path:name>")
def resolve_name(name):
    try:
        record = nameserver.resolve(name, bypass_cache=request.args.get("bypass_cache") == "1")
    except ValueError as exc:
        return jsonify({"status": "invalid_name", "message": str(exc)}), 400
    if not record:
        return jsonify({"status": "not_found", "name": name}), 404
    if record["kind"] not in ("process", "slice") or record.get("status") == "terminated":
        return jsonify(_public(record, record.get("status", "resolved")
                               if record["kind"] != "worker" else "resolved"))
    found, _mode = _locate(record["resource_id"], record.get("owner_node"))
    if not found:
        return jsonify({"status": "not_found", "name": record["name"]}), 404
    return jsonify(_public(record, found[1].get("status")))


@app.get("/api/names/prefixes")
def naming_prefixes():
    return jsonify({"prefix_table": PREFIX_TABLE,
                    "static_worker_names": [f"worker/{n}" for n in STATIC_NODES],
                    "leased_worker_names": [f"worker/{n}" for n in nameserver.dynamic_workers],
                    "cache": nameserver.cache_stats()})


@app.route("/api/names", methods=["GET", "DELETE"])
def names_collection():
    prefix = request.args.get("prefix", "")
    try:
        if request.method == "DELETE":
            return jsonify({"status": "deleted", "count": nameserver.delete_prefix(prefix)})
        return jsonify({"names": nameserver.list_names(prefix)})
    except ValueError as exc:
        return jsonify({"status": "invalid_name", "message": str(exc)}), 400


def _record_for(name):
    return nameserver.resolve(name if "/" in name or name.startswith("urn:") else f"process/{name}")


@app.get("/api/processes/<path:name>")
def locate_process(name):
    try:
        record = _record_for(name)
    except ValueError as exc:
        return jsonify({"status": "invalid_name", "message": str(exc)}), 400
    if not record:
        return jsonify({"status": "not_found", "name": name}), 404
    if record["kind"] not in ("process", "slice") or record.get("status") == "terminated":
        return jsonify(_public(record, record.get("status", "resolved")))
    found, mode = _locate(record["resource_id"], record.get("owner_node"))
    if not found:
        return jsonify({"status": "not_found", "name": name}), 404
    body = _public(record, found[1].get("status"))
    body["process"] = found[1].get("process")
    response = jsonify(body)
    response.headers["X-Routing"] = mode
    return response


@app.post("/api/processes/<path:name>/terminate")
def terminate_process(name):
    try:
        record = _record_for(name)
    except ValueError as exc:
        return jsonify({"status": "invalid_name", "message": str(exc)}), 400
    if not record or record["kind"] == "worker":
        return jsonify({"status": "not_found", "name": name}), 404
    rid = record["resource_id"]
    if record.get("status") == "terminated":
        return jsonify({"status": "already_terminated", "resource_id": rid, "coordination_acks": 0})
    with ownership_lock:
        row = dict(ownership.get(rid) or {})
    targets = [n for n in (row.get("owner"), row.get("standby")) if n and members.is_alive(n)]
    if not targets:                                    # no owner hint: fall back to broadcast
        targets = [n for n, _u, _s in members.alive()]

    def kill(node):
        try:
            return _call("DELETE", f"{members.url(node)}/api/process/{rid}").status_code == 200
        except requests.RequestException:
            return False

    acks = sum(_pmap(kill, targets))
    nameserver.set_state(rid, "terminated")
    with ownership_lock:
        ownership.pop(rid, None)
    if not acks:
        return jsonify({"status": "unavailable", "resource_id": rid}), 503
    return jsonify({"status": "terminated", "resource_id": rid, "coordination_acks": acks,
                    "lifecycle": ["located", "coordinated", "terminated"]})


if __name__ == "__main__":
    threading.Thread(target=members.run, name="heartbeat-failure-detector", daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5101")), threaded=True)
