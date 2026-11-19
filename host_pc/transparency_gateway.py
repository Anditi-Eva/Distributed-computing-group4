"""Milestone 9 logical slice gateway.

Clients use /api/slices and stable resource IDs. Worker addresses, placement,
synchronous standby replication, migration and promotion stay behind this API.
Run with TRANSPARENCY_NODES='edge=http://edge:5001,core=http://core:5002'.
"""
import os
import json
import hashlib
import re
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

import requests
from flask import Flask, jsonify, request
from naming_service import PREFIX_TABLE, NamingService

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = int(os.getenv("MAX_FILE_BYTES", str(16 * 1024 * 1024)))
TIMEOUT = float(os.getenv("TRANSPARENCY_TIMEOUT", "1.5"))
MAX_FILE_BYTES = int(os.getenv("MAX_FILE_BYTES", str(16 * 1024 * 1024)))
NODES = {}
for entry in os.getenv(
    "TRANSPARENCY_NODES", "edge=http://127.0.0.1:5001,core=http://127.0.0.1:5002"
).split(","):
    if "=" in entry:
        name, url = entry.split("=", 1)
        NODES[name.strip()] = url.strip().rstrip("/")
NAMING_DB = os.getenv("NAMING_DB", "/tmp/naming.sqlite3")
nameserver = NamingService(NAMING_DB, NODES)
_rpc_local = threading.local()
_storage_local = threading.local()
_shared_write_lock = threading.RLock()


def _storage_begin():
    _storage_local.stats = [2, len(request.get_data(cache=True))]


def _storage_finish(response):
    stats = getattr(_storage_local, "stats", [0, 0])
    stats[1] += len(response.get_data())
    response.headers["X-Storage-Message-Count"] = str(stats[0])
    response.headers["X-Storage-Message-Bytes"] = str(stats[1])
    _storage_local.stats = None
    return response


def _request(method, url, **kwargs):
    try:
        response = requests.request(method, url, timeout=TIMEOUT, **kwargs)
        stats = getattr(_rpc_local, "stats", None)
        if stats is not None:
            request_bytes = len(json.dumps(kwargs.get("json"), separators=(",", ":")).encode()) \
                if kwargs.get("json") is not None else len(kwargs.get("data") or b"")
            stats[0] += 2
            stats[1] += request_bytes + len(response.content)
        storage_stats = getattr(_storage_local, "stats", None)
        if storage_stats is not None:
            request_bytes = len(json.dumps(kwargs.get("json"), separators=(",", ":")).encode()) \
                if kwargs.get("json") is not None else len(kwargs.get("data") or b"")
            storage_stats[0] += 2
            storage_stats[1] += request_bytes + len(response.content)
        return response
    except requests.RequestException:
        stats = getattr(_rpc_local, "stats", None)
        if stats is not None:
            stats[0] += 1
        storage_stats = getattr(_storage_local, "stats", None)
        if storage_stats is not None:
            storage_stats[0] += 1
        raise


def _worker_rpc(base, method, params=None):
    envelope = {"jsonrpc": "2.0", "id": str(uuid.uuid4()),
                "method": method, "params": params or {}}
    response = _request("POST", f"{base}/rpc", json=envelope)
    response.raise_for_status()
    message = response.json()
    if "error" in message:
        error = message["error"]
        raise RuntimeError(f"worker RPC {error.get('code')}: {error.get('message')}")
    return message["result"]


def _state_records(key):
    nodes = _healthy_nodes()
    if not nodes:
        raise RuntimeError("no worker is reachable")
    records = []
    for node_name, base, _status in nodes:
        records.append((node_name, base, _worker_rpc(base, "state.get", {"key": key})))
    highest = max(record[2]["version"] for record in records)
    newest = [record[2] for record in records if record[2]["version"] == highest]
    first = newest[0]
    if any((row["value"], row["deleted"]) != (first["value"], first["deleted"]) for row in newest[1:]):
        raise RuntimeError(f"consistency violation: conflicting replicas at version {highest}")
    for _name, base, row in records:
        if row["version"] < highest:
            _worker_rpc(base, "state.install", {"key": key, **first})
    first["replica_count"] = len(nodes)
    first["consistency"] = "linearizable-via-single-coordinator"
    return first, nodes


def _shared_state_write(key, value=None, expected_version=None, deleted=False):
    if not isinstance(key, str) or not key:
        raise ValueError("key must be a non-empty string")
    with _shared_write_lock:
        current, nodes = _state_records(key)
        current_version = current["version"]
        if expected_version is not None and int(expected_version) != current_version:
            raise RuntimeError(f"version conflict: expected {expected_version}, found {current_version}")
        tx_id = str(uuid.uuid4())
        prepared = []
        try:
            for node_name, base, _status in nodes:
                result = _worker_rpc(base, "state.prepare", {
                    "tx_id": tx_id, "key": key, "value": value,
                    "expected_version": current_version, "deleted": deleted,
                })
                if result.get("status") != "PREPARED":
                    raise RuntimeError(f"{node_name} did not prepare the state write")
                prepared.append((node_name, base))
        except Exception:
            for _name, base in prepared:
                try:
                    _worker_rpc(base, "state.abort", {"tx_id": tx_id})
                except Exception:
                    pass
            raise

        commits, failures = [], []
        for node_name, base in prepared:
            try:
                commits.append((node_name, _worker_rpc(base, "state.commit", {"tx_id": tx_id})))
            except Exception as exc:
                # Commit is idempotent; retry once to close transient response loss.
                try:
                    commits.append((node_name, _worker_rpc(base, "state.commit", {"tx_id": tx_id})))
                except Exception as retry_exc:
                    failures.append(f"{node_name}: {retry_exc or exc}")
        if failures:
            raise RuntimeError(f"state commit in doubt for transaction {tx_id}: {failures}")
        result = commits[0][1]
        result.update({"consistency": "linearizable-via-single-coordinator",
                       "replica_count": len(commits), "transaction_id": tx_id})
        return result


def _shared_state_read(key):
    # Readers wait for the coordinator's sequential replica commits to finish,
    # so they cannot observe a partially committed write through this API.
    with _shared_write_lock:
        current, _nodes = _state_records(key)
        return current


def _shared_state_list(prefix=""):
    with _shared_write_lock:
        nodes = _healthy_nodes()
        if not nodes:
            raise RuntimeError("no worker is reachable")
        combined = {}
        for _name, base, _status in nodes:
            response = _worker_rpc(base, "state.list", {"prefix": prefix})
            for row in response.get("entries", []):
                current = combined.get(row["key"])
                if current is None or row["version"] > current["version"]:
                    combined[row["key"]] = row
                elif row["version"] == current["version"] and \
                        (row["value"], row["deleted"]) != (current["value"], current["deleted"]):
                    raise RuntimeError(f"consistency violation for key {row['key']}")
        entries = [row for row in combined.values() if not row["deleted"]]
        return {"entries": sorted(entries, key=lambda row: row["key"]),
                "replica_count": len(nodes), "consistency": "linearizable-via-single-coordinator"}


def _healthy_nodes():
    healthy = []
    for name, base in NODES.items():
        try:
            response = _request("GET", f"{base}/api/status")
            if response.ok:
                healthy.append((name, base, response.json()))
        except requests.RequestException:
            continue
    return healthy


_FILE_COMPONENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


def _normalize_file_path(value):
    path = str(value or "").strip("/")
    parts = path.split("/")
    if (not path or len(path) > 512 or len(parts) > 16 or
            any(part in (".", "..") or not _FILE_COMPONENT.fullmatch(part) for part in parts)):
        raise ValueError("file path must contain 1-16 safe components (letters, numbers, . _ @ + -)")
    return path


def _internal_file_url(base, path, suffix=""):
    return f"{base}/internal/files/{quote(path, safe='/')}{suffix}"


def _file_records(path):
    nodes = _healthy_nodes()
    if not nodes:
        raise RuntimeError("no storage node is reachable")
    records = []
    for name, base, _status in nodes:
        response = _request("GET", _internal_file_url(base, path))
        response.raise_for_status()
        content = response.content
        checksum = response.headers.get("X-File-Checksum", hashlib.sha256(b"").hexdigest())
        if response.headers.get("X-File-Found") == "true" and hashlib.sha256(content).hexdigest() != checksum:
            raise RuntimeError(f"checksum mismatch for {path} on {name}")
        records.append({"name": name, "base": base, "content": content,
                        "checksum": checksum, "version": int(response.headers.get("X-File-Version", "0")),
                        "found": response.headers.get("X-File-Found") == "true",
                        "deleted": response.headers.get("X-File-Deleted") == "true",
                        "updated_at": response.headers.get("X-File-Updated-At") or None})
    highest = max(row["version"] for row in records)
    newest = [row for row in records if row["version"] == highest]
    winner = newest[0]
    if any((row["checksum"], row["deleted"]) != (winner["checksum"], winner["deleted"])
           for row in newest[1:]):
        raise RuntimeError(f"file consistency violation at version {highest}: {path}")
    repaired = 0
    for row in records:
        if row["version"] < highest:
            response = _request("PUT", _internal_file_url(row["base"], path, "/install"),
                                data=winner["content"], headers={
                                    "X-File-Version": str(highest),
                                    "X-File-Deleted": str(winner["deleted"]).lower(),
                                    "X-File-Checksum": winner["checksum"],
                                    "X-File-Updated-At": str(winner["updated_at"] or ""),
                                })
            response.raise_for_status()
            repaired += 1
    return winner, len(nodes), repaired


def _write_distributed_file(path, content=b"", expected_version=None, deleted=False):
    path = _normalize_file_path(path)
    if len(content) > MAX_FILE_BYTES:
        raise ValueError(f"file exceeds maximum size of {MAX_FILE_BYTES} bytes")
    with _shared_write_lock:
        current, replica_count, _repaired = _file_records(path)
        version = current["version"]
        if expected_version is not None and int(expected_version) != version:
            raise RuntimeError(f"file version conflict: expected {expected_version}, found {version}")
        tx_id = str(uuid.uuid4())
        headers = {"X-Transaction-ID": tx_id, "X-Expected-Version": str(version),
                   "X-File-Deleted": str(bool(deleted)).lower()}
        prepared = []
        try:
            for name, base, _status in _healthy_nodes():
                response = _request("PUT", _internal_file_url(base, path, "/prepare"),
                                    data=(b"" if deleted else content), headers=headers)
                if not response.ok:
                    raise RuntimeError(f"{name} rejected file prepare: {response.text}")
                prepared.append((name, base))
        except Exception:
            for _name, base in prepared:
                try:
                    _request("POST", f"{base}/internal/files/transactions/{tx_id}/abort")
                except requests.RequestException:
                    pass
            raise
        committed = []
        failures = []
        for name, base in prepared:
            url = f"{base}/internal/files/transactions/{tx_id}/commit"
            try:
                response = _request("POST", url, json={})
                if not response.ok:
                    raise RuntimeError(response.text)
                committed.append((name, response.json()))
            except Exception as exc:
                try:
                    response = _request("POST", url, json={})
                    response.raise_for_status()
                    committed.append((name, response.json()))
                except Exception:
                    failures.append(f"{name}: {exc}")
        if failures:
            raise RuntimeError(f"file transaction {tx_id} commit is in doubt: {failures}")
        if not committed:
            raise RuntimeError("file transaction had no commit acknowledgements")
        winner = committed[0][1]
        winner.update({"replica_count": len(committed), "transaction_id": tx_id,
                       "consistency": "synchronous all-reachable replicas via single gateway"})
        return winner


def _file_listing(prefix=""):
    prefix = str(prefix or "").strip("/")
    if prefix:
        prefix = _normalize_file_path(prefix)
    with _shared_write_lock:
        nodes = _healthy_nodes()
        if not nodes:
            raise RuntimeError("no storage node is reachable")
        combined = {}
        for _name, base, _status in nodes:
            response = _request("GET", f"{base}/internal/files", params={"prefix": prefix})
            response.raise_for_status()
            for row in response.json().get("files", []):
                old = combined.get(row["path"])
                if old is None or row["version"] > old["version"]:
                    combined[row["path"]] = row
                elif row["version"] == old["version"] and (row["checksum"], row["deleted"]) != \
                        (old["checksum"], old["deleted"]):
                    raise RuntimeError(f"file consistency violation at version {row['version']}: {row['path']}")
        return {"files": sorted((row for row in combined.values() if not row["deleted"]),
                                 key=lambda row: row["path"]),
                "replica_count": len(nodes), "consistency": "version-merged through single gateway"}


def _register_file_name(path):
    name = f"file/{path}"
    if nameserver.resolve(name) is None:
        nameserver.register_many([{"name": name, "resource_id": path, "kind": "file",
                                  "logical_uri": f"/api/files/{path}"}])
    else:
        nameserver.set_state(path, "active")


@app.put("/api/files/<path:file_path>")
def put_distributed_file(file_path):
    _storage_begin()
    try:
        file_path = _normalize_file_path(file_path)
        expected = request.headers.get("If-Match")
        if expected:
            expected = expected.strip('"')
        result = _write_distributed_file(file_path, request.get_data(), expected_version=expected)
        _register_file_name(file_path)
        return _storage_finish(jsonify({key: value for key, value in result.items() if key != "content"})), 201
    except ValueError as exc:
        return _storage_finish(jsonify({"status": "invalid_file", "message": str(exc)})), 400
    except RuntimeError as exc:
        return _storage_finish(jsonify({"status": "conflict_or_unavailable", "message": str(exc)})), 409
    except (requests.RequestException, sqlite3.Error) as exc:
        return _storage_finish(jsonify({"status": "storage_error", "message": str(exc)})), 503


@app.get("/api/files/<path:file_path>")
def get_distributed_file(file_path):
    _storage_begin()
    try:
        file_path = _normalize_file_path(file_path)
        with _shared_write_lock:
            row, replicas, repaired = _file_records(file_path)
        if not row["found"]:
            return _storage_finish(jsonify({"status": "not_found", "path": file_path,
                                            "version": row["version"], "deleted": row["deleted"]})), 404
        response = app.response_class(row["content"], mimetype="application/octet-stream")
        response.headers["ETag"] = f'"{row["version"]}"'
        response.headers["X-Resource-Version"] = str(row["version"])
        response.headers["X-Content-SHA256"] = row["checksum"]
        response.headers["X-Storage-Replicas"] = str(replicas)
        response.headers["X-Read-Repairs"] = str(repaired)
        return _storage_finish(response)
    except ValueError as exc:
        return _storage_finish(jsonify({"status": "invalid_file", "message": str(exc)})), 400
    except RuntimeError as exc:
        return _storage_finish(jsonify({"status": "storage_error", "message": str(exc)})), 503
    except requests.RequestException as exc:
        return _storage_finish(jsonify({"status": "storage_error", "message": str(exc)})), 503


@app.delete("/api/files/<path:file_path>")
def delete_distributed_file(file_path):
    _storage_begin()
    try:
        file_path = _normalize_file_path(file_path)
        expected = request.headers.get("If-Match")
        if expected:
            expected = expected.strip('"')
        result = _write_distributed_file(file_path, expected_version=expected, deleted=True)
        nameserver.set_state(file_path, "deleted")
        return _storage_finish(jsonify({key: value for key, value in result.items() if key != "content"}))
    except ValueError as exc:
        return _storage_finish(jsonify({"status": "invalid_file", "message": str(exc)})), 400
    except RuntimeError as exc:
        return _storage_finish(jsonify({"status": "conflict_or_unavailable", "message": str(exc)})), 409
    except (requests.RequestException, sqlite3.Error) as exc:
        return _storage_finish(jsonify({"status": "storage_error", "message": str(exc)})), 503


@app.get("/api/files")
def list_distributed_files():
    _storage_begin()
    try:
        return _storage_finish(jsonify(_file_listing(request.args.get("prefix", ""))))
    except (ValueError, RuntimeError) as exc:
        return _storage_finish(jsonify({"status": "storage_error", "message": str(exc)})), 503
    except requests.RequestException as exc:
        return _storage_finish(jsonify({"status": "storage_error", "message": str(exc)})), 503


@app.get("/api/storage/status")
def distributed_storage_status():
    nodes = _healthy_nodes()
    return jsonify({"storage": "replicated-worker-file-store", "healthy_nodes": [name for name, _, _ in nodes],
                    "configured_nodes": list(NODES), "replication_factor": len(nodes),
                    "degraded": len(nodes) < len(NODES), "maximum_file_bytes": MAX_FILE_BYTES,
                    "durability": "SQLite BLOBs on per-worker persistent Docker volumes"})


def _broadcast_lookup(resource_id):
    """Broadcast a name lookup to workers concurrently and repair owner cache."""
    outcomes, reachable, unavailable = [], [], 0

    def lookup(item):
        node_name, base = item
        response = _request("GET", f"{base}/api/process/{resource_id}")
        return node_name, base, response

    with ThreadPoolExecutor(max_workers=max(1, len(NODES))) as pool:
        futures = [pool.submit(lookup, (name, base)) for name, base in NODES.items()]
        for future in as_completed(futures):
            try:
                name, base, response = future.result()
                reachable.append((name, base))
                if response.ok:
                    outcomes.append((name, response.json()))
            except requests.RequestException:
                unavailable += 1

    active = next(((name, row) for name, row in outcomes if row.get("status") != "replicated"), None)
    replicas = [(name, row) for name, row in outcomes if row.get("status") == "replicated"]
    if active is None and replicas and unavailable:
        for _name, base in reachable:
            try:
                _request("POST", f"{base}/api/replicas/promote", json={})
            except requests.RequestException:
                pass
        with ThreadPoolExecutor(max_workers=max(1, len(reachable))) as pool:
            futures = [pool.submit(lookup, (name, base)) for name, base in reachable]
            for future in as_completed(futures):
                try:
                    name, _base, response = future.result()
                    if response.ok:
                        row = response.json()
                        if row.get("status") != "replicated":
                            active = (name, row)
                            break
                except requests.RequestException:
                    continue
    selected = active or (outcomes[0] if outcomes else None)
    if selected:
        node_name, row = selected
        nameserver.update_owner(resource_id, node_name, row.get("status", "active"))
        return row, node_name
    return None, None


def _resource_id_from_name(name):
    """Resolve a registered hierarchical name or accept a process ID/URN."""
    resolved = nameserver.resolve(name if "/" in name or name.startswith("urn:")
                                  else f"process/{name}")
    return resolved


def _create_process(payload):
    total_started = time.perf_counter()
    process = dict(payload)
    process.setdefault("pid", str(uuid.uuid4()))
    required = ("name", "cpu_needed", "memory_needed", "remaining_burst")
    missing = [field for field in required if field not in process]
    if missing:
        return {"error": f"Missing fields: {', '.join(missing)}"}, 400
    slice_config = None
    if process.get("config_path"):
        try:
            config_path = _normalize_file_path(process["config_path"])
            with _shared_write_lock:
                config_record, config_replicas, _ = _file_records(config_path)
            if not config_record["found"]:
                return {"error": f"Slice configuration file not found: {config_path}"}, 404
            process["config_path"] = config_path
            process["config_version"] = config_record["version"]
            process["config_checksum"] = config_record["checksum"]
            slice_config = {"path": config_path, "version": config_record["version"],
                            "checksum": config_record["checksum"], "replicas": config_replicas}
        except ValueError as exc:
            return {"error": str(exc)}, 400
        except (RuntimeError, requests.RequestException) as exc:
            return {"error": f"Slice configuration storage unavailable: {exc}"}, 503
    process_id = str(process["pid"])
    worker_process = {key: process[key] for key in
                      ("pid", "name", "cpu_needed", "memory_needed", "remaining_burst",
                       "arrival_time", "start_time", "config_path", "config_version",
                       "config_checksum") if key in process}
    slice_type = str(process.get("slice_type", "generic")).lower().replace(" ", "-")
    aliases = [f"process/{process_id}", f"slice/{slice_type}/{process_id}"]
    logical_name = process.get("logical_name")
    if logical_name:
        logical_name = str(logical_name)
        if "/" not in logical_name:
            logical_name = f"process/{logical_name}"
        if logical_name not in aliases:
            aliases.append(logical_name)
    create_ms = (time.perf_counter() - total_started) * 1000
    phase_started = time.perf_counter()
    try:
        nameserver.register_many([{"name": alias, "resource_id": process_id, "kind": "process"}
                                 for alias in aliases])
    except (ValueError, sqlite3.IntegrityError) as exc:
        return {"error": f"Name registration failed: {exc}"}, 409
    naming_ms = (time.perf_counter() - phase_started) * 1000

    phase_started = time.perf_counter()
    nodes = _healthy_nodes()
    locate_ms = (time.perf_counter() - phase_started) * 1000
    if not nodes:
        nameserver.remove_resource(process_id)
        return {"error": "No worker is reachable"}, 503
    phase_started = time.perf_counter()
    nodes.sort(key=lambda n: (n[2].get("queue_length", 0), -n[2].get("available_cpu", 0)))
    primary = nodes[0]
    schedule_ms = (time.perf_counter() - phase_started) * 1000
    phase_started = time.perf_counter()
    replicas = []
    for node_name, base, _status in nodes[1:]:
        try:
            response = _request("POST", f"{base}/api/replica", json={"process": worker_process})
            if response.ok:
                replicas.append(node_name)
        except requests.RequestException:
            pass

    try:
        submitted = _request("POST", f"{primary[1]}/api/submit", json=worker_process)
    except requests.RequestException:
        submitted = None
    if submitted is None or not submitted.ok:
        # A live replica is promoted when the selected worker failed during
        # admission; callers continue to address the same logical name.
        for node_name, base, _status in nodes[1:]:
            try:
                _request("POST", f"{base}/api/replicas/promote", json={})
            except requests.RequestException:
                continue
        located, owner = _broadcast_lookup(process_id)
        if not located or located.get("status") == "replicated":
            nameserver.remove_resource(process_id)
            return {"error": "No worker could admit the process"}, 503
        replicas = []
    else:
        pass
    coordinate_ms = (time.perf_counter() - phase_started) * 1000
    phase_started = time.perf_counter()
    if submitted is not None and submitted.ok:
        located, owner = _broadcast_lookup(process_id)
    final_locate_ms = (time.perf_counter() - phase_started) * 1000
    nameserver.update_owner(process_id, owner, located.get("status", "active") if located else "active")
    return {
        "status": "accepted", "resource_id": process_id,
        "urn": f"urn:dcg4:process:{process_id}",
        "name": aliases[0], "aliases": aliases,
        "logical_uri": f"/api/processes/{aliases[0]}",
        "replica_count": len(replicas),
        "replication": "synchronous" if replicas else "degraded",
        "slice_config": slice_config,
        "lifecycle": ["created", "named", "located", "scheduled", "coordinated", "accepted"],
        "phase_ms": {"create": create_ms, "name_resolution_registration": naming_ms,
                     "locate": locate_ms + final_locate_ms, "schedule": schedule_ms,
                     "coordinate": coordinate_ms,
                     "total": (time.perf_counter() - total_started) * 1000},
    }, 201


@app.get("/health")
def health():
    return jsonify({"status": "up", "service": "logical-slice-gateway"})


@app.get("/api/nodes")
def logical_capacity():
    """Report aggregate logical capacity without exposing physical node URLs."""
    nodes = _healthy_nodes()
    return jsonify({
        "available_nodes": len(nodes),
        "total_nodes": len(NODES),
        "available_cpu": sum(n[2].get("available_cpu", 0) for n in nodes),
        "available_memory": sum(n[2].get("available_memory", 0) for n in nodes),
        "logical_resource": "distributed-worker-pool",
    }), (200 if nodes else 503)


@app.get("/api/nodes/<node_name>")
def logical_node_status(node_name):
    """Resolve a configured worker name to its stable gateway location."""
    base = NODES.get(node_name)
    if not base:
        return jsonify({"status": "not_found", "name": f"worker/{node_name}"}), 404
    try:
        response = _request("GET", f"{base}/api/status")
        if not response.ok:
            raise requests.RequestException("worker returned an error")
        state = response.json()
    except requests.RequestException:
        return jsonify({"name": f"worker/{node_name}", "status": "unavailable",
                        "location": {"uri": f"/api/nodes/{node_name}", "scheme": "gateway-logical"}}), 503
    return jsonify({"name": f"worker/{node_name}", "status": "available",
                    "location": {"uri": f"/api/nodes/{node_name}", "scheme": "gateway-logical"},
                    "available_cpu": state.get("available_cpu"),
                    "available_memory": state.get("available_memory"),
                    "queue_length": state.get("queue_length")})


@app.post("/api/slices")
def create_slice():
    """Create a process through a logical resource ID; placement remains internal."""
    process = request.get_json(silent=True)
    if not isinstance(process, dict):
        return jsonify({"status": "error", "message": "JSON process object required"}), 400
    result, code = _create_process(process)
    return jsonify(result), code


@app.post("/api/processes")
def create_managed_process():
    """Milestone 10 entry point for Create → Locate → Schedule → Coordinate."""
    process = request.get_json(silent=True)
    if not isinstance(process, dict):
        return jsonify({"status": "error", "message": "JSON process object required"}), 400
    result, code = _create_process(process)
    return jsonify(result), code


@app.get("/api/slices/<resource_id>")
def get_slice(resource_id):
    """Resolve a stable logical ID across nodes, promoting replicas after failure."""
    located, _owner = _broadcast_lookup(resource_id)
    if located:
        if nameserver.resolve(f"process/{resource_id}") is None:
            try:
                nameserver.register_many([{"name": f"process/{resource_id}", "resource_id": resource_id}])
            except (ValueError, sqlite3.IntegrityError):
                pass
        return jsonify({"resource_id": resource_id, "status": located["status"],
                        "process": located.get("process")}), 200
    return jsonify({"status": "not_found", "resource_id": resource_id}), 404


@app.post("/api/names/register")
def register_name():
    data = request.get_json(silent=True) or {}
    try:
        records = data.get("records") or [data]
        if not records or any(not r.get("name") or not r.get("resource_id") for r in records):
            return jsonify({"status": "error", "message": "name and resource_id are required"}), 400
        result = nameserver.register_many(records)
    except (ValueError, sqlite3.IntegrityError) as exc:
        return jsonify({"status": "conflict", "message": str(exc)}), 409
    if data.get("include_records", len(result) == 1):
        public_records = [{key: value for key, value in row.items() if key != "owner_node"} for row in result]
        return jsonify({"status": "registered", "records": public_records}), 201
    return jsonify({"status": "registered", "count": len(result)}), 201


def _resolve_name(name):
    try:
        record = nameserver.resolve(name)
    except ValueError as exc:
        return None, (jsonify({"status": "invalid_name", "message": str(exc)}), 400)
    if not record:
        return None, (jsonify({"status": "not_found", "name": name}), 404)
    if record["kind"] == "worker":
        return {"name": record["name"], "kind": record["kind"],
                "resource_id": record["resource_id"], "status": "resolved",
                "location": {"uri": record["logical_uri"], "scheme": "gateway-logical"}}, None
    if record["kind"] == "file":
        return {"name": record["name"], "kind": record["kind"],
                "resource_id": record["resource_id"],
                "status": "deleted" if record.get("status") == "deleted" else "resolved",
                "location": {"uri": record["logical_uri"], "scheme": "gateway-logical"}}, None
    if record["kind"] not in ("process", "slice"):
        return {"name": record["name"], "kind": record["kind"],
                "resource_id": record["resource_id"], "status": "resolved",
                "location": {"uri": record["logical_uri"], "scheme": "gateway-logical"}}, None
    if record.get("status") == "terminated":
        return {"name": record["name"], "kind": record["kind"],
                "resource_id": record["resource_id"], "status": "terminated",
                "location": {"uri": record["logical_uri"], "scheme": "gateway-logical"}}, None
    located, _owner = _broadcast_lookup(record["resource_id"])
    state = located.get("status") if located else record.get("status", "not_found")
    nameserver.update_owner(record["resource_id"], _owner, state)
    if state == "not_found" or located is None:
        return None, (jsonify({"status": "not_found", "name": record["name"]}), 404)
    return {"name": record["name"], "kind": record["kind"],
            "resource_id": record["resource_id"], "status": state,
            "location": {"uri": record["logical_uri"], "scheme": "gateway-logical"}}, None


@app.get("/api/names/resolve/<path:name>")
def resolve_name(name):
    result, error = _resolve_name(name)
    return error if error else jsonify(result)


@app.get("/api/names/prefixes")
def naming_prefixes():
    return jsonify({"prefix_table": PREFIX_TABLE,
                    "static_worker_names": [f"worker/{name}" for name in NODES]})


@app.route("/api/names", methods=["GET", "DELETE"])
def names_collection():
    prefix = request.args.get("prefix", "")
    try:
        if request.method == "DELETE":
            return jsonify({"status": "deleted", "count": nameserver.delete_prefix(prefix)})
        return jsonify({"names": nameserver.list_names(prefix)})
    except ValueError as exc:
        return jsonify({"status": "invalid_name", "message": str(exc)}), 400


@app.get("/api/processes/<path:name>")
def locate_process(name):
    result, code = _locate_named_process(name)
    return jsonify(result), code


def _locate_named_process(name):
    try:
        record = _resource_id_from_name(name)
    except ValueError as exc:
        return {"status": "invalid_name", "message": str(exc)}, 400
    if not record:
        return {"status": "not_found", "name": name}, 404
    if record["kind"] == "worker":
        return {"name": record["name"], "status": "resolved",
                "location": {"uri": record["logical_uri"], "scheme": "gateway-logical"}}, 200
    if record["kind"] not in ("process", "slice"):
        return {"name": record["name"], "resource_id": record["resource_id"],
                "status": "resolved",
                "location": {"uri": record["logical_uri"], "scheme": "gateway-logical"}}, 200
    if record.get("status") == "terminated":
        return {"name": record["name"], "resource_id": record["resource_id"],
                "status": "terminated",
                "location": {"uri": record["logical_uri"], "scheme": "gateway-logical"}}, 200
    located, _owner = _broadcast_lookup(record["resource_id"])
    if not located:
        return {"status": "not_found", "name": name}, 404
    return {"name": record["name"], "resource_id": record["resource_id"],
            "status": located["status"], "process": located.get("process"),
            "location": {"uri": record["logical_uri"], "scheme": "gateway-logical"}}, 200


@app.post("/api/processes/<path:name>/terminate")
def terminate_managed_process(name):
    result, code = _terminate_named_process(name)
    return jsonify(result), code


def _terminate_named_process(name):
    try:
        record = _resource_id_from_name(name)
    except ValueError as exc:
        return {"status": "invalid_name", "message": str(exc)}, 400
    if not record or record["kind"] == "worker":
        return {"status": "not_found", "name": name}, 404
    if record.get("status") == "terminated":
        return {"status": "already_terminated", "resource_id": record["resource_id"],
                "coordination_acks": 0, "lifecycle": ["located", "terminated"]}, 200
    resource_id = record["resource_id"]
    acknowledgements = 0
    with ThreadPoolExecutor(max_workers=max(1, len(NODES))) as pool:
        futures = [pool.submit(_request, "DELETE", f"{base}/api/process/{resource_id}")
                   for base in NODES.values()]
        for future in as_completed(futures):
            try:
                response = future.result()
                acknowledgements += response.ok
            except requests.RequestException:
                pass
    nameserver.set_state(resource_id, "terminated")
    if not acknowledgements:
        return {"status": "unavailable", "resource_id": resource_id}, 503
    return {"status": "terminated", "resource_id": resource_id,
            "coordination_acks": acknowledgements,
            "lifecycle": ["located", "coordinated", "terminated"]}, 200


def _rpc_dispatch(method, params):
    if method == "rpc.ping":
        return {"status": "ok", "echo": params.get("echo"), "server_time": time.time()}
    if method == "worker.status":
        workers = []
        for name, base, _status in _healthy_nodes():
            workers.append(_worker_rpc(base, "worker.status"))
        return {"workers": workers, "available_nodes": len(workers), "total_nodes": len(NODES)}
    if method == "rpc.invoke":
        name = str(params.get("name", ""))
        record = nameserver.resolve(name)
        if not record or record.get("kind") != "worker":
            raise ValueError("rpc.invoke requires a registered worker name such as worker/edge")
        base = NODES.get(record["resource_id"])
        if not base:
            raise KeyError("named worker is not configured")
        remote_method = str(params.get("method", ""))
        if remote_method not in ("system.ping", "worker.status", "process.submit",
                                 "process.get", "process.terminate", "state.get", "state.list"):
            raise ValueError("remote method is not in the worker RPC allowlist")
        return _worker_rpc(base, remote_method, params.get("params") or {})
    if method == "process.create":
        result, code = _create_process(params)
        if code >= 400:
            raise RuntimeError(result.get("error", "process creation failed"))
        return result
    if method == "process.locate":
        result, code = _locate_named_process(str(params.get("name", "")))
        if code >= 400:
            raise KeyError(result.get("message", result.get("status", "not found")))
        return result
    if method == "process.terminate":
        result, code = _terminate_named_process(str(params.get("name", "")))
        if code >= 400:
            raise RuntimeError(result.get("status", "termination failed"))
        return result
    if method == "state.get":
        return _shared_state_read(str(params.get("key", "")))
    if method == "state.put":
        if "value" not in params:
            raise ValueError("state.put requires a value")
        return _shared_state_write(str(params.get("key", "")), params["value"],
                                   params.get("expected_version"), deleted=False)
    if method == "state.delete":
        return _shared_state_write(str(params.get("key", "")), expected_version=params.get("expected_version"),
                                   deleted=True)
    if method == "state.list":
        return _shared_state_list(str(params.get("prefix", "")))
    raise LookupError(f"unknown RPC method: {method}")


@app.post("/rpc/call")
def gateway_rpc_server_stub():
    """JSON-RPC 2.0 server stub; client and worker stubs exchange JSON over HTTP."""
    envelope = request.get_json(silent=True)
    if not isinstance(envelope, dict) or envelope.get("jsonrpc") != "2.0" or not envelope.get("id"):
        return jsonify({"jsonrpc": "2.0", "id": envelope.get("id") if isinstance(envelope, dict) else None,
                        "error": {"code": -32600, "message": "invalid JSON-RPC request"}}), 400
    method, params, request_id = envelope.get("method"), envelope.get("params") or {}, envelope["id"]
    if not isinstance(method, str) or not isinstance(params, dict):
        return jsonify({"jsonrpc": "2.0", "id": request_id,
                        "error": {"code": -32602, "message": "method and object params are required"}}), 400

    incoming = len(request.get_data())
    _rpc_local.stats = [2, incoming]  # one client request and one response
    try:
        result = _rpc_dispatch(method, params)
        reply = {"jsonrpc": "2.0", "id": request_id, "result": result}
    except LookupError as exc:
        reply = {"jsonrpc": "2.0", "id": request_id,
                 "error": {"code": -32601, "message": str(exc)}}
    except KeyError as exc:
        reply = {"jsonrpc": "2.0", "id": request_id,
                 "error": {"code": -32004, "message": str(exc)}}
    except ValueError as exc:
        reply = {"jsonrpc": "2.0", "id": request_id,
                 "error": {"code": -32602, "message": str(exc)}}
    except Exception as exc:
        reply = {"jsonrpc": "2.0", "id": request_id,
                 "error": {"code": -32000, "message": str(exc)}}
    _rpc_local.stats[1] += len(json.dumps(reply, separators=(",", ":")).encode())
    reply["meta"] = {"message_count": _rpc_local.stats[0],
                     "message_bytes": _rpc_local.stats[1]}
    _rpc_local.stats = None
    return jsonify(reply)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5100")), threaded=True)
