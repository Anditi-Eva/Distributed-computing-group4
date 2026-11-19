import os
import sys
import time
import threading
import requests
from flask import Flask, request, jsonify
from prometheus_client import start_http_server, Gauge, Counter, Histogram
from dataclasses import dataclass, asdict
from shared_state import SharedStateStore
from distributed_file_store import DistributedFileStore
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import uuid
from fault_injection import FaultInjector as WorkerFaultInjector

# ==========================================
# MILESTONE 2: Prometheus Metrics
# ==========================================
CPU_UTILIZATION = Gauge('node_cpu_utilization', 'CPU utilization percentage')
MEM_UTILIZATION = Gauge('node_memory_utilization', 'Memory utilization percentage')
PROCESSES_COMPLETED = Counter('node_processes_completed_total', 'Total processes completed')
MIGRATED_PROCESSES = Counter('node_processes_migrated_total', 'Processes offloaded to other nodes')
FILE_REPLICAS_COMMITTED = Counter('node_file_replicas_committed_total', 'File replicas committed on this worker')
FILE_REPLICA_BYTES = Counter('node_file_replica_bytes_total', 'File bytes committed on this worker')

MIGRATION_LATENCY = Histogram('node_migration_latency_seconds', 'Time taken to offload a process to a peer')
API_LATENCY = Histogram(
    'node_api_latency_seconds', 
    'Time taken for a process to complete after submission', 
    buckets=(1.0, 2.0, 5.0, 10.0, 20.0, float("inf"))
)

# Deployment knobs (Milestone 8). Defaults keep the original behaviour.
PROM_PORT = int(os.environ.get("PROM_PORT", "8000"))      # metrics port (change to run 2 nodes on one PC)
SCHED_TICK = float(os.environ.get("SCHED_TICK", "1.0"))   # seconds per scheduling quantum
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "distributed.process-events")
kafka_producer = None
shared_state = SharedStateStore(os.getenv("SHARED_STATE_DB", "/tmp/shared-state.sqlite3"))
file_store = DistributedFileStore(os.getenv("FILE_STORE_DB", "/tmp/distributed-files.sqlite3"))

def publish_event(event_type, payload):
    """Publish lifecycle events without making worker availability depend on Kafka."""
    if kafka_producer is None:
        return
    event = {"event": event_type, "timestamp": time.time(), **payload}
    try:
        kafka_producer.send(KAFKA_TOPIC, event).add_errback(
            lambda exc: print(f"Kafka publish failed: {exc}")
        )
    except KafkaError as exc:
        print(f"Kafka publish failed: {exc}")

# ==========================================
# MILESTONE 1 & 2: Models and Resource Manager
# ==========================================
@dataclass
class Process:
    pid: str
    name: str
    cpu_needed: int
    memory_needed: int
    remaining_burst: int
    arrival_time: float = 0.0  
    start_time: float = 0.0    
    config_path: str = ""
    config_version: int = 0
    config_checksum: str = ""

class ResourceManager:
    def __init__(self, cpu, memory):
        self.total_cpu = cpu
        self.total_memory = memory
        self.avail_cpu = cpu
        self.avail_memory = memory
        self.lock = threading.Lock()

    def allocate(self, cpu, memory) -> bool:
        with self.lock:
            if self.avail_cpu >= cpu and self.avail_memory >= memory:
                self.avail_cpu -= cpu
                self.avail_memory -= memory
                return True
            return False

    def release(self, cpu, memory):
        with self.lock:
            self.avail_cpu += cpu
            self.avail_memory += memory

    def snapshot(self):
        with self.lock:
            return self.avail_cpu, self.avail_memory

    def reset(self):
        with self.lock:
            self.avail_cpu, self.avail_memory = self.total_cpu, self.total_memory

    def update_metrics(self):
        CPU_UTILIZATION.set(((self.total_cpu - self.avail_cpu) / self.total_cpu) * 100)
        MEM_UTILIZATION.set(((self.total_memory - self.avail_memory) / self.total_memory) * 100)

# ==========================================
# MILESTONE 1, 2, & 3: Distributed Worker Node
# ==========================================
class WorkerNode:
    def __init__(self, node_id, port, cpu, memory, peer_ip, peer_port):
        self.node_id = node_id
        self.incarnation = uuid.uuid4().hex
        self.port = port
        self.peer_url = f"http://{peer_ip}:{peer_port}"
        self.resource_manager = ResourceManager(cpu, memory)
        self.ready_queue = []
        self.completed_queue = []
        self.completion_log = []   # per-process results for the Milestone 8 experiments
        self.shadow_processes = {} # synchronous standby copies; never scheduled until promoted
        self.terminated_processes = {}
        self.rejected = 0
        self.lock = threading.Lock()
        
        # Start Round-Robin Scheduler background thread (Milestone 1)
        threading.Thread(target=self._scheduler_loop, daemon=True).start()

    def submit_process(self, process_data, is_migrated=False):
        start_network_time = time.time()
        try:
            p = Process(**process_data)
        except TypeError:
            return False, "Invalid process payload"

        if p.arrival_time == 0.0:
            p.arrival_time = time.time()

        # A mirrored or retried request must not execute twice on the same worker.
        with self.lock:
            if p.pid in self.terminated_processes:
                return False, "Process has been terminated"
            if any(x.pid == p.pid for x in self.ready_queue + self.completed_queue):
                return True, "Process already present"

        # 1. Try to allocate locally
        if self.resource_manager.allocate(p.cpu_needed, p.memory_needed):
            with self.lock:
                self.ready_queue.append(p)
                if is_migrated:
                    self.shadow_processes.pop(p.pid, None)
            self.resource_manager.update_metrics()
            publish_event("process.accepted", {"node_id": self.node_id, "process": asdict(p)})
            
            # Record API Latency
            API_LATENCY.observe(time.time() - start_network_time)
            return True, "Process admitted locally"
        
        # 2. Resource Allocation Failure -> Inter-node Communication (Milestone 2)
        if not is_migrated:
            print(f"[{self.node_id}] Resource full. Offloading '{p.name}' to {self.peer_url}...")
            try:
                offload_start = time.time()
                # Send to Peer via REST
                res = requests.post(f"{self.peer_url}/api/migrate", json={"process": asdict(p)}, timeout=3)
                if res.status_code == 200:
                    MIGRATED_PROCESSES.inc()
                    MIGRATION_LATENCY.observe(time.time() - offload_start) 
                    return True, f"Offloaded to peer {self.peer_url}"
            except requests.exceptions.RequestException:
                pass 
            self.rejected += 1
            return False, "Resource denied (Local full, peer unreachable)"

        self.rejected += 1
        return False, "Resource denied (Migrated process rejected)"

    def _scheduler_loop(self):
        """Round-Robin Scheduler Simulation (Milestone 1)"""
        while True:
            time.sleep(SCHED_TICK) # Scheduling tick (1 s by default)
            with self.lock:
                if not self.ready_queue:
                    continue
                
                p = self.ready_queue.pop(0)
                
                if p.start_time == 0.0:
                    p.start_time = time.time()
                
                # Execute for 2 units of burst time
                p.remaining_burst -= 2
                
                if p.remaining_burst <= 0:
                    self.resource_manager.release(p.cpu_needed, p.memory_needed)
                    self.completed_queue.append(p)
                    PROCESSES_COMPLETED.inc()
                    
                    # Calculate Total Processing Latency
                    total_latency = time.time() - p.arrival_time
                    API_LATENCY.observe(total_latency)
                    self.completion_log.append({"pid": p.pid, "name": p.name,
                                                "latency_s": total_latency})
                    publish_event("process.completed", {"node_id": self.node_id, "pid": p.pid,
                                                          "name": p.name, "latency_s": total_latency})
                    threading.Thread(target=self._notify_replica_completed,
                                     args=(p.pid, total_latency), daemon=True).start()
                    
                    print(f"[{self.node_id}] COMPLETED {p.name} | Total Latency: {total_latency:.2f}s")
                else:
                    # Put back in queue if not finished
                    self.ready_queue.append(p)
                
                self.resource_manager.update_metrics()

    def _notify_replica_completed(self, pid, latency_s):
        try:
            requests.post(f"{self.peer_url}/api/replicas/{pid}/complete",
                          json={"latency_s": latency_s}, timeout=0.5)
        except requests.RequestException:
            pass

# ==========================================
# Flask API Implementation
# ==========================================
app = Flask(__name__)
faults = WorkerFaultInjector(os.getenv("NODE_ID", "worker"))


@app.before_request
def apply_injected_faults():
    peer_request = request.path.startswith(("/api/replica", "/api/replicas", "/internal/"))
    reply = faults.request_gate(request.path, peer_request)
    if reply is None:
        return None
    body, status = reply
    return (jsonify(body) if isinstance(body, dict) else body), status


@app.route('/chaos/inject', methods=['POST'])
def inject_fault():
    data = request.get_json(silent=True) or {}
    fault_type = data.pop("type", "")
    try:
        result = faults.inject(fault_type, **data)
    except (TypeError, ValueError) as exc:
        return jsonify({"error": str(exc)}), 400
    if fault_type == "crash":
        WorkerFaultInjector.crash_now()
    return jsonify(result)


@app.route('/chaos/clear', methods=['POST'])
def clear_faults():
    data = request.get_json(silent=True) or {}
    return jsonify({"cleared": faults.clear(data.get("type"))})


@app.route('/chaos/status', methods=['GET'])
def fault_status():
    return jsonify({"node_id": worker.node_id if 'worker' in globals() else os.getenv("NODE_ID", "worker"),
                    "active": faults.active(), "history": faults.history})
app.config["MAX_CONTENT_LENGTH"] = int(os.getenv("MAX_FILE_BYTES", str(16 * 1024 * 1024)))


@app.route('/internal/files/<path:file_path>', methods=['GET'])
def internal_file_read(file_path):
    row = file_store.get(file_path)
    return app.response_class(row["content"], mimetype="application/octet-stream", headers={
        "X-File-Version": str(row["version"]), "X-File-Found": str(row["found"]).lower(),
        "X-File-Deleted": str(row["deleted"]).lower(), "X-File-Checksum": row["checksum"],
        "X-File-Updated-At": str(row["updated_at"] or ""), "X-Node-ID": worker.node_id,
    })


@app.route('/internal/files/<path:file_path>/prepare', methods=['PUT'])
def internal_file_prepare(file_path):
    try:
        result = file_store.prepare(request.headers.get("X-Transaction-ID", ""), file_path,
                                    request.get_data(), int(request.headers.get("X-Expected-Version", "-1")),
                                    request.headers.get("X-File-Deleted", "false").lower() == "true")
        return jsonify(result)
    except (ValueError, RuntimeError) as exc:
        return jsonify({"status": "conflict", "message": str(exc)}), 409


@app.post('/internal/files/transactions/<tx_id>/commit')
def internal_file_commit(tx_id):
    try:
        result = file_store.commit(tx_id)
        if not result.get("idempotent"):
            FILE_REPLICAS_COMMITTED.inc()
            FILE_REPLICA_BYTES.inc(len(result.get("content") or b""))
            publish_event("file.replica_committed", {
                "node_id": worker.node_id, "transaction_id": tx_id, "path": result["path"],
                "version": result["version"], "checksum": result["checksum"],
                "deleted": result["deleted"],
            })
        result.pop("content", None)
        return jsonify(result)
    except (KeyError, RuntimeError) as exc:
        return jsonify({"status": "error", "message": str(exc)}), 409


@app.post('/internal/files/transactions/<tx_id>/abort')
def internal_file_abort(tx_id):
    return jsonify(file_store.abort(tx_id))


@app.route('/internal/files/<path:file_path>/install', methods=['PUT'])
def internal_file_install(file_path):
    try:
        result = file_store.install(file_path, request.get_data(),
                                    int(request.headers.get("X-File-Version", "0")),
                                    request.headers.get("X-File-Deleted", "false").lower() == "true",
                                    request.headers.get("X-File-Checksum"),
                                    float(request.headers.get("X-File-Updated-At", "0") or 0) or None)
        return jsonify(result)
    except (ValueError, RuntimeError) as exc:
        return jsonify({"status": "error", "message": str(exc)}), 409


@app.get('/internal/files')
def internal_file_list():
    return jsonify({"files": file_store.list_files(request.args.get("prefix", ""))})
worker = None
RPC_REQUESTS = Counter("node_rpc_requests_total", "RPC server-stub requests", ["method", "outcome"])

@app.route('/api/status', methods=['GET'])
def status():
    avail_cpu, avail_mem = worker.resource_manager.snapshot()   # consistent read under the lock
    with worker.lock:
        queue_len, completed = len(worker.ready_queue), len(worker.completed_queue)
    payload = {
        "node_id": worker.node_id,
        "available_cpu": avail_cpu,
        "available_memory": avail_mem,
        "total_cpu": worker.resource_manager.total_cpu,
        "total_memory": worker.resource_manager.total_memory,
        "queue_length": queue_len,
        "completed": completed,
        "rejected": worker.rejected,
        "sched_tick": SCHED_TICK
    }
    if faults.byzantine_mode() in ("corrupt", "collude"):
        payload["available_cpu"] = faults.corrupt(avail_cpu)
    return jsonify(payload)

# ==========================================
# MILESTONE 8: health + experiment support
# ==========================================
@app.route('/api/health', methods=['GET'])
def health():
    """Cheap liveness probe used by dispatchers / failover logic."""
    if faults.get("service") is not None:
        return jsonify({"status": "SERVICE_FAILED", "node_id": worker.node_id}), 503
    return jsonify({"status": "up", "node_id": worker.node_id, "incarnation": worker.incarnation,
                    "replication": {"mode": "async", "lag_s": 0.0}})


@app.route('/health', methods=['GET'])
def ft_health():
    return health()


@app.route('/admin/restart_service', methods=['POST'])
def restart_service():
    return jsonify({"ok": True, "cleared": faults.clear_service_level()})


@app.route('/api/failover/promote', methods=['POST'])
def ft_promote_replicas():
    return promote_replicas()


@app.route('/api/resync', methods=['POST'])
def ft_resync():
    return jsonify({"ok": True, "node_id": worker.node_id, "replicas": len(worker.shadow_processes)})

@app.route('/api/completions', methods=['GET'])
def completions():
    """Node-side latency (queue + service) of every finished process."""
    with worker.lock:
        return jsonify({"node_id": worker.node_id, "completions": list(worker.completion_log)})

@app.route('/api/process/<pid>', methods=['GET'])
def process_status(pid):
    """Internal process lookup used by the logical gateway; includes standby copies."""
    result = _process_record(pid)
    return (jsonify(result), 200) if result else (jsonify({"status": "not_found", "pid": pid}), 404)


def _process_record(pid):
    with worker.lock:
        if pid in worker.terminated_processes:
            return {"pid": pid, "status": "terminated",
                    "terminated_at": worker.terminated_processes[pid]}
        for p in worker.ready_queue:
            if p.pid == pid:
                return {"pid": pid, "status": "running", "process": asdict(p)}
        for p in worker.completed_queue:
            if p.pid == pid:
                return {"pid": pid, "status": "completed", "process": asdict(p)}
        if pid in worker.shadow_processes:
            replica = worker.shadow_processes[pid]
            state = "completed" if replica.get("replica_state") == "completed" else "replicated"
            return {"pid": pid, "status": state, "process": replica}
    return None

@app.route('/api/replica', methods=['POST'])
def receive_replica():
    """Store a standby process copy without consuming resources or scheduling it."""
    data = request.get_json(silent=True) or {}
    process_data = data.get("process") or {}
    if not process_data.get("pid"):
        return jsonify({"status": "error", "message": "process.pid is required"}), 400
    if faults.byzantine_mode() == "lying_ack":
        return jsonify({"status": "replicated", "node_id": worker.node_id, "stored": False})
    with worker.lock:
        known = any(p.pid == process_data["pid"] for p in worker.ready_queue + worker.completed_queue)
        if not known:
            worker.shadow_processes[process_data["pid"]] = process_data
    return jsonify({"status": "replicated", "node_id": worker.node_id})

@app.route('/api/replicas/promote', methods=['POST'])
def promote_replicas():
    """Activate standby work after its former owner becomes unreachable."""
    with worker.lock:
        copies = list(worker.shadow_processes.items())
    promoted, rejected = [], []
    for pid, payload in copies:
        with worker.lock:
            if payload.get("replica_state") == "completed":
                worker.shadow_processes.pop(pid, None)
                completed = {key: payload[key] for key in Process.__dataclass_fields__ if key in payload}
                completed["remaining_burst"] = 0
                worker.completed_queue.append(Process(**completed))
                continue
        ok, message = worker.submit_process(payload, is_migrated=True)
        if ok:
            with worker.lock:
                worker.shadow_processes.pop(pid, None)
        (promoted if ok else rejected).append(pid if ok else {"pid": pid, "reason": message})
    return jsonify({"status": "promoted", "node_id": worker.node_id,
                    "promoted": promoted, "rejected": rejected})

@app.route('/api/replicas/<pid>/complete', methods=['POST'])
def mark_replica_completed(pid):
    with worker.lock:
        payload = worker.shadow_processes.get(pid)
        if payload is not None:
            payload["replica_state"] = "completed"
            payload["replica_latency_s"] = (request.get_json(silent=True) or {}).get("latency_s")
    return jsonify({"status": "updated"})

@app.route('/api/replicas/<pid>', methods=['DELETE'])
def discard_replica(pid):
    with worker.lock:
        worker.shadow_processes.pop(pid, None)
    return jsonify({"status": "discarded", "pid": pid})

@app.route('/api/process/<pid>', methods=['DELETE'])
def terminate_process(pid):
    """Idempotently terminate active/completed work or discard its standby copy."""
    result, code = _terminate_process(pid)
    return jsonify(result), code


def _terminate_process(pid):
    with worker.lock:
        removed_ready = [p for p in worker.ready_queue if p.pid == pid]
        worker.ready_queue = [p for p in worker.ready_queue if p.pid != pid]
        removed_completed = [p for p in worker.completed_queue if p.pid == pid]
        worker.completed_queue = [p for p in worker.completed_queue if p.pid != pid]
        had_shadow = worker.shadow_processes.pop(pid, None) is not None
        already = pid in worker.terminated_processes
        found = bool(removed_ready or removed_completed or had_shadow)
        if found:
            worker.terminated_processes[pid] = time.time()
    for process in removed_ready:
        worker.resource_manager.release(process.cpu_needed, process.memory_needed)
    worker.resource_manager.update_metrics()
    if found or already:
        return {"status": "terminated" if found else "already_terminated", "pid": pid}, 200
    return {"status": "not_found", "pid": pid}, 404


@app.route('/rpc', methods=['POST'])
def worker_rpc_server_stub():
    """JSON-RPC 2.0 server stub that dispatches whitelisted worker/state methods."""
    envelope = request.get_json(silent=True)
    if not isinstance(envelope, dict) or envelope.get("jsonrpc") != "2.0" or not envelope.get("id"):
        return jsonify({"jsonrpc": "2.0", "id": envelope.get("id") if isinstance(envelope, dict) else None,
                        "error": {"code": -32600, "message": "invalid JSON-RPC request"}})
    request_id, method = envelope["id"], envelope.get("method")
    params = envelope.get("params") or {}
    if not isinstance(params, dict):
        return jsonify({"jsonrpc": "2.0", "id": request_id,
                        "error": {"code": -32602, "message": "params must be an object"}})
    try:
        if method == "system.ping":
            result = {"node_id": worker.node_id, "status": "up", "echo": params.get("echo")}
        elif method == "worker.status":
            result = status().get_json()
        elif method == "process.submit":
            success, message = worker.submit_process(params.get("process") or {})
            if not success:
                raise RuntimeError(message)
            result = {"status": "accepted", "message": message}
        elif method == "process.get":
            result = _process_record(str(params.get("pid", "")))
            if result is None:
                raise KeyError("process not found")
        elif method == "process.terminate":
            result, code = _terminate_process(str(params.get("pid", "")))
            if code >= 400:
                raise KeyError("process not found")
        elif method == "state.get":
            result = shared_state.get(str(params.get("key", "")))
        elif method == "state.list":
            result = {"entries": shared_state.list_values(str(params.get("prefix", "")))}
        elif method == "state.prepare":
            result = shared_state.prepare(str(params["tx_id"]), str(params["key"]),
                                          params.get("value"), int(params["expected_version"]),
                                          bool(params.get("deleted", False)))
        elif method == "state.commit":
            result = shared_state.commit(str(params["tx_id"]))
        elif method == "state.abort":
            result = shared_state.abort(str(params["tx_id"]))
        elif method == "state.install":
            result = shared_state.install(str(params["key"]), params.get("value"),
                                          int(params["version"]), bool(params.get("deleted", False)),
                                          params.get("updated_at"))
        else:
            RPC_REQUESTS.labels(method=str(method), outcome="method_not_found").inc()
            return jsonify({"jsonrpc": "2.0", "id": request_id,
                            "error": {"code": -32601, "message": "method not found"}})
        RPC_REQUESTS.labels(method=method, outcome="ok").inc()
        result["node_id"] = worker.node_id if isinstance(result, dict) else None
        return jsonify({"jsonrpc": "2.0", "id": request_id, "result": result})
    except KeyError as exc:
        RPC_REQUESTS.labels(method=str(method), outcome="not_found").inc()
        return jsonify({"jsonrpc": "2.0", "id": request_id,
                        "error": {"code": -32004, "message": str(exc)}})
    except (ValueError, RuntimeError) as exc:
        RPC_REQUESTS.labels(method=str(method), outcome="failed").inc()
        return jsonify({"jsonrpc": "2.0", "id": request_id,
                        "error": {"code": -32010, "message": str(exc)}})

@app.route('/api/reset', methods=['POST'])
def reset():
    """Clear queues, logs and resource accounting between experiment runs."""
    with worker.lock:
        worker.ready_queue.clear()
        worker.completed_queue.clear()
        worker.completion_log.clear()
        worker.shadow_processes.clear()
        worker.terminated_processes.clear()
        worker.rejected = 0
    worker.resource_manager.reset()
    worker.resource_manager.update_metrics()
    return jsonify({"status": "success", "node_id": worker.node_id})

@app.route('/api/submit', methods=['POST'])
def submit():
    data = request.json
    success, msg = worker.submit_process(data)
    return jsonify({"status": "success" if success else "error", "message": msg}), (200 if success else 503)

@app.route('/api/migrate', methods=['POST'])
def migrate():
    data = request.json
    process_data = data.get("process")
    success, msg = worker.submit_process(process_data, is_migrated=True)
    return jsonify({"status": "success" if success else "error", "message": msg}), (200 if success else 503)

# ==========================================
# MILESTONE 3: Service Decomposition Endpoint
# ==========================================
@app.route('/api/deploy', methods=['POST'])
def deploy_service():
    """
    Allows the Cloud/Client layer (Host PC) to explicitly deploy 
    a microservice to this specific node.
    """
    data = request.json
    service_type = data.get("service_type")
    
    # Simulate the time it takes to initialize the service
    time.sleep(0.5) 
    
    print(f"[{worker.node_id}] DEPLOYED new service: {service_type}")
    
    return jsonify({
        "status": "success",
        "message": f"Service '{service_type}' successfully deployed to {worker.node_id}",
        "node_id": worker.node_id
    }), 200

# ==========================================
# Main Execution
# ==========================================
if __name__ == "__main__":
    # Requires 6 arguments: node_id, port, cpu, memory, peer_ip, peer_port
    if len(sys.argv) < 7:
        # Compose-friendly defaults; command-line arguments remain supported for experiments.
        sys.argv = [sys.argv[0], os.getenv("NODE_ID", "edge"), os.getenv("NODE_PORT", "5001"),
                    os.getenv("NODE_CPU", "4"), os.getenv("NODE_MEMORY", "1024"),
                    os.getenv("PEER_HOST", "core"), os.getenv("PEER_PORT", "5002")]
    
    worker = WorkerNode(
        node_id=sys.argv[1], 
        port=int(sys.argv[2]), 
        cpu=int(sys.argv[3]), 
        memory=int(sys.argv[4]), 
        peer_ip=sys.argv[5], 
        peer_port=int(sys.argv[6])
    )
    
    # Start Prometheus metrics exporter on port 8000 (Milestone 2)
    if KAFKA_BOOTSTRAP:
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                retries=3,
            )
            print(f"[{worker.node_id}] Kafka events -> {KAFKA_BOOTSTRAP}/{KAFKA_TOPIC}")
        except KafkaError as exc:
            print(f"[{worker.node_id}] Kafka unavailable; event publishing disabled: {exc}")
    start_http_server(PROM_PORT)
    print(f"[{worker.node_id}] Prometheus metrics on port {PROM_PORT} | scheduler tick {SCHED_TICK}s")
    print(f"[{worker.node_id}] Peer Node set to: {worker.peer_url}")
    
    # Start Flask API
    app.run(host="0.0.0.0", port=worker.port)
