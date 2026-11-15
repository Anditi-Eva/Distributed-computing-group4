import os
import sys
import time
import threading
import requests
from flask import Flask, request, jsonify
from prometheus_client import start_http_server, Gauge, Counter, Histogram
from dataclasses import dataclass, asdict

# ==========================================
# MILESTONE 2: Prometheus Metrics
# ==========================================
CPU_UTILIZATION = Gauge('node_cpu_utilization', 'CPU utilization percentage')
MEM_UTILIZATION = Gauge('node_memory_utilization', 'Memory utilization percentage')
PROCESSES_COMPLETED = Counter('node_processes_completed_total', 'Total processes completed')
MIGRATED_PROCESSES = Counter('node_processes_migrated_total', 'Processes offloaded to other nodes')

MIGRATION_LATENCY = Histogram('node_migration_latency_seconds', 'Time taken to offload a process to a peer')
API_LATENCY = Histogram(
    'node_api_latency_seconds', 
    'Time taken for a process to complete after submission', 
    buckets=(1.0, 2.0, 5.0, 10.0, 20.0, float("inf"))
)

# Deployment knobs (Milestone 8). Defaults keep the original behaviour.
PROM_PORT = int(os.environ.get("PROM_PORT", "8000"))      # metrics port (change to run 2 nodes on one PC)
SCHED_TICK = float(os.environ.get("SCHED_TICK", "1.0"))   # seconds per scheduling quantum

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
        self.port = port
        self.peer_url = f"http://{peer_ip}:{peer_port}"
        self.resource_manager = ResourceManager(cpu, memory)
        self.ready_queue = []
        self.completed_queue = []
        self.completion_log = []   # per-process results for the Milestone 8 experiments
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

        # 1. Try to allocate locally
        if self.resource_manager.allocate(p.cpu_needed, p.memory_needed):
            with self.lock:
                self.ready_queue.append(p)
            self.resource_manager.update_metrics()
            
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
                    
                    print(f"[{self.node_id}] COMPLETED {p.name} | Total Latency: {total_latency:.2f}s")
                else:
                    # Put back in queue if not finished
                    self.ready_queue.append(p)
                
                self.resource_manager.update_metrics()

# ==========================================
# Flask API Implementation
# ==========================================
app = Flask(__name__)
worker = None

@app.route('/api/status', methods=['GET'])
def status():
    avail_cpu, avail_mem = worker.resource_manager.snapshot()   # consistent read under the lock
    with worker.lock:
        queue_len, completed = len(worker.ready_queue), len(worker.completed_queue)
    return jsonify({
        "node_id": worker.node_id,
        "available_cpu": avail_cpu,
        "available_memory": avail_mem,
        "total_cpu": worker.resource_manager.total_cpu,
        "total_memory": worker.resource_manager.total_memory,
        "queue_length": queue_len,
        "completed": completed,
        "rejected": worker.rejected,
        "sched_tick": SCHED_TICK
    })

# ==========================================
# MILESTONE 8: health + experiment support
# ==========================================
@app.route('/api/health', methods=['GET'])
def health():
    """Cheap liveness probe used by dispatchers / failover logic."""
    return jsonify({"status": "up", "node_id": worker.node_id})

@app.route('/api/completions', methods=['GET'])
def completions():
    """Node-side latency (queue + service) of every finished process."""
    with worker.lock:
        return jsonify({"node_id": worker.node_id, "completions": list(worker.completion_log)})

@app.route('/api/reset', methods=['POST'])
def reset():
    """Clear queues, logs and resource accounting between experiment runs."""
    with worker.lock:
        worker.ready_queue.clear()
        worker.completed_queue.clear()
        worker.completion_log.clear()
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
        print("Usage: python worker_node.py <node_id> <port> <cpu> <memory> <peer_ip> <peer_port>")
        sys.exit(1)
    
    worker = WorkerNode(
        node_id=sys.argv[1], 
        port=int(sys.argv[2]), 
        cpu=int(sys.argv[3]), 
        memory=int(sys.argv[4]), 
        peer_ip=sys.argv[5], 
        peer_port=int(sys.argv[6])
    )
    
    # Start Prometheus metrics exporter on port 8000 (Milestone 2)
    start_http_server(PROM_PORT)
    print(f"[{worker.node_id}] Prometheus metrics on port {PROM_PORT} | scheduler tick {SCHED_TICK}s")
    print(f"[{worker.node_id}] Peer Node set to: {worker.peer_url}")
    
    # Start Flask API
    app.run(host="0.0.0.0", port=worker.port)