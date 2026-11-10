import sys
import time
import threading
import requests
from flask import Flask, request, jsonify
from prometheus_client import start_http_server, Gauge, Counter, Histogram
from dataclasses import dataclass, asdict

# ----------------- Prometheus Metrics -----------------
CPU_UTILIZATION = Gauge('node_cpu_utilization', 'CPU utilization percentage')
MEM_UTILIZATION = Gauge('node_memory_utilization', 'Memory utilization percentage')
PROCESSES_COMPLETED = Counter('node_processes_completed_total', 'Total processes completed')
MIGRATED_PROCESSES = Counter('node_processes_migrated_total', 'Processes offloaded to other nodes')
# NEW MILESTONE 2 METRICS
MIGRATION_LATENCY = Histogram('node_migration_latency_seconds', 'Time taken to offload a process to a peer')
API_LATENCY = Histogram('node_api_latency_seconds', 'Time taken for a process to complete after submission', buckets=(1.0, 2.0, 5.0, 10.0, 20.0, float("inf")))

# ----------------- Models -----------------
@dataclass
class Process:
    pid: str
    name: str
    cpu_needed: int
    memory_needed: int
    remaining_burst: int
    arrival_time: float = 0.0  # Milestone 2: Track when it arrived
    start_time: float = 0.0    # Milestone 2: Track when it started

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

    def update_metrics(self):
        CPU_UTILIZATION.set(((self.total_cpu - self.avail_cpu) / self.total_cpu) * 100)
        MEM_UTILIZATION.set(((self.total_memory - self.avail_memory) / self.total_memory) * 100)

# ----------------- Distributed Worker Node -----------------
class WorkerNode:
    def __init__(self, node_id, port, cpu, memory, peer_ip, peer_port):
        self.node_id = node_id
        self.port = port
        self.peer_url = f"http://{peer_ip}:{peer_port}"
        self.resource_manager = ResourceManager(cpu, memory)
        self.ready_queue = []
        self.completed_queue = []
        self.lock = threading.Lock()
        
        threading.Thread(target=self._scheduler_loop, daemon=True).start()

    def submit_process(self, process_data, is_migrated=False):
        start_network_time = time.time()
        try:
            p = Process(**process_data)
        except TypeError:
            return False, "Invalid process payload"

        # Record when the process arrived
        if p.arrival_time == 0.0:
            p.arrival_time = time.time()

        # 1. Try to allocate locally
        if self.resource_manager.allocate(p.cpu_needed, p.memory_needed):
            with self.lock:
                self.ready_queue.append(p)
            self.resource_manager.update_metrics()
            
            # Record API Latency (Time from network request to queue insertion)
            API_LATENCY.observe(time.time() - start_network_time)
            return True, "Process admitted locally"
        
        # 2. Resource Allocation Failure -> Inter-node Communication
        if not is_migrated:
            print(f"[{self.node_id}] Resource full. Offloading '{p.name}'...")
            try:
                offload_start = time.time()
                res = requests.post(f"{self.peer_url}/api/migrate", json={"process": asdict(p)}, timeout=3)
                if res.status_code == 200:
                    MIGRATED_PROCESSES.inc()
                    MIGRATION_LATENCY.observe(time.time() - offload_start) # Measure migration network time
                    return True, f"Offloaded to peer {self.peer_url}"
            except requests.exceptions.RequestException:
                pass # Packet Loss occurs here if network fails
            return False, "Resource denied (Local full, peer unreachable)"
        
        return False, "Resource denied (Migrated process rejected)"

    def _scheduler_loop(self):
        """Round-Robin Scheduler Simulation"""
        while True:
            time.sleep(1) # Scheduling tick
            with self.lock:
                if not self.ready_queue:
                    continue
                
                p = self.ready_queue.pop(0)
                
                # Record start time if it hasn't started yet
                if p.start_time == 0.0:
                    p.start_time = time.time()
                
                p.remaining_burst -= 2
                
                if p.remaining_burst <= 0:
                    self.resource_manager.release(p.cpu_needed, p.memory_needed)
                    self.completed_queue.append(p)
                    PROCESSES_COMPLETED.inc()
                    
                    # Calculate Total Processing Latency (Completion Time - Arrival Time)
                    total_latency = time.time() - p.arrival_time
                    API_LATENCY.observe(total_latency)
                    
                    print(f"[{self.node_id}] COMPLETED {p.name} | Total Latency: {total_latency:.2f}s")
                else:
                    self.ready_queue.append(p)
                
                self.resource_manager.update_metrics()

# ----------------- Flask API -----------------
app = Flask(__name__)
worker = None

@app.route('/api/status', methods=['GET'])
def status():
    return jsonify({
        "node_id": worker.node_id,
        "available_cpu": worker.resource_manager.avail_cpu,
        "available_memory": worker.resource_manager.avail_memory,
        "queue_length": len(worker.ready_queue),
        "completed": len(worker.completed_queue)
    })

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

if __name__ == "__main__":
    if len(sys.argv) < 7:
        print("Usage: python worker_node.py <node_id> <port> <cpu> <memory> <peer_ip> <peer_port>")
        sys.exit(1)
    
    worker = WorkerNode(
        node_id=sys.argv[1], port=int(sys.argv[2]), 
        cpu=int(sys.argv[3]), memory=int(sys.argv[4]), 
        peer_ip=sys.argv[5], peer_port=int(sys.argv[6])
    )
    
    start_http_server(8000)
    print(f"[{worker.node_id}] Prometheus metrics on port 8000")
    app.run(host="0.0.0.0", port=worker.port)