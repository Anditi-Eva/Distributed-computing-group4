import sys
import time
import threading
import requests
from flask import Flask, request, jsonify
from prometheus_client import start_http_server, Gauge, Counter
import networkx as nx # For the Resource Allocation Graph

# --- Prometheus Metrics ---
CPU_UTILIZATION = Gauge('node_cpu_utilization', 'CPU utilization percentage')
MEM_UTILIZATION = Gauge('node_memory_utilization', 'Memory utilization percentage')
DEADLOCK_COUNT = Counter('node_deadlocks_detected_total', 'Total deadlocks detected')
MIGRATED_PROCESSES = Counter('node_processes_migrated_total', 'Processes offloaded')

app = Flask(__name__)

# --- Resource Allocation Graph (RAG) ---
# We use a NetworkX Directed Graph to detect circular waits.
# Nodes are either "Process_X" or "Resource_Y"
rag = nx.DiGraph()
rag_lock = threading.Lock()

class ResourceManager:
    def __init__(self, total_cpu, total_mem):
        self.total_cpu = total_cpu
        self.total_mem = total_mem
        self.avail_cpu = total_cpu
        self.avail_mem = total_mem
        self.lock = threading.Lock()

    def allocate(self, pid, cpu, mem):
        with self.lock:
            if self.avail_cpu >= cpu and self.avail_mem >= mem:
                self.avail_cpu -= cpu
                self.avail_mem -= mem
                CPU_UTILIZATION.set(((self.total_cpu - self.avail_cpu) / self.total_cpu) * 100)
                MEM_UTILIZATION.set(((self.total_mem - self.avail_mem) / self.total_mem) * 100)
                return True
            return False

    def release(self, cpu, mem):
        with self.lock:
            self.avail_cpu += cpu
            self.avail_mem += mem
            CPU_UTILIZATION.set(((self.total_cpu - self.avail_cpu) / self.total_cpu) * 100)
            MEM_UTILIZATION.set(((self.total_mem - self.avail_mem) / self.total_mem) * 100)

resource_manager = None
node_id = ""

def process_task(pid, cpu, mem):
    """Simulates a concurrent telecom process"""
    print(f"[{node_id}] Process {pid} requesting {cpu} CPU, {mem} MEM...")
    
    # 1. Resource Contention
    if resource_manager.allocate(pid, cpu, mem):
        with rag_lock:
            # RAG: Resource -> Process (Allocation Edge)
            rag.add_edge(f"Res_CPU", f"Proc_{pid}")
            rag.add_edge(f"Res_MEM", f"Proc_{pid}")
            
        print(f"[{node_id}] Process {pid} ACQUIRED resources. Working...")
        time.sleep(4) # Simulate work
        
        # Release resources
        resource_manager.release(cpu, mem)
        with rag_lock:
            # Remove edges (Release)
            rag.remove_edges_from([(f"Res_CPU", f"Proc_{pid}"), (f"Res_MEM", f"Proc_{pid}")])
        print(f"[{node_id}] Process {pid} COMPLETED and released resources.")
    else:
        print(f"[{node_id}] Process {pid} REJECTED. Not enough resources.")

@app.route('/api/execute_concurrent', methods=['POST'])
def execute_concurrent():
    """Triggers multiple concurrent processes"""
    data = request.json
    # Spawn processes in separate threads to simulate concurrency
    threading.Thread(target=process_task, args=(data['pid'], data['cpu'], data['mem'])).start()
    return jsonify({"status": "started"}), 200

@app.route('/api/deadlock_check', methods=['GET'])
def deadlock_check():
    """Uses NetworkX to detect cycles in the Wait-For Graph (Deadlock Detection)"""
    with rag_lock:
        try:
            # A cycle in the RAG indicates a deadlock
            cycle = nx.find_cycle(rag)
            DEADLOCK_COUNT.inc()
            return jsonify({"status": "DEADLOCK DETECTED", "cycle": cycle}), 200
        except nx.NetworkXNoCycle:
            return jsonify({"status": "NO DEADLOCK"}), 200

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python concurrent_worker.py <node_id> <cpu> <mem>")
        sys.exit(1)
        
    node_id = sys.argv[1]
    resource_manager = ResourceManager(int(sys.argv[2]), int(sys.argv[3]))
    
    start_http_server(8000) # Prometheus
    print(f"[{node_id}] Prometheus on 8000")
    app.run(host="0.0.0.0", port=7001) # Run on port 7001