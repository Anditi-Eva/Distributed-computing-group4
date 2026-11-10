import time
import uuid
import requests

# ==========================================
# UPDATE THESE IPs WITH YOUR VM IP ADDRESSES
# ==========================================
NODE1_IP = "192.168.56.104" 
NODE2_IP = "192.168.56.107" 

NODE1_URL = f"http://{NODE1_IP}:5001"
NODE2_URL = f"http://{NODE2_IP}:5002"

def create_process(name, cpu, mem, burst):
    return {
        "pid": str(uuid.uuid4())[:8],
        "name": name,
        "cpu_needed": cpu,
        "memory_needed": mem,
        "remaining_burst": burst
    }

def submit_to_node(node_url, process):
    try:
        response = requests.post(f"{node_url}/api/submit", json=process, timeout=5)
        return response.json()
    except requests.exceptions.RequestException:
        return {"status": "offline", "message": "Node unreachable"}

def get_node_status(node_url):
    try:
        response = requests.get(f"{node_url}/api/status", timeout=3)
        return response.json()
    except requests.exceptions.RequestException:
        return {"node_id": "UNKNOWN", "available_cpu": "-", "queue_length": "-", "completed": "-"}

def main():
    print("=== MILESTONE 1: Distributed OS Foundation (Host Interface) ===\n")

    # We create a heavy workload to force node1 to offload to node2
    processes = [
        create_process("Heavy-Video-Stream", cpu=3, mem=800, burst=10), 
        create_process("AI-Edge-Compute", cpu=3, mem=800, burst=10),     
        create_process("Telemetry-Update", cpu=1, mem=100, burst=6),     
    ]

    print("--> Submitting ALL tasks to VM 1 (node1)...")
    print("    (Note: VM 1 only has 4 CPUs and 1024 RAM. Task 2 will force an inter-node migration to VM 2)")
    
    for p in processes:
        res = submit_to_node(NODE1_URL, p)
        print(f"   [{p['name']}] -> {res.get('message')}")
        time.sleep(0.5)

    print("\n--> Monitoring nodes (6 seconds)...")
    for _ in range(6):
        time.sleep(1)
        n1 = get_node_status(NODE1_URL)
        n2 = get_node_status(NODE2_URL)
        print(f"   [VM1] CPU: {n1.get('available_cpu')} | Queue: {n1.get('queue_length')} | Completed: {n1.get('completed')}")
        print(f"   [VM2] CPU: {n2.get('available_cpu')} | Queue: {n2.get('queue_length')} | Completed: {n2.get('completed')}")

    print("\n=== Final Status ===")
    print("Node 1:", get_node_status(NODE1_URL))
    print("Node 2:", get_node_status(NODE2_URL))

if __name__ == "__main__":
    main()