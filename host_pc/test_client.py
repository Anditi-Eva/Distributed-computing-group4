import time
import uuid
import requests
import csv

# --- ARCHITECTURE IPS ---
# Node 1 (Edge) & Node 2 (Core) run on the VMs
NODE1_IP = "192.168.56.104" 
NODE2_IP = "192.168.56.107" 

NODE1_URL = f"http://{NODE1_IP}:5001" # Edge (mMTC)
NODE2_URL = f"http://{NODE2_IP}:5002" # Core (eMBB/URLLC)

# ==========================================
# CLOUD TIER (WINDOWS HOST PC)
# ==========================================
class CloudTierGateway:
    def __init__(self):
        print("=== INITIALIZING CLOUD TIER (WINDOWS HOST PC) ===")
        print("Role: API Gateway, Load Balancer, and Service Orchestrator")
        self.cloud_completed = 0 # Cloud can process its own tasks if VMs fail

    def deploy_services(self):
        """Cloud Tier pushes microservices down to Edge and Core"""
        print("\n[Cloud Tier] Deploying Microservices to Edge and Core...")
        
        # 1. Deploy lightweight mMTC service to Edge
        edge_payload = {"service_type": "IoT_Data_Aggregator"}
        try:
            res_edge = requests.post(f"{NODE1_URL}/api/deploy", json=edge_payload)
            print(f"   Cloud -> Edge: {res_edge.json().get('message')}")
        except Exception:
            print("   Cloud -> Edge: FAILED (Node unreachable)")

        # 2. Deploy heavy eMBB/URLLC service to Core
        core_payload = {"service_type": "Video_Optimization_Engine"}
        try:
            res_core = requests.post(f"{NODE2_URL}/api/deploy", json=core_payload)
            print(f"   Cloud -> Core: {res_core.json().get('message')}")
        except Exception:
            print("   Cloud -> Core: FAILED (Node unreachable)")

    def route_task(self, process, slice_type):
        """Load Balancer Logic: Decides where the task goes"""
        if slice_type == "mMTC":
            target_url = NODE1_URL
            target_name = "Edge (Node 1)"
        else:
            target_url = NODE2_URL
            target_name = "Core (Node 2)"
            
        try:
            start_req = time.time()
            response = requests.post(f"{target_url}/api/submit", json=process, timeout=3)
            latency = time.time() - start_req 
            return response.json(), latency, target_name
        except requests.exceptions.RequestException:
            # CLOUD FALLBACK: If Edge/Core is down, the Host PC (Cloud) takes the task
            print(f"   [!] {target_name} unreachable. Cloud Tier is taking over the task.")
            time.sleep(0.5) # Simulate Cloud processing
            self.cloud_completed += 1
            return {"status": "success", "message": "Processed by Cloud Fallback"}, 0.5, "Cloud (Host PC)"

# ==========================================
# WORKLOAD GENERATOR
# ==========================================
def create_process_from_dataset(row):
    slice_type = row['slice_type']
    
    if slice_type == "URLLC":
        cpu, mem, burst = 3, 128, 4   
    elif slice_type == "eMBB":
        cpu, mem, burst = 2, 512, 12  
    else: # mMTC
        cpu, mem, burst = 1, 64, 6    

    return {
        "pid": str(uuid.uuid4())[:8],
        "name": f"Slice-{row['slice_id']}-{slice_type}",
        "cpu_needed": cpu,
        "memory_needed": mem,
        "remaining_burst": burst
    }, slice_type

def get_total_completed():
    total = 0
    for url in [NODE1_URL, NODE2_URL]:
        try:
            res = requests.get(f"{url}/api/status", timeout=1)
            total += res.json().get("completed", 0)
        except:
            pass
    return total

# ==========================================
# MAIN EXECUTION
# ==========================================
def main():
    # 1. Initialize the Cloud Tier (Host PC)
    cloud = CloudTierGateway()
    
    # 2. Cloud pushes services to Edge and Core (Proves Service Decomposition)
    cloud.deploy_services()

    # 3. Load Dataset
    workload = []
    try:
        with open('slices.csv', mode='r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if int(row['slice_id']) <= 20:
                    process, slice_type = create_process_from_dataset(row)
                    workload.append((process, slice_type))
    except FileNotFoundError:
        print("Error: slices.csv not found.")
        return

    num_slices = len(workload)
    print(f"\n[Cloud Tier] Loaded {num_slices} network slices. Starting routing...")

    # 4. Cloud acts as Load Balancer, routing tasks to Edge/Core
    start_time = time.time()
    total_network_latency = 0
    successful_submissions = 0
    
    for p, slice_type in workload:
        res, lat, routed_to = cloud.route_task(p, slice_type)
        total_network_latency += lat
        if res.get("status") == "success":
            successful_submissions += 1
        
        print(f"   [{p['name']}] routed to {routed_to} -> {res.get('message')}")
        time.sleep(0.2) 

    # 5. Monitor System
    print(f"\n[Cloud Tier] Awaiting system completion...")
    while True:
        time.sleep(1)
        completed = get_total_completed() + cloud.cloud_completed
        print(f"   [Monitor] Total Slices Completed: {completed}/{num_slices}")
        if completed >= num_slices:
            break
            
    end_time = time.time()

    # 6. Results
    total_time = end_time - start_time
    throughput = successful_submissions / total_time 
    avg_latency = (total_network_latency / num_slices) * 1000

    print("\n=== PERFORMANCE EVALUATION (CLOUD GATEWAY) ===")
    print(f"Total Execution Time : {total_time:.2f} seconds")
    print(f"System Throughput    : {throughput:.2f} slices/second")
    print(f"Avg API Latency      : {avg_latency:.2f} ms")
    print(f"Packet Loss          : {((num_slices - successful_submissions) / num_slices) * 100:.1f}%")

if __name__ == "__main__":
    main()