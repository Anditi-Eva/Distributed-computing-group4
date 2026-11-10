import time
import uuid
import requests
import csv


# ==========================================
NODE1_IP = "192.168.56.104" 
NODE2_IP = "192.168.56.107" 

NODE1_URL = f"http://{NODE1_IP}:5001"
NODE2_URL = f"http://{NODE2_IP}:5002"

# --- Milestone 1 Requirement: Process Creation ---
def create_process_from_dataset(row):
    """Maps a 5G dataset row to our Distributed OS Process"""
    slice_type = row['slice_type']
    
    # Resource Mapping Logic based on 5G Slice Type
    if slice_type == "URLLC":
        # High compute, low latency requirement -> Short burst, high CPU
        cpu, mem, burst = 3, 128, 4   
    elif slice_type == "eMBB":
        # High memory/bandwidth -> Long burst, high Memory
        cpu, mem, burst = 2, 512, 12  
    else: # mMTC
        # Massive IoT -> Low compute, low memory, medium burst
        cpu, mem, burst = 1, 64, 6    

    return {
        "pid": str(uuid.uuid4())[:8],
        "name": f"Slice-{row['slice_id']}-{slice_type}",
        "cpu_needed": cpu,
        "memory_needed": mem,
        "remaining_burst": burst
    }

def submit_to_node(node_url, process):
    """Sends a process via REST and measures API latency"""
    try:
        start_req = time.time()
        response = requests.post(f"{node_url}/api/submit", json=process, timeout=5)
        latency = time.time() - start_req 
        return response.json(), latency
    except requests.exceptions.RequestException:
        return {"status": "offline", "message": "Node unreachable"}, 0

def get_total_completed():
    """Polls the nodes to check how many processes have finished"""
    total = 0
    for url in [NODE1_URL, NODE2_URL]:
        try:
            res = requests.get(f"{url}/api/status", timeout=2)
            total += res.json().get("completed", 0)
        except:
            pass
    return total

def main():
    print("=== MILESTONE 1 & 2: Distributed 5G Network Slicing ===")
    
    # --- Milestone 2 Requirement: Load Dataset ---
    workload = []
    try:
        with open('slices.csv', mode='r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # To keep the test manageable, let's load the first 20 slices from your 1000-row dataset
                if int(row['slice_id']) <= 20:
                    workload.append(create_process_from_dataset(row))
    except FileNotFoundError:
        print("Error: slices.csv not found. Please create it in this folder.")
        return

    num_slices = len(workload)
    print(f"Loaded {num_slices} network slices from 5G dataset.")

    print(f"\n--> Dispatching Slices to VM 1 (node1)...")
    
    # --- Milestone 2 Requirement: Performance Measurement Start ---
    start_time = time.time()
    total_network_latency = 0
    successful_submissions = 0
    
    # --- Milestone 1 Requirement: Dispatch to Nodes ---
    for p in workload:
        res, lat = submit_to_node(NODE1_URL, p)
        total_network_latency += lat
        if res.get("status") == "success":
            successful_submissions += 1
        print(f"   [{p['name']}] -> {res.get('message')}")
        time.sleep(0.2) # Slight delay between requests

    # --- Milestone 1 Requirement: Inter-node Migration Demo ---
    print("\n--> DEMONSTRATING INTER-NODE COMMUNICATION (Forced Migration)...")
    # We send a heavy task directly to Node 2's migrate endpoint to prove REST P2P works
    migrated_process = {
        "pid": str(uuid.uuid4())[:8],
        "name": "Forced-Migration-Slice",
        "cpu_needed": 1,
        "memory_needed": 64,
        "remaining_burst": 4
    }
    try:
        # In a real scenario, Node 1 sends this. We simulate it from the host to test the endpoint.
        res = requests.post(f"{NODE2_URL}/api/migrate", json={"process": migrated_process})
        print(f"   [Forced-Migration-Slice] -> {res.json().get('message')}")
    except Exception as e:
        print(f"   Migration failed: {e}")


    print(f"\n--> Awaiting system completion (Monitoring queues)...")
    
    # --- Milestone 2 Requirement: Throughput Tracking ---
    while True:
        time.sleep(1)
        completed = get_total_completed()
        print(f"   [Monitor] Total Slices Completed: {completed}/{num_slices}")
        if completed >= num_slices:
            break
            
    end_time = time.time()

    # --- Milestone 2 Requirement: Performance Calculations ---
    total_time = end_time - start_time
    throughput = successful_submissions / total_time 
    avg_latency = (total_network_latency / num_slices) * 1000

    print("\n=== PERFORMANCE EVALUATION RESULTS ===")
    print(f"Total Execution Time : {total_time:.2f} seconds")
    print(f"System Throughput    : {throughput:.2f} slices/second")
    print(f"Avg Network Latency  : {avg_latency:.2f} ms")
    print(f"Packet Loss          : {((num_slices - successful_submissions) / num_slices) * 100:.1f}%")

if __name__ == "__main__":
    main()