import requests
import time
import statistics

# IPs of your VMs
EDGE_URL = "http://192.168.56.104:5001/api/sync"
CORE_URL = "http://192.168.56.107:5002/api/sync"

class ConsensusLeader:
    def __init__(self):
        self.lamport_clock = 0

    def propose_slice(self, slice_id):
        """Leader proposes a slice and sends sync messages to followers"""
        self.lamport_clock += 1
        payload = {"slice_id": slice_id, "lamport_time": self.lamport_clock}
        
        print(f"\n[Leader] Proposing {slice_id} at Logical Time: {self.lamport_clock}")
        
        # Send to Edge and Core
        total_latency = 0
        for name, url in [("Edge", EDGE_URL), ("Core", CORE_URL)]:
            start = time.time()
            try:
                res = requests.post(url, json=payload, timeout=2)
                latency = (time.time() - start) * 1000
                total_latency += latency
                data = res.json()
                print(f"  -> [{name}] {data['status']} | Synced Clock: {data['local_lamport_time']} | Delay: {latency:.2f}ms")
            except Exception as e:
                print(f"  -> [{name}] FAILED: {e}")
                
        return total_latency / 2

def main():
    leader = ConsensusLeader()
    latencies = []
    
    print("=== MILESTONE 4: Consensus & Coordination Evaluation ===")
    for i in range(5):
        lat = leader.propose_slice(f"slice_{i}")
        latencies.append(lat)
        time.sleep(1)
        
    print("\n--- EXPERIMENTAL RESULTS ---")
    print(f"Avg Synchronization Delay: {statistics.mean(latencies):.2f} ms")
    print(f"Coordination Overhead (Jitter): {statistics.stdev(latencies):.2f} ms")
    print(f"Message Complexity: O(N) -> 2 messages sent per slice.")

if __name__ == "__main__":
    main()