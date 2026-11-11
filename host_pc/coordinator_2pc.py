# coordinator_2pc.py
import requests
import time
import uuid

NODE1_URL = "http://192.168.56.104:6001" # Edge
NODE2_URL = "http://192.168.56.107:6002" # Core

def execute_2pc(slice_data):
    tx_id = str(uuid.uuid4())[:8]
    participants = [NODE1_URL, NODE2_URL]
    
    print(f"\n--- Starting 2PC Transaction: {tx_id} for Slice {slice_data['slice_id']} ---")
    
    # PHASE 1: PREPARE
    votes = []
    prepare_start = time.time()
    
    for node in participants:
        try:
            res = requests.post(f"{node}/prepare", json={"tx_id": tx_id, "slice": slice_data}, timeout=3)
            votes.append(res.json().get("status"))
        except Exception:
            votes.append("ABORT")

    print(f"[Phase 1] Prepare Votes: {votes}")

    # PHASE 2: COMMIT OR ABORT
    if all(v == "PREPARED" for v in votes):
        print("[Coordinator] All nodes PREPARED. Sending COMMIT...")
        commit_start = time.time()
        for node in participants:
            res = requests.post(f"{node}/commit", json={"tx_id": tx_id}, timeout=3)
            print(f"   -> Node at {node} response: {res.json()}")
        print(f"[Coordinator] Transaction {tx_id} Successfully COMMITTED.")
    else:
        print("[Coordinator] ABORTING transaction due to a node failure/vote.")
        # (In a full implementation, you would send an /abort endpoint here to release locks)

def main():
    # Simulate 5 slice transactions
    for i in range(5):
        slice_data = {"slice_id": f"slice_TX_{i}", "slice_type": "eMBB"}
        execute_2pc(slice_data)
        time.sleep(1)

if __name__ == "__main__":
    main()