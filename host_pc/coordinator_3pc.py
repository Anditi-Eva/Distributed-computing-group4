# coordinator_3pc.py
import requests
import time
import uuid

NODE1_URL = "http://192.168.56.104:6001" # Edge
NODE2_URL = "http://192.168.56.107:6002" # Core
PARTICIPANTS = [NODE1_URL, NODE2_URL]

def execute_3pc(slice_data):
    tx_id = str(uuid.uuid4())[:8]
    print(f"\n--- Starting 3PC Transaction: {tx_id} ---")
    
    # PHASE 1: PREPARE (Can we do this?)
    print("[Phase 1] Sending PREPARE...")
    for node in PARTICIPANTS:
        try:
            res = requests.post(f"{node}/prepare", json={"tx_id": tx_id, "slice": slice_data}, timeout=2)
            if res.json().get("status") != "PREPARED":
                print(f"[ABORT] Node {node} rejected prepare.")
                return
        except Exception:
            print(f"[ABORT] Node {node} unreachable during PREPARE.")
            return

    # PHASE 2: PRE-COMMIT (The 3PC safety net)
    print("[Phase 2] All nodes PREPARED. Sending PRE-COMMIT...")
    for node in PARTICIPANTS:
        try:
            res = requests.post(f"{node}/precommit", json={"tx_id": tx_id}, timeout=2)
            if res.json().get("status") != "PRECOMMITTED":
                print(f"[ABORT] Node {node} failed pre-commit.")
                return
        except Exception:
            print(f"[ABORT] Node {node} failed during PRE-COMMIT.")
            return

    # PHASE 3: COMMIT (Finalize)
    print("[Phase 3] All nodes PRE-COMMITTED. Sending FINAL COMMIT...")
    for node in PARTICIPANTS:
        try:
            requests.post(f"{node}/commit", json={"tx_id": tx_id}, timeout=2)
            print(f"   -> Node {node} COMMITTED.")
        except Exception:
            print(f"   -> Node {node} failed final commit (would recover on reboot).")

if __name__ == "__main__":
    for i in range(3):
        execute_3pc({"slice_id": f"slice_3PC_{i}", "slice_type": "URLLC"})
        time.sleep(1)