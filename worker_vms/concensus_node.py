import sys
import time
import requests
from flask import Flask, request, jsonify
from dataclasses import dataclass, asdict

app = Flask(__name__)

# --- MILESTONE 4: Logical Clocks & Consensus State ---
class ConsensusNode:
    def __init__(self, node_id, port, peer_ip, peer_port):
        self.node_id = node_id
        self.port = port
        self.peer_url = f"http://{peer_ip}:{peer_port}"
        
        # Logical Clock (Event Ordering)
        self.lamport_clock = 0 
        self.lock = time.time()
        
        # State (Network Slices)
        self.active_slices = {}
        self.is_leader = False # By default, Host PC is Leader

    def increment_clock(self):
        self.lamport_clock += 1
        return self.lamport_clock

    def update_clock(self, received_time):
        self.lamport_clock = max(self.lamport_clock, received_time) + 1
        return self.lamport_clock

node = None

# --- API ENDPOINTS ---
@app.route('/api/sync', methods=['POST'])
def sync_consensus():
    """Receives consensus sync messages from the Leader (Cloud/Host)"""
    data = request.json
    leader_clock = data.get("lamport_time")
    slice_id = data.get("slice_id")
    
    # Update local clock based on Leader's message (Synchronization)
    new_time = node.update_clock(leader_clock)
    
    # Commit the slice to local state (Consistency)
    node.active_slices[slice_id] = "ACTIVE"
    
    return jsonify({
        "status": "ACK",
        "node_id": node.node_id,
        "local_lamport_time": new_time,
        "message": f"Slice {slice_id} committed consistently."
    })

@app.route('/api/status', methods=['GET'])
def status():
    return jsonify({
        "node_id": node.node_id,
        "lamport_clock": node.lamport_clock,
        "active_slices": list(node.active_slices.keys())
    })

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: python consensus_node.py <node_id> <port> <peer_ip> <peer_port>")
        sys.exit(1)
        
    node = ConsensusNode(sys.argv[1], int(sys.argv[2]), sys.argv[3], int(sys.argv[4]))
    print(f"[{node.node_id}] Consensus Node started on port {node.port}")
    app.run(host="0.0.0.0", port=node.port)