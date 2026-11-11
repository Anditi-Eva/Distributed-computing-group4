# participant_3pc.py
import sys
import psycopg2
from flask import Flask, request, jsonify

app = Flask(__name__)

DB_CONFIG = {
    "dbname": "telecom_slices",
    "user": "postgres",
    "password": "admin",
    "host": "localhost",
    "port": "5432"
}

# In-memory state: 'PREPARED', 'PRECOMMITTED', 'COMMITTED'
transaction_state = {}

@app.route('/prepare', methods=['POST'])
def prepare():
    data = request.json
    tx_id = data['tx_id']
    slice_data = data['slice']
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("SELECT status FROM slices WHERE slice_id = %s", (slice_data['slice_id'],))
        if cur.fetchone():
            return jsonify({"status": "ABORT", "reason": "Slice already exists"}), 409
            
        # Phase 1: Lock and Prepare
        transaction_state[tx_id] = {"data": slice_data, "status": "PREPARED"}
        conn.close()
        return jsonify({"status": "PREPARED"}), 200
    except Exception as e:
        return jsonify({"status": "ABORT", "reason": str(e)}), 500

@app.route('/precommit', methods=['POST'])
def precommit():
    """NEW: 3PC Phase 2"""
    data = request.json
    tx_id = data['tx_id']
    
    if tx_id in transaction_state and transaction_state[tx_id]["status"] == "PREPARED":
        transaction_state[tx_id]["status"] = "PRECOMMITTED"
        return jsonify({"status": "PRECOMMITTED"}), 200
    
    return jsonify({"status": "ERROR", "reason": "Not prepared"}), 404

@app.route('/commit', methods=['POST'])
def commit():
    """3PC Phase 3"""
    data = request.json
    tx_id = data['tx_id']
    
    if tx_id not in transaction_state or transaction_state[tx_id]["status"] != "PRECOMMITTED":
        return jsonify({"status": "ERROR", "reason": "Cannot commit directly"}), 404
        
    slice_data = transaction_state[tx_id]["data"]
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO slices (slice_id, slice_type, status) VALUES (%s, %s, %s)",
            (slice_data['slice_id'], slice_data['slice_type'], 'ACTIVE')
        )
        conn.commit()
        del transaction_state[tx_id]
        conn.close()
        return jsonify({"status": "COMMITTED"}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({"status": "ERROR", "reason": str(e)}), 500

if __name__ == "__main__":
    port = int(sys.argv[1])
    app.run(host="0.0.0.0", port=port)