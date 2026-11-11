# participant_db.py
import sys
import psycopg2
from flask import Flask, request, jsonify

app = Flask(__name__)

# Database Configuration
DB_CONFIG = {
    "dbname": "telecom_slices",
    "user": "postgres",
    "password": "admin",
    "host": "localhost",
    "port": "5432"
}

# In-memory lock state for 2PC
transaction_state = {}

@app.route('/prepare', methods=['POST'])
def prepare():
    data = request.json
    tx_id = data['tx_id']
    slice_data = data['slice']
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Check for conflicts (e.g., slice already exists)
        cur.execute("SELECT status FROM slices WHERE slice_id = %s", (slice_data['slice_id'],))
        if cur.fetchone():
            return jsonify({"status": "ABORT", "reason": "Slice already exists"}), 409
            
        # LOCK the database row (simulate holding a lock until commit)
        transaction_state[tx_id] = slice_data
        conn.close()
        
        # Phase 1: VOTE COMMIT
        return jsonify({"status": "PREPARED"}), 200
        
    except Exception as e:
        return jsonify({"status": "ABORT", "reason": str(e)}), 500

@app.route('/commit', methods=['POST'])
def commit():
    data = request.json
    tx_id = data['tx_id']
    
    if tx_id not in transaction_state:
        return jsonify({"status": "ERROR", "reason": "Transaction not found"}), 404
        
    slice_data = transaction_state[tx_id]
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Phase 2: WRITE TO DB
        cur.execute(
            "INSERT INTO slices (slice_id, slice_type, status) VALUES (%s, %s, %s)",
            (slice_data['slice_id'], slice_data['slice_type'], 'ACTIVE')
        )
        conn.commit()
        
        del transaction_state[tx_id] # Release lock
        conn.close()
        return jsonify({"status": "COMMITTED"}), 200
        
    except Exception as e:
        conn.rollback()
        return jsonify({"status": "ERROR", "reason": str(e)}), 500

if __name__ == "__main__":
    port = int(sys.argv[1])
    app.run(host="0.0.0.0", port=port)