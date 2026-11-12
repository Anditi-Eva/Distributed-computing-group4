"""Durable PostgreSQL participant for the Milestone 5 2PC/3PC demos."""
import os
import sys

import psycopg2
from flask import Flask, jsonify, request
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "telecom_slices"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "admin"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}
PROTOCOL = os.getenv("TRANSACTION_PROTOCOL", "2pc").lower()


def connect():
    return psycopg2.connect(**DB_CONFIG)


def ensure_schema():
    with connect() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS slices (
                slice_id VARCHAR(50) PRIMARY KEY,
                slice_type VARCHAR(20) NOT NULL,
                status VARCHAR(20) NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS distributed_transactions (
                tx_id VARCHAR(64) PRIMARY KEY,
                slice_id VARCHAR(50) NOT NULL,
                slice_type VARCHAR(20) NOT NULL,
                protocol VARCHAR(8) NOT NULL,
                state VARCHAR(20) NOT NULL,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS slice_reservations (
                slice_id VARCHAR(50) PRIMARY KEY,
                tx_id VARCHAR(64) NOT NULL UNIQUE
                    REFERENCES distributed_transactions(tx_id) ON DELETE CASCADE
            );
        """)


def tx_record(cur, tx_id):
    cur.execute("SELECT tx_id, slice_id, slice_type, protocol, state FROM distributed_transactions WHERE tx_id = %s", (tx_id,))
    row = cur.fetchone()
    return dict(zip(("tx_id", "slice_id", "slice_type", "protocol", "state"), row)) if row else None


@app.get('/health')
def health():
    try:
        with connect() as conn:
            conn.cursor().execute("SELECT 1")
        return jsonify({"status": "OK", "protocol": PROTOCOL}), 200
    except Exception as exc:
        return jsonify({"status": "UNAVAILABLE", "reason": str(exc)}), 503


@app.post('/prepare')
def prepare():
    data = request.get_json(silent=True) or {}
    tx_id, slice_data = data.get("tx_id"), data.get("slice") or {}
    slice_id, slice_type = slice_data.get("slice_id"), slice_data.get("slice_type")
    if not all((tx_id, slice_id, slice_type)):
        return jsonify({"status": "ABORT", "reason": "tx_id, slice_id and slice_type are required"}), 400
    try:
        with connect() as conn, conn.cursor() as cur:
            existing = tx_record(cur, tx_id)
            if existing:
                if existing["slice_id"] != slice_id or existing["slice_type"] != slice_type:
                    return jsonify({"status": "ABORT", "reason": "tx_id reused with different data"}), 409
                return jsonify({"status": existing["state"]}), 200
            cur.execute("SELECT 1 FROM slices WHERE slice_id = %s", (slice_id,))
            if cur.fetchone():
                return jsonify({"status": "ABORT", "reason": "Slice already exists"}), 409
            cur.execute("""INSERT INTO distributed_transactions
                (tx_id, slice_id, slice_type, protocol, state)
                VALUES (%s, %s, %s, %s, 'PREPARED')""",
                (tx_id, slice_id, slice_type, PROTOCOL))
            cur.execute("INSERT INTO slice_reservations (slice_id, tx_id) VALUES (%s, %s)", (slice_id, tx_id))
        return jsonify({"status": "PREPARED"}), 200
    except psycopg2.IntegrityError:
        return jsonify({"status": "ABORT", "reason": "Slice is reserved or already exists"}), 409
    except Exception as exc:
        return jsonify({"status": "ABORT", "reason": str(exc)}), 500


@app.post('/precommit')
def precommit():
    tx_id = (request.get_json(silent=True) or {}).get("tx_id")
    if not tx_id:
        return jsonify({"status": "ERROR", "reason": "tx_id is required"}), 400
    if PROTOCOL != "3pc":
        return jsonify({"status": "ERROR", "reason": "precommit is only available in 3PC mode"}), 400
    with connect() as conn, conn.cursor() as cur:
        cur.execute("UPDATE distributed_transactions SET state='PRECOMMITTED', updated_at=CURRENT_TIMESTAMP WHERE tx_id=%s AND state='PREPARED'", (tx_id,))
        if cur.rowcount == 0:
            cur.execute("SELECT state FROM distributed_transactions WHERE tx_id=%s", (tx_id,))
            row = cur.fetchone()
            if row and row[0] == "PRECOMMITTED":
                return jsonify({"status": "PRECOMMITTED"}), 200
            return jsonify({"status": "ERROR", "reason": "Transaction not prepared"}), 404
    return jsonify({"status": "PRECOMMITTED"}), 200


@app.post('/commit')
def commit():
    tx_id = (request.get_json(silent=True) or {}).get("tx_id")
    if not tx_id:
        return jsonify({"status": "ERROR", "reason": "tx_id is required"}), 400
    try:
        with connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT slice_id, slice_type, protocol, state FROM distributed_transactions WHERE tx_id=%s FOR UPDATE", (tx_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"status": "ERROR", "reason": "Transaction not found"}), 404
            slice_id, slice_type, protocol, state = row
            if state == "COMMITTED":
                return jsonify({"status": "COMMITTED"}), 200
            if state in ("ABORTED", "COMPLETED"):
                return jsonify({"status": "ERROR", "reason": f"Transaction is {state}"}), 409
            if protocol == "3pc" and state != "PRECOMMITTED":
                return jsonify({"status": "ERROR", "reason": "3PC transaction must be PRECOMMITTED"}), 409
            cur.execute("INSERT INTO slices (slice_id, slice_type, status) VALUES (%s, %s, 'ACTIVE')", (slice_id, slice_type))
            cur.execute("UPDATE distributed_transactions SET state='COMMITTED', updated_at=CURRENT_TIMESTAMP WHERE tx_id=%s", (tx_id,))
            cur.execute("DELETE FROM slice_reservations WHERE tx_id=%s", (tx_id,))
        return jsonify({"status": "COMMITTED"}), 200
    except Exception as exc:
        return jsonify({"status": "ERROR", "reason": str(exc)}), 500


@app.post('/complete')
def complete():
    tx_id = (request.get_json(silent=True) or {}).get("tx_id")
    with connect() as conn, conn.cursor() as cur:
        cur.execute("UPDATE distributed_transactions SET state='COMPLETED', updated_at=CURRENT_TIMESTAMP WHERE tx_id=%s AND state='COMMITTED'", (tx_id,))
        if not cur.rowcount:
            cur.execute("SELECT state FROM distributed_transactions WHERE tx_id=%s", (tx_id,))
            row = cur.fetchone()
            if row and row[0] == "COMPLETED":
                return jsonify({"status": "COMPLETED"}), 200
            return jsonify({"status": "ERROR", "reason": "Transaction is not committed"}), 409
    return jsonify({"status": "COMPLETED"}), 200


@app.post('/abort')
def abort():
    tx_id = (request.get_json(silent=True) or {}).get("tx_id")
    if not tx_id:
        return jsonify({"status": "ERROR", "reason": "tx_id is required"}), 400
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT state FROM distributed_transactions WHERE tx_id=%s FOR UPDATE", (tx_id,))
        row = cur.fetchone()
        if not row:
            return jsonify({"status": "ABORTED"}), 200
        if row[0] in ("COMMITTED", "COMPLETED"):
            return jsonify({"status": "ERROR", "reason": "Cannot abort a committed transaction"}), 409
        cur.execute("UPDATE distributed_transactions SET state='ABORTED', updated_at=CURRENT_TIMESTAMP WHERE tx_id=%s", (tx_id,))
        cur.execute("DELETE FROM slice_reservations WHERE tx_id=%s", (tx_id,))
    return jsonify({"status": "ABORTED"}), 200


@app.get('/transactions/<tx_id>')
def transaction_status(tx_id):
    with connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT tx_id, slice_id, protocol, state, updated_at FROM distributed_transactions WHERE tx_id=%s", (tx_id,))
        row = cur.fetchone()
    return (jsonify(dict(row)), 200) if row else (jsonify({"status": "NOT_FOUND"}), 404)


@app.post('/recover')
def recover():
    """Apply the coordinator's durable decision to locally in-doubt records."""
    decision_url = (request.get_json(silent=True) or {}).get("decision_url")
    if not decision_url:
        return jsonify({"error": "decision_url is required"}), 400
    try:
        import requests
        with connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT tx_id, state FROM distributed_transactions WHERE state IN ('PREPARED','PRECOMMITTED','COMMITTED') ORDER BY updated_at")
            pending = cur.fetchall()
        actions = []
        for tx_id, local_state in pending:
            response = requests.get(f"{decision_url.rstrip('/')}/transactions/{tx_id}", timeout=3)
            if response.status_code == 404:
                continue
            response.raise_for_status()
            decision = response.json().get("decision")
            if decision == "COMMIT":
                if local_state == "PREPARED" and PROTOCOL == "3pc":
                    requests.post(f"{request.host_url.rstrip('/')}/precommit", json={"tx_id": tx_id}, timeout=3).raise_for_status()
                if local_state != "COMMITTED":
                    requests.post(f"{request.host_url.rstrip('/')}/commit", json={"tx_id": tx_id}, timeout=3).raise_for_status()
                requests.post(f"{request.host_url.rstrip('/')}/complete", json={"tx_id": tx_id}, timeout=3).raise_for_status()
                actions.append({"tx_id": tx_id, "action": "COMMIT"})
            elif decision == "ABORT":
                requests.post(f"{request.host_url.rstrip('/')}/abort", json={"tx_id": tx_id}, timeout=3).raise_for_status()
                actions.append({"tx_id": tx_id, "action": "ABORT"})
        return jsonify({"recovered": actions}), 200
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


if __name__ == '__main__':
    ensure_schema()
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 6001
    app.run(host="0.0.0.0", port=port)
