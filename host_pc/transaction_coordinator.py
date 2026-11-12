"""Shared durable 2PC/3PC coordinator and small HTTP decision log."""
import json
import os
import sqlite3
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import requests

PARTICIPANTS = [x.rstrip("/") for x in os.getenv(
    "TRANSACTION_PARTICIPANTS", "http://192.168.56.104:6001,http://192.168.56.107:6002"
).split(",") if x.strip()]
LOG_PATH = os.getenv("COORDINATOR_LOG", os.path.join(os.path.dirname(__file__), "transactions.sqlite3"))
DECISION_PORT = int(os.getenv("COORDINATOR_DECISION_PORT", "6100"))
TIMEOUT = float(os.getenv("TRANSACTION_TIMEOUT", "3"))
_server = None


def _db():
    conn = sqlite3.connect(LOG_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS transactions (
        tx_id TEXT PRIMARY KEY, protocol TEXT NOT NULL, slice_id TEXT NOT NULL,
        slice_type TEXT NOT NULL, decision TEXT NOT NULL, state TEXT NOT NULL,
        participants TEXT NOT NULL, started REAL NOT NULL, ended REAL,
        latency_ms REAL, error TEXT)""")
    return conn


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        tx_id = urlparse(self.path).path.rsplit("/", 1)[-1]
        with _db() as conn:
            row = conn.execute("SELECT decision,state FROM transactions WHERE tx_id=?", (tx_id,)).fetchone()
        if not row:
            self._reply(404, {"status": "NOT_FOUND"})
        else:
            self._reply(200, {"tx_id": tx_id, "decision": row[0], "state": row[1]})

    def _reply(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


def start_decision_service():
    global _server
    if _server is None:
        _server = ThreadingHTTPServer(("0.0.0.0", DECISION_PORT), _Handler)
        threading.Thread(target=_server.serve_forever, daemon=True).start()


def _record(tx_id, protocol, data, decision, state, participants, started, ended=None, latency=None, error=None):
    with _db() as conn:
        conn.execute("""INSERT INTO transactions
            (tx_id,protocol,slice_id,slice_type,decision,state,participants,started,ended,latency_ms,error)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(tx_id) DO UPDATE SET decision=excluded.decision,state=excluded.state,
            ended=excluded.ended,latency_ms=excluded.latency_ms,error=excluded.error""",
            (tx_id, protocol, data["slice_id"], data["slice_type"], decision, state,
             json.dumps(participants), started, ended, latency, error))


def _post(node, endpoint, tx_id, data=None):
    response = requests.post(f"{node}/{endpoint}", json={"tx_id": tx_id, **(data or {})}, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


def execute(protocol, slice_data, participants=None):
    protocol = protocol.lower()
    if protocol not in ("2pc", "3pc"):
        raise ValueError("protocol must be '2pc' or '3pc'")
    participants = list(participants or PARTICIPANTS)
    if not participants:
        raise ValueError("At least one participant is required")
    if not slice_data.get("slice_id") or not slice_data.get("slice_type"):
        raise ValueError("slice_data requires slice_id and slice_type")
    start_decision_service()
    tx_id, started = str(uuid.uuid4()), time.perf_counter()
    _record(tx_id, protocol, slice_data, "UNDECIDED", "PREPARING", participants, started)
    try:
        for node in participants:
            result = _post(node, "prepare", tx_id, {"slice": slice_data})
            if result.get("status") != "PREPARED":
                raise RuntimeError(f"{node} rejected prepare: {result}")
        if protocol == "3pc":
            for node in participants:
                result = _post(node, "precommit", tx_id)
                if result.get("status") != "PRECOMMITTED":
                    raise RuntimeError(f"{node} rejected precommit: {result}")
        # Persist the decision before dispatching commit so recovery can finish it.
        _record(tx_id, protocol, slice_data, "COMMIT", "COMMIT_DECIDED", participants, started)
        for node in participants:
            result = _post(node, "commit", tx_id)
            if result.get("status") != "COMMITTED":
                raise RuntimeError(f"{node} failed commit: {result}")
        for node in participants:
            result = _post(node, "complete", tx_id)
            if result.get("status") != "COMPLETED":
                raise RuntimeError(f"{node} failed completion: {result}")
        ended = time.perf_counter()
        latency = (ended - started) * 1000
        _record(tx_id, protocol, slice_data, "COMMIT", "COMPLETED", participants, started, ended, latency)
        return {"tx_id": tx_id, "protocol": protocol, "status": "COMPLETED", "latency_ms": latency}
    except Exception as exc:
        with _db() as conn:
            row = conn.execute("SELECT decision FROM transactions WHERE tx_id=?", (tx_id,)).fetchone()
        decision = row[0] if row else "UNDECIDED"
        if decision == "UNDECIDED":
            _record(tx_id, protocol, slice_data, "ABORT", "ABORT_DECIDED", participants, started, error=str(exc))
            for node in participants:
                try:
                    _post(node, "abort", tx_id)
                except Exception:
                    pass
        raise RuntimeError(f"{protocol} transaction {tx_id} failed ({decision}): {exc}") from exc


def recover(participants=None):
    """Retry durable COMMIT decisions and resend ABORT to the recorded participant set."""
    start_decision_service()
    with _db() as conn:
        rows = conn.execute("SELECT tx_id,decision,state,protocol,participants FROM transactions WHERE state!='COMPLETED' AND state!='ABORTED'").fetchall()
    outcomes = []
    for tx_id, decision, state, protocol, raw_participants in rows:
        targets = participants or json.loads(raw_participants)
        ok, errors = True, []
        for node in targets:
            try:
                if decision == "COMMIT":
                    status = requests.get(f"{node}/transactions/{tx_id}", timeout=TIMEOUT)
                    if status.status_code == 404:
                        # Participant may not have received prepare; coordinator keeps the decision for inspection.
                        raise RuntimeError("participant has no durable transaction record")
                    local_state = status.json().get("state")
                    if local_state == "PREPARED" and protocol == "3pc":
                        _post(node, "precommit", tx_id)
                    if local_state not in ("COMMITTED", "COMPLETED"):
                        _post(node, "commit", tx_id)
                    _post(node, "complete", tx_id)
                elif decision == "ABORT":
                    _post(node, "abort", tx_id)
                else:
                    continue
            except Exception as exc:
                ok = False
                errors.append(f"{node}: {exc}")
        if ok:
            with _db() as conn:
                conn.execute("UPDATE transactions SET state=? WHERE tx_id=?", ("COMPLETED" if decision == "COMMIT" else "ABORTED", tx_id))
        outcomes.append({"tx_id": tx_id, "decision": decision, "recovered": ok, "errors": errors})
    return outcomes


def run_experiment(protocol, count=20, failure_after_prepare=False):
    rows = []
    for index in range(count):
        participants = list(PARTICIPANTS)
        if failure_after_prepare and index == count // 2 and len(participants) > 1:
            # unreachable participant triggers prepare failure, exercising abort and cleanup
            participants[-1] = "http://127.0.0.1:1"
        payload = {"slice_id": f"{protocol}_{uuid.uuid4().hex[:12]}", "slice_type": "eMBB"}
        started = time.perf_counter()
        try:
            outcome = execute(protocol, payload, participants)
            rows.append({"ok": True, "latency_ms": outcome["latency_ms"]})
        except Exception as exc:
            rows.append({"ok": False, "latency_ms": (time.perf_counter()-started)*1000, "error": str(exc)})
    duration_s = sum(row["latency_ms"] for row in rows) / 1000
    successes = sum(row["ok"] for row in rows)
    latency_success = [row["latency_ms"] for row in rows if row["ok"]]
    return {"protocol": protocol, "attempts": count, "successes": successes,
            "failures": count-successes, "elapsed_transaction_time_s": duration_s,
            "throughput_tx_per_s": successes / duration_s if duration_s else 0,
            "mean_latency_ms": sum(latency_success)/len(latency_success) if latency_success else None,
            "availability_success_rate": successes/count if count else 0, "results": rows}
