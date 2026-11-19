"""Milestone 9 experiments against the Docker Compose deployment.

Run from host_pc/: python transparency_experiments.py --requests 30
Optional destructive fault injection is explicit: --inject-failure stops and
restarts the worker that owns one long-running process using Docker Compose.
"""
import argparse
import csv
import json
import os
import statistics
import subprocess
import time
import uuid

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
OUT = os.getenv("MILESTONE9_RESULTS_DIR", os.path.join(ROOT, "results", "milestone9"))
GATEWAY = os.getenv("GATEWAY_URL", "http://127.0.0.1:5100")
DIRECT = os.getenv("DIRECT_WORKER_URL", "http://127.0.0.1:5001")
NODES = {}
for entry in os.getenv(
    "TRANSPARENCY_WORKERS", "edge=http://127.0.0.1:5001,core=http://127.0.0.1:5002"
).split(","):
    if "=" in entry:
        name, url = entry.split("=", 1)
        NODES[name.strip()] = url.strip().rstrip("/")


def percentile(values, q):
    if not values:
        return None
    xs = sorted(values)
    return xs[min(len(xs) - 1, int((len(xs) - 1) * q))]


def submit(url, pid, burst=2):
    payload = {"pid": pid, "name": "milestone9", "cpu_needed": 1,
               "memory_needed": 1, "remaining_burst": burst}
    start = time.perf_counter()
    response = requests.post(url, json=payload, timeout=4)
    elapsed_ms = (time.perf_counter() - start) * 1000
    return response, elapsed_ms


def wait_for_completion(pid, through_gateway, timeout=8.0):
    """Keep samples from saturating the small demo cluster and biasing latency."""
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        if through_gateway:
            try:
                response = requests.get(f"{GATEWAY}/api/slices/{pid}", timeout=1)
                if response.ok and response.json().get("status") == "completed":
                    return True
            except requests.RequestException:
                pass
        else:
            for base in NODES.values():
                try:
                    response = requests.get(f"{base}/api/process/{pid}", timeout=1)
                    if response.ok and response.json().get("status") == "completed":
                        return True
                except requests.RequestException:
                    pass
        time.sleep(0.1)
    return False


def main():
    parser = argparse.ArgumentParser(description="Transparency, replication and overhead evaluation")
    parser.add_argument("--requests", type=int, default=30, help="paired gateway/direct submissions")
    parser.add_argument("--inject-failure", action="store_true",
                        help="stop and restart one Compose worker to measure replica promotion")
    args = parser.parse_args()
    if args.requests < 1:
        parser.error("--requests must be positive")

    os.makedirs(OUT, exist_ok=True)
    records = []
    replication_acks = 0
    location_hidden = True
    gateway_latencies, direct_latencies = [], []

    for i in range(args.requests):
        direct_id = f"m9-direct-{uuid.uuid4().hex[:10]}"
        direct, dt = submit(f"{DIRECT}/api/submit", direct_id)
        if direct.ok:
            wait_for_completion(direct_id, through_gateway=False)
        gateway_id = f"m9-logical-{uuid.uuid4().hex[:10]}"
        gateway, gt = submit(f"{GATEWAY}/api/slices", gateway_id)
        if gateway.ok:
            wait_for_completion(gateway_id, through_gateway=True)
        direct_latencies.append(dt)
        gateway_latencies.append(gt)
        if gateway.ok:
            body = gateway.json()
            replication_acks += body.get("replica_count", 0) > 0
            location_hidden &= not any(key in body for key in ("node", "node_id", "url", "worker"))
        records.append({"trial": i + 1, "direct_ms": dt, "gateway_ms": gt,
                        "direct_ok": direct.ok, "gateway_ok": gateway.ok,
                        "replica_count": gateway.json().get("replica_count", 0) if gateway.ok else 0})

    # Force the existing edge overflow path (edge capacity is 4 CPU units;
    # core capacity is 8) and resolve the moved work only by its logical ID.
    migration_id = f"m9-migration-{uuid.uuid4().hex[:10]}"
    migration_payload = {"pid": migration_id, "name": "m9-overflow", "cpu_needed": 5,
                         "memory_needed": 1, "remaining_burst": 100000}
    migration_response = requests.post(f"{DIRECT}/api/submit", json=migration_payload, timeout=4)
    migration_ack = migration_response.json() if migration_response.ok else {}
    logical_lookup = requests.get(f"{GATEWAY}/api/slices/{migration_id}", timeout=5)
    migration_result = {"edge_submission_accepted": migration_response.ok,
                        "peer_overflow_used": "Offloaded to peer" in migration_ack.get("message", ""),
                        "logical_lookup_succeeded": logical_lookup.ok,
                        "logical_status": logical_lookup.json().get("status") if logical_lookup.ok else None,
                        "resource_id": migration_id}

    fault_result = {"attempted": False}
    if args.inject_failure:
        pid = f"m9-failover-{uuid.uuid4().hex[:10]}"
        response, _ = submit(f"{GATEWAY}/api/slices", pid, burst=100000)
        if not response.ok:
            raise RuntimeError(f"could not prepare failover workload: {response.text}")
        owner = None
        for name, base in NODES.items():
            try:
                current = requests.get(f"{base}/api/process/{pid}", timeout=1)
                if current.ok and current.json().get("status") == "running":
                    owner = name
                    break
            except requests.RequestException:
                continue
        if owner is None:
            raise RuntimeError("could not identify active owner for controlled failure")
        try:
            subprocess.run(["docker", "compose", "stop", owner], cwd=ROOT, check=True,
                           capture_output=True, text=True)
            time.sleep(1)
            started = time.perf_counter()
            recovered = requests.get(f"{GATEWAY}/api/slices/{pid}", timeout=8)
            recovery_ms = (time.perf_counter() - started) * 1000
            fault_result = {"attempted": True, "worker": owner, "recovered": recovered.ok,
                            "recovered_status": recovered.json().get("status") if recovered.ok else None,
                            "recovery_ms": recovery_ms}
        finally:
            subprocess.run(["docker", "compose", "start", owner], cwd=ROOT, check=False,
                           capture_output=True, text=True)

    overhead_ms = statistics.mean(gateway_latencies) - statistics.mean(direct_latencies)
    summary = {
        "requests": args.requests,
        "direct_successes": sum(row["direct_ok"] for row in records),
        "logical_gateway_successes": sum(row["gateway_ok"] for row in records),
        "logical_location_hidden": location_hidden,
        "replicated_submissions": replication_acks,
        "direct_latency_ms": {"mean": statistics.mean(direct_latencies),
                              "p50": percentile(direct_latencies, .50),
                              "p95": percentile(direct_latencies, .95)},
        "gateway_latency_ms": {"mean": statistics.mean(gateway_latencies),
                               "p50": percentile(gateway_latencies, .50),
                               "p95": percentile(gateway_latencies, .95)},
        "gateway_overhead_ms": overhead_ms,
        "gateway_overhead_pct": (overhead_ms / statistics.mean(direct_latencies) * 100
                                  if statistics.mean(direct_latencies) else None),
        "migration_experiment": migration_result,
        "replica_failover": fault_result,
        "notes": "Gateway/direct timings are host-local; replication copies on admission and replays accepted work after owner loss.",
    }
    with open(os.path.join(OUT, "transparency_requests.csv"), "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    with open(os.path.join(OUT, "summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)
    print(json.dumps(summary, indent=2))
    print(f"Results: {OUT}")


if __name__ == "__main__":
    main()
