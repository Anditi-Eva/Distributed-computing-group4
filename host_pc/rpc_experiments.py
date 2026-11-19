"""Milestone 11 RPC and replicated shared-state communication benchmarks."""
import argparse
import concurrent.futures
import csv
import json
import os
import statistics
import time
import uuid

from rpc_client import RpcClient, RpcError

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
OUT = os.getenv("MILESTONE11_RESULTS_DIR", os.path.join(ROOT, "results", "milestone11"))
ENDPOINT = os.getenv("RPC_ENDPOINT", "http://127.0.0.1:5100/rpc/call")


def percentile(values, q):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * q))] if ordered else None


def rpc_call(method, params=None):
    return RpcClient(ENDPOINT, timeout=8).call(method, params)


def write_csv(path, rows):
    if not rows:
        return
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with open(path, "w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description="RPC latency, throughput, consistency and message overhead")
    parser.add_argument("--ping-requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--state-writes", type=int, default=30)
    parser.add_argument("--value-bytes", type=int, default=128)
    args = parser.parse_args()
    if min(args.ping_requests, args.concurrency, args.state_writes, args.value_bytes) < 1:
        parser.error("all numeric arguments must be positive")
    os.makedirs(OUT, exist_ok=True)
    run_id = uuid.uuid4().hex[:10]
    measurement_rows, consistency_rows = [], []

    def ping(index):
        response = rpc_call("rpc.invoke", {"name": "worker/edge", "method": "system.ping",
                                             "params": {"echo": index}})
        result = response["result"]
        return {"operation": "rpc_ping", "trial": index, "latency_ms": response["latency_ms"],
                "messages": response.get("message_count"), "message_bytes": response.get("message_bytes"),
                "ok": result.get("echo") == index}

    ping_started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        ping_rows = list(pool.map(ping, range(args.ping_requests)))
    ping_elapsed = time.perf_counter() - ping_started
    measurement_rows.extend(ping_rows)

    key = f"m11/{run_id}/shared-counter"
    initial = rpc_call("state.put", {"key": key, "value": {"sequence": 0}, "expected_version": 0})
    current_version = initial["result"]["version"]
    write_latencies, write_messages, write_bytes = [], [], []
    write_started_all = time.perf_counter()
    consistent_reads = 0
    for index in range(1, args.state_writes + 1):
        value = {"sequence": index, "payload": "x" * args.value_bytes}
        started = time.perf_counter()
        write = rpc_call("state.put", {"key": key, "value": value,
                                        "expected_version": current_version})
        latency = (time.perf_counter() - started) * 1000
        current_version = write["result"]["version"]
        write_latencies.append(latency)
        write_messages.append(write.get("message_count") or 0)
        write_bytes.append(write.get("message_bytes") or 0)

        replicas = []
        for worker_name in ("worker/edge", "worker/core"):
            direct = rpc_call("rpc.invoke", {"name": worker_name, "method": "state.get",
                                              "params": {"key": key}})["result"]
            replicas.append(direct)
        same = all(row["version"] == current_version and row["value"] == value for row in replicas)
        consistent_reads += same
        consistency_rows.append({"trial": index, "coordinator_version": current_version,
                                 "edge_version": replicas[0]["version"], "core_version": replicas[1]["version"],
                                 "replicas_match": same})
        measurement_rows.append({"operation": "state_put", "trial": index, "latency_ms": latency,
                                 "messages": write.get("message_count"),
                                 "message_bytes": write.get("message_bytes"), "ok": same})
    write_elapsed = time.perf_counter() - write_started_all

    # Competing compare-and-set writes at one version must admit exactly one.
    race_version = current_version
    def cas(value):
        try:
            return rpc_call("state.put", {"key": key, "value": {"race": value},
                                           "expected_version": race_version})
        except RpcError as exc:
            return exc
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        race_results = list(pool.map(cas, ("a", "b")))
    race_successes = sum(isinstance(item, dict) for item in race_results)
    race_conflicts = sum(isinstance(item, RpcError) for item in race_results)

    final_read = rpc_call("state.get", {"key": key})["result"]
    edge_response = rpc_call("rpc.invoke", {"name": "worker/edge", "method": "state.get",
                                              "params": {"key": key}})
    core_read = rpc_call("rpc.invoke", {"name": "worker/core", "method": "state.get",
                                         "params": {"key": key}})["result"]
    edge_read = edge_response["result"]
    final_consistent = (final_read["version"] == edge_read["version"] == core_read["version"] and
                        edge_read["value"] == core_read["value"])
    delete = rpc_call("state.delete", {"key": key, "expected_version": final_read["version"]})["result"]
    edge_deleted = rpc_call("rpc.invoke", {"name": "worker/edge", "method": "state.get",
                                            "params": {"key": key}})["result"]
    core_deleted = rpc_call("rpc.invoke", {"name": "worker/core", "method": "state.get",
                                            "params": {"key": key}})["result"]
    tombstone_consistent = (edge_deleted["deleted"] and core_deleted["deleted"] and
                            edge_deleted["version"] == core_deleted["version"] == delete["version"])

    ping_latency = [row["latency_ms"] for row in ping_rows]
    summary = {
        "run_id": run_id,
        "rpc_ping": {"requests": args.ping_requests, "concurrency": args.concurrency,
                      "successes": sum(row["ok"] for row in ping_rows),
                      "throughput_calls_per_s": args.ping_requests / ping_elapsed,
                      "latency_mean_ms": statistics.mean(ping_latency),
                      "latency_p50_ms": percentile(ping_latency, .50),
                      "latency_p95_ms": percentile(ping_latency, .95)},
        "shared_state": {"writes": args.state_writes, "replica_consistency_checks": consistent_reads,
                         "final_read_consistent": final_consistent,
                         "cas_race_successes": race_successes, "cas_race_conflicts": race_conflicts,
                         "race_consistency_ok": race_successes == 1 and race_conflicts == 1,
                         "tombstone_consistency_ok": tombstone_consistent,
                         "write_latency_mean_ms": statistics.mean(write_latencies),
                         "write_latency_p95_ms": percentile(write_latencies, .95),
                         "write_throughput_ops_per_s": args.state_writes / write_elapsed,
                         "direct_replica_read_ms": edge_response["latency_ms"],
                         "estimated_synchronization_overhead_ms": statistics.mean(write_latencies) - edge_response["latency_ms"],
                         "average_write_message_count": statistics.mean(write_messages),
                         "average_write_message_bytes": statistics.mean(write_bytes),
                         "deleted_tombstone_version": delete["version"],
                         "consistency_model": "synchronous all-reachable replicas through one serialized gateway"},
        "limits": "Single gateway serializes writes. Results characterize this two-worker Compose host, not a production cluster.",
    }
    write_csv(os.path.join(OUT, "rpc_measurements.csv"), measurement_rows)
    write_csv(os.path.join(OUT, "consistency_trials.csv"), consistency_rows)
    with open(os.path.join(OUT, "summary.json"), "w", encoding="utf-8") as output:
        json.dump(summary, output, indent=2)
    print(json.dumps(summary, indent=2))
    print(f"Results: {OUT}")


if __name__ == "__main__":
    main()
