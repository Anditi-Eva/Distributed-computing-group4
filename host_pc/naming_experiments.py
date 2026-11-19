"""Milestone 10 name-resolution latency, scale, and process lifecycle experiments.

Run from host_pc/: python naming_experiments.py --sizes 10,100,500,1000
The naming-load namespace is deleted after the run. Managed-process trials are
terminated through the logical name, so no worker address is needed by clients.
"""
import argparse
import csv
import json
import os
import statistics
import time
import uuid
from urllib.parse import quote

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
OUT = os.getenv("MILESTONE10_RESULTS_DIR", os.path.join(ROOT, "results", "milestone10"))
GATEWAY = os.getenv("GATEWAY_URL", "http://127.0.0.1:5100").rstrip("/")


def pct(values, q):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * q))] if ordered else None


def measure_scale(sizes, run_id):
    rows, registered = [], []
    previous = 0
    prefix = f"service/bench-{run_id}"
    try:
        for size in sizes:
            additions = [{"name": f"{prefix}/item-{index:06d}",
                          "resource_id": f"benchmark-{run_id}-{index}",
                          "kind": "service",
                          "logical_uri": f"/api/bench-resources/{run_id}/{index}"}
                         for index in range(previous, size)]
            started = time.perf_counter()
            response = requests.post(f"{GATEWAY}/api/names/register",
                                     json={"records": additions, "include_records": False}, timeout=20)
            insert_ms = (time.perf_counter() - started) * 1000
            response.raise_for_status()
            registered.extend(record["name"] for record in additions)

            sample_count = min(size, 100)
            indexes = sorted({(i * (size - 1)) // max(1, sample_count - 1)
                              for i in range(sample_count)})
            lookup_ms = []
            resolved = 0
            for index in indexes:
                name = f"{prefix}/item-{index:06d}"
                started = time.perf_counter()
                result = requests.get(f"{GATEWAY}/api/names/resolve/{quote(name, safe='/')}", timeout=5)
                lookup_ms.append((time.perf_counter() - started) * 1000)
                resolved += result.ok

            started = time.perf_counter()
            listing = requests.get(f"{GATEWAY}/api/names", params={"prefix": prefix}, timeout=10)
            prefix_ms = (time.perf_counter() - started) * 1000
            listing.raise_for_status()
            rows.append({"registry_size": size, "new_registrations": len(additions),
                         "registration_batch_ms": insert_ms, "sampled_lookups": len(indexes),
                         "resolved_lookups": resolved,
                         "resolve_mean_ms": statistics.mean(lookup_ms) if lookup_ms else None,
                         "resolve_p50_ms": pct(lookup_ms, .50), "resolve_p95_ms": pct(lookup_ms, .95),
                         "prefix_list_ms": prefix_ms,
                         "prefix_list_count": len(listing.json().get("names", []))})
            previous = size
    finally:
        requests.delete(f"{GATEWAY}/api/names", params={"prefix": prefix}, timeout=10)
    return rows


def process_lifecycle(count, run_id):
    rows = []
    for index in range(count):
        process_id = f"m10-{run_id}-{index}"
        logical_name = f"process/{process_id}"
        payload = {"pid": process_id, "name": "managed-slice", "slice_type": "eMBB",
                   "logical_name": logical_name, "cpu_needed": 1, "memory_needed": 1,
                   "remaining_burst": 100000}
        started = time.perf_counter()
        created = requests.post(f"{GATEWAY}/api/processes", json=payload, timeout=10)
        create_ms = (time.perf_counter() - started) * 1000
        if not created.ok:
            rows.append({"process_id": process_id, "created": False, "error": created.text})
            continue
        creation = created.json()
        name_path = quote(logical_name, safe="/")

        started = time.perf_counter()
        resolved = requests.get(f"{GATEWAY}/api/names/resolve/{name_path}", timeout=10)
        resolve_ms = (time.perf_counter() - started) * 1000

        started = time.perf_counter()
        located = requests.get(f"{GATEWAY}/api/processes/{name_path}", timeout=10)
        locate_ms = (time.perf_counter() - started) * 1000

        started = time.perf_counter()
        terminated = requests.post(f"{GATEWAY}/api/processes/{name_path}/terminate", timeout=10)
        terminate_ms = (time.perf_counter() - started) * 1000

        rows.append({"process_id": process_id, "created": True, "create_ms": create_ms,
                     "name_register_ms": creation.get("phase_ms", {}).get("name_resolution_registration"),
                     "placement_locate_ms": creation.get("phase_ms", {}).get("locate"),
                     "schedule_ms": creation.get("phase_ms", {}).get("schedule"),
                     "coordinate_ms": creation.get("phase_ms", {}).get("coordinate"),
                     "gateway_total_ms": creation.get("phase_ms", {}).get("total"),
                     "resolve_ms": resolve_ms, "resolved": resolved.ok,
                     "locate_ms": locate_ms, "located": located.ok,
                     "terminate_ms": terminate_ms, "terminated": terminated.ok,
                     "termination_acks": terminated.json().get("coordination_acks") if terminated.ok else 0,
                     "lifecycle": creation.get("lifecycle")})
    return rows


def write_csv(path, rows):
    if not rows:
        return
    fields = list(dict.fromkeys(key for row in rows for key, value in row.items()
                                if not isinstance(value, (list, dict))))
    with open(path, "w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description="Milestone 10 distributed naming/process-management evaluation")
    parser.add_argument("--sizes", default="10,100,500,1000", help="cumulative naming table sizes")
    parser.add_argument("--lifecycle-count", type=int, default=20)
    args = parser.parse_args()
    sizes = [int(value) for value in args.sizes.split(",") if value.strip()]
    if not sizes or any(size < 1 for size in sizes) or args.lifecycle_count < 1:
        parser.error("sizes and lifecycle-count must be positive")
    sizes = sorted(set(sizes))
    os.makedirs(OUT, exist_ok=True)
    run_id = uuid.uuid4().hex[:10]
    naming_rows = measure_scale(sizes, run_id)
    lifecycle_rows = process_lifecycle(args.lifecycle_count, run_id)
    write_csv(os.path.join(OUT, "naming_scalability.csv"), naming_rows)
    write_csv(os.path.join(OUT, "process_lifecycle.csv"), lifecycle_rows)

    all_lookup = [row["resolve_mean_ms"] for row in naming_rows if row["resolve_mean_ms"] is not None]
    successful = [row for row in lifecycle_rows if row.get("created")]
    summary = {
        "run_id": run_id,
        "naming_scalability": naming_rows,
        "process_trials": len(lifecycle_rows),
        "created": sum(bool(row.get("created")) for row in lifecycle_rows),
        "resolved": sum(bool(row.get("resolved")) for row in successful),
        "located": sum(bool(row.get("located")) for row in successful),
        "terminated": sum(bool(row.get("terminated")) for row in successful),
        "mean_create_ms": statistics.mean(row["create_ms"] for row in successful) if successful else None,
        "mean_resolve_ms": statistics.mean(row["resolve_ms"] for row in successful if row.get("resolved"))
        if any(row.get("resolved") for row in successful) else None,
        "mean_terminate_ms": statistics.mean(row["terminate_ms"] for row in successful
                                              if row.get("terminated"))
        if any(row.get("terminated") for row in successful) else None,
        "largest_registry_size": max(sizes),
        "mean_sampled_lookup_ms_across_sizes": statistics.mean(all_lookup) if all_lookup else None,
        "results_dir": OUT,
    }
    with open(os.path.join(OUT, "summary.json"), "w", encoding="utf-8") as output:
        json.dump(summary, output, indent=2)
    print(json.dumps(summary, indent=2))
    print(f"Results: {OUT}")


if __name__ == "__main__":
    main()
