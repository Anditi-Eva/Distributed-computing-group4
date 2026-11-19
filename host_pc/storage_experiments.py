"""Milestone 12 replicated file-store latency, throughput and integrity benchmark."""
import argparse
import concurrent.futures
import csv
import json
import os
import statistics
import time
import uuid

import requests


GATEWAY = os.getenv("STORAGE_GATEWAY", "http://127.0.0.1:5100")
OUT = os.getenv("MILESTONE12_RESULTS_DIR", os.path.join(os.path.dirname(__file__), "..", "results", "milestone12"))


def percentile(values, q):
    values = sorted(values)
    return values[min(len(values) - 1, int((len(values) - 1) * q))] if values else None


def main():
    parser = argparse.ArgumentParser(description="Benchmark replicated telecom slice file storage")
    parser.add_argument("--files", type=int, default=20)
    parser.add_argument("--file-bytes", type=int, default=4096)
    args = parser.parse_args()
    if args.files < 2 or args.file_bytes < 1 or args.file_bytes > 4 * 1024 * 1024:
        parser.error("files must be >=2; file-bytes must be in [1, 4194304]")
    os.makedirs(OUT, exist_ok=True)
    run = uuid.uuid4().hex[:10]
    session = requests.Session()
    rows, upload_latencies, read_latencies = [], [], []
    successful_bytes = 0
    started_all = time.perf_counter()

    for index in range(args.files):
        path = f"milestone12/{run}/slice-{index:04d}.bin"
        seed = f"slice={index};run={run};".encode()
        content = (seed * ((args.file_bytes // len(seed)) + 1))[:args.file_bytes]
        started = time.perf_counter()
        uploaded = session.put(f"{GATEWAY}/api/files/{path}", data=content,
                               headers={"Content-Type": "application/octet-stream"}, timeout=20)
        upload_ms = (time.perf_counter() - started) * 1000
        if uploaded.status_code != 201:
            raise RuntimeError(f"upload failed for {path}: {uploaded.status_code} {uploaded.text}")
        metadata = uploaded.json()
        upload_latencies.append(upload_ms)
        started = time.perf_counter()
        downloaded = session.get(f"{GATEWAY}/api/files/{path}", timeout=20)
        read_ms = (time.perf_counter() - started) * 1000
        valid = downloaded.status_code == 200 and downloaded.content == content
        if not valid:
            raise RuntimeError(f"read/checksum validation failed for {path}")
        read_latencies.append(read_ms)
        successful_bytes += len(content)
        rows.append({"path": path, "size_bytes": len(content), "version": metadata["version"],
                     "replicas": metadata["replica_count"], "upload_ms": upload_ms,
                     "read_ms": read_ms, "upload_messages": int(uploaded.headers.get("X-Storage-Message-Count", 0)),
                     "upload_message_bytes": int(uploaded.headers.get("X-Storage-Message-Bytes", 0)),
                     "read_messages": int(downloaded.headers.get("X-Storage-Message-Count", 0)),
                     "read_message_bytes": int(downloaded.headers.get("X-Storage-Message-Bytes", 0)),
                     "checksum_ok": valid})

    # Confirm hierarchical resource names resolve to the gateway's logical file URL.
    resolve = session.get(f"{GATEWAY}/api/names/resolve/file/milestone12/{run}/slice-0000.bin", timeout=10)
    if resolve.status_code != 200 or resolve.json().get("kind") != "file":
        raise RuntimeError(f"file name did not resolve: {resolve.status_code} {resolve.text}")

    # Two writers use the same ETag. Gateway serialization and version checks must admit one.
    race_path = f"milestone12/{run}/cas-race.bin"
    first = session.put(f"{GATEWAY}/api/files/{race_path}", data=b"initial", timeout=20)
    version = first.json()["version"]
    def race_write(body):
        with requests.Session() as concurrent_session:
            return concurrent_session.put(f"{GATEWAY}/api/files/{race_path}", data=body,
                                          headers={"If-Match": f'"{version}"'}, timeout=20)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        race_responses = list(pool.map(race_write, (b"writer-a", b"writer-b")))
    cas_successes = sum(response.status_code == 201 for response in race_responses)
    cas_conflicts = sum(response.status_code == 409 for response in race_responses)
    if (cas_successes, cas_conflicts) != (1, 1):
        raise RuntimeError(f"CAS race expected one success and one conflict, got {cas_successes}/{cas_conflicts}")

    listed = session.get(f"{GATEWAY}/api/files", params={"prefix": f"milestone12/{run}"}, timeout=20)
    if listed.status_code != 200:
        raise RuntimeError(f"listing failed: {listed.status_code} {listed.text}")
    listed_paths = {item["path"] for item in listed.json()["files"]}
    if len(listed_paths) != args.files + 1:
        raise RuntimeError(f"listing returned {len(listed_paths)} files, expected {args.files + 1}")

    elapsed = time.perf_counter() - started_all
    message_counts = [row["upload_messages"] for row in rows]
    message_bytes = [row["upload_message_bytes"] for row in rows]
    summary = {
        "run_id": run,
        "storage_nodes": session.get(f"{GATEWAY}/api/storage/status", timeout=10).json(),
        "files_uploaded_and_verified": args.files,
        "file_size_bytes": args.file_bytes,
        "total_verified_payload_bytes": successful_bytes,
        "upload_latency_mean_ms": statistics.mean(upload_latencies),
        "upload_latency_p95_ms": percentile(upload_latencies, .95),
        "download_latency_mean_ms": statistics.mean(read_latencies),
        "download_latency_p95_ms": percentile(read_latencies, .95),
        "end_to_end_throughput_files_per_s": args.files / elapsed,
        "average_upload_messages": statistics.mean(message_counts),
        "average_upload_message_bytes": statistics.mean(message_bytes),
        "checksum_integrity_successes": sum(row["checksum_ok"] for row in rows),
        "name_resolution_ok": True,
        "listing_count": len(listed_paths),
        "cas_race_successes": cas_successes,
        "cas_race_conflicts": cas_conflicts,
        "consistency": "versioned, gateway-serialized two-phase replication to currently reachable workers",
        "limits": "Single gateway and local Docker worker volumes; not a multi-site HDFS/NFS/cloud-provider benchmark.",
    }
    with open(os.path.join(OUT, "storage_measurements.csv"), "w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    with open(os.path.join(OUT, "summary.json"), "w", encoding="utf-8") as output:
        json.dump(summary, output, indent=2)
    print(json.dumps(summary, indent=2))
    print(f"Results: {OUT}")


if __name__ == "__main__":
    main()
