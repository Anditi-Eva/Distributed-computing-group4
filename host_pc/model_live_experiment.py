#!/usr/bin/env python3
"""
Milestone 8 - live experiment on the real prototype (Edge VM + Core VM).

Runs the SAME slice workload (slices.csv) through five dispatch strategies, each
one implementing a distributed system model on top of the worker_node.py REST API:

  Host-based      every request goes to ONE node (the Core VM); the Edge VM idles
  Workstation     users are statically partitioned between the nodes; no sharing
  Server-based    Milestone-3 rule: mMTC -> Edge, eMBB/URLLC -> Core; no sharing
  Processor pool  a dispatcher asks both nodes for free CPU and picks the freer one
  Integrated      class affinity (URLLC+mMTC -> Edge, eMBB -> Core) + peer overflow
                  (/api/submit lets the worker migrate) + health-aware failover

All baselines call /api/migrate so a node can never silently offload to its peer;
only the Integrated model uses /api/submit (which enables the built-in migration).

Measured per model: accepted / rejected / unreachable, end-to-end latency
(client RTT + node queue + service), throughput, CPU/memory utilisation per node
(sampled from /api/status). With --failure the Core node is treated as crashed
for the middle third of the run, so you can compare how each model degrades.

Prerequisites (on each VM, Milestone-8 worker_node.py):
    SCHED_TICK=0.05 python3 worker_node.py "edge" 5001 4 1024 <core-ip> 5002
    SCHED_TICK=0.05 python3 worker_node.py "core" 5002 8 2048 <edge-ip> 5001
(SCHED_TICK shortens the 1 s scheduler quantum so the experiment finishes quickly.)

Usage on the host PC:
    set NODE1_IP=192.168.56.104 & set NODE2_IP=192.168.56.107    (PowerShell: $env:NODE1_IP=...)
    python model_live_experiment.py --requests 80 --rate 8 --repeat 3 --failure

Laptop dry-run without VMs (starts two local worker_node.py processes):
    python model_live_experiment.py --local --requests 60 --rate 8

Results are written to ../results/milestone8/live_results.{csv,json}. Report the
file you generated - these are measurements, not simulated values.
"""
import argparse
import csv
import json
import os
import statistics
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
MODELS = ["Host-based", "Processor pool", "Workstation", "Server-based", "Integrated"]

# Same slice -> process mapping as test_client.py / host_client.py
PROFILE = {"URLLC": (3, 128, 4), "eMBB": (2, 512, 12), "mMTC": (1, 64, 6)}


def load_workload(path, n):
    rows = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            if r["slice_type"] in PROFILE:
                rows.append(r)
    # Interleave classes so every run sees the same URLLC/eMBB/mMTC mix
    by = {c: [r for r in rows if r["slice_type"] == c] for c in PROFILE}
    out, i = [], 0
    while len(out) < n:
        for c in PROFILE:
            if len(out) < n:
                out.append(by[c][i % len(by[c])])
        i += 1
    return out


def make_process(row):
    cpu, mem, burst = PROFILE[row["slice_type"]]
    return {"pid": uuid.uuid4().hex[:8], "name": f"Slice-{row['slice_id']}-{row['slice_type']}",
            "cpu_needed": cpu, "memory_needed": mem, "remaining_burst": burst}


class Cluster:
    """Edge = node 0, Core = node 1. `failed` simulates a crashed node from the client's view."""

    def __init__(self, edge_url, core_url):
        self.urls = [edge_url, core_url]
        self.names = ["edge", "core"]
        self.failed = set()

    def post(self, idx, path, body, timeout=5):
        if idx in self.failed:
            raise requests.exceptions.ConnectionError(f"{self.names[idx]} is down (injected)")
        return requests.post(f"{self.urls[idx]}{path}", json=body, timeout=timeout)

    def get(self, idx, path, timeout=2):
        if idx in self.failed:
            raise requests.exceptions.ConnectionError(f"{self.names[idx]} is down (injected)")
        return requests.get(f"{self.urls[idx]}{path}", timeout=timeout)

    def alive(self, idx):
        try:
            return self.get(idx, "/api/health").status_code == 200
        except requests.exceptions.RequestException:
            return False


# ---------------------------------------------------------------- dispatch policies
def dispatch(model, cl, proc, row):
    """Return (node_index_used, http_response). Raises ConnectionError if unreachable."""
    cls, sid = row["slice_type"], int(row["slice_id"])
    if model == "Host-based":
        return 1, cl.post(1, "/api/migrate", {"process": proc})
    if model == "Workstation":
        i = sid % 2
        return i, cl.post(i, "/api/migrate", {"process": proc})
    if model == "Server-based":
        i = 0 if cls == "mMTC" else 1
        return i, cl.post(i, "/api/migrate", {"process": proc})
    if model == "Processor pool":
        free = []
        for i in (0, 1):
            try:
                st = cl.get(i, "/api/status").json()
                free.append((st["available_cpu"] / st["total_cpu"], i))
            except requests.exceptions.RequestException:
                pass
        if not free:
            raise requests.exceptions.ConnectionError("pool empty")
        i = max(free)[1]
        return i, cl.post(i, "/api/migrate", {"process": proc})
    if model == "Integrated":
        pref = 1 if cls == "eMBB" else 0
        order = [pref, 1 - pref]
        last = None
        for i in order:                      # health-aware failover
            try:
                return i, cl.post(i, "/api/submit", proc)   # /api/submit: worker may also offload to its peer
            except requests.exceptions.RequestException as e:
                last = e
        raise last
    raise ValueError(model)


# ---------------------------------------------------------------- one run
def sample_utilisation(cl, stop, store):
    while not stop.is_set():
        for i in (0, 1):
            try:
                st = cl.get(i, "/api/status").json()
                store[i].append((1 - st["available_cpu"] / st["total_cpu"],
                                 1 - st["available_memory"] / st["total_memory"]))
            except requests.exceptions.RequestException:
                pass
        time.sleep(0.2)


def run_once(model, cl, workload, rate, failure, drain_timeout):
    for i in (0, 1):
        cl.post(i, "/api/reset", {})
    n = len(workload)
    fail_lo, fail_hi = (n // 3, 2 * n // 3) if failure else (n + 1, n + 1)
    results, lock = [], threading.Lock()
    util = {0: [], 1: []}
    stop = threading.Event()
    sampler = threading.Thread(target=sample_utilisation, args=(cl, stop, util), daemon=True)
    sampler.start()

    def send(k, row, t_sched):
        delay = t_sched - time.time()
        if delay > 0:
            time.sleep(delay)
        if k == fail_lo:
            cl.failed.add(1)             # core "crashes" for the middle third
        if k == fail_hi:
            cl.failed.discard(1)
        proc = make_process(row)
        t0 = time.time()
        try:
            node, resp = dispatch(model, cl, proc, row)
            rtt = time.time() - t0
            ok = resp.status_code == 200
            rec = {"pid": proc["pid"], "cls": row["slice_type"], "node": node, "ok": ok, "rtt_s": rtt,
                   "state": "accepted" if ok else "rejected"}
        except requests.exceptions.RequestException:
            rec = {"pid": proc["pid"], "cls": row["slice_type"], "node": None, "ok": False,
                   "rtt_s": time.time() - t0, "state": "unreachable"}
        with lock:
            results.append(rec)

    start = time.time()
    with ThreadPoolExecutor(max_workers=32) as ex:
        for k, row in enumerate(workload):
            ex.submit(send, k, row, start + k / rate)
    cl.failed.clear()
    t_submit_end = time.time()

    accepted = sum(1 for r in results if r["ok"])
    deadline = time.time() + drain_timeout
    done = 0
    while time.time() < deadline:                     # drain: wait for accepted work to finish
        done = 0
        for i in (0, 1):
            try:
                done += cl.get(i, "/api/status").json()["completed"]
            except requests.exceptions.RequestException:
                pass
        if done >= accepted:
            break
        time.sleep(0.2)
    t_end = time.time()
    stop.set()
    sampler.join(timeout=1)

    node_lat = {}
    for i in (0, 1):
        try:
            for c in cl.get(i, "/api/completions").json()["completions"]:
                node_lat[c["pid"]] = c["latency_s"]
        except requests.exceptions.RequestException:
            pass
    e2e = [r["rtt_s"] + node_lat[r["pid"]] for r in results if r["ok"] and r["pid"] in node_lat]
    e2e_cls = {c: [r["rtt_s"] + node_lat[r["pid"]] for r in results
                   if r["ok"] and r["cls"] == c and r["pid"] in node_lat] for c in PROFILE}

    def pct(xs, p):
        if not xs:
            return float("nan")
        xs = sorted(xs)
        return xs[min(len(xs) - 1, int(p / 100 * len(xs) + 0.999999) - 1)]

    wall = t_end - start
    out = {
        "model": model, "requests": n, "accepted": accepted,
        "rejected": sum(1 for r in results if r["state"] == "rejected"),
        "unreachable": sum(1 for r in results if r["state"] == "unreachable"),
        "completed": len(e2e),
        "drained": done >= accepted,
        "accept_pct": 100.0 * accepted / n,
        "throughput_per_s": len(e2e) / wall,
        "mean_latency_s": statistics.mean(e2e) if e2e else float("nan"),
        "p95_latency_s": pct(e2e, 95),
        "p95_URLLC_s": pct(e2e_cls["URLLC"], 95),
        "p95_eMBB_s": pct(e2e_cls["eMBB"], 95),
        "p95_mMTC_s": pct(e2e_cls["mMTC"], 95),
        "mean_rtt_ms": 1000 * statistics.mean(r["rtt_s"] for r in results if r["ok"]) if accepted else float("nan"),
        "edge_cpu_util": statistics.mean(u[0] for u in util[0]) if util[0] else float("nan"),
        "core_cpu_util": statistics.mean(u[0] for u in util[1]) if util[1] else float("nan"),
        "edge_mem_util": statistics.mean(u[1] for u in util[0]) if util[0] else float("nan"),
        "core_mem_util": statistics.mean(u[1] for u in util[1]) if util[1] else float("nan"),
        "to_edge": sum(1 for r in results if r["node"] == 0 and r["ok"]),
        "to_core": sum(1 for r in results if r["node"] == 1 and r["ok"]),
        "wall_s": wall, "failure_injected": failure,
    }
    return out


# ---------------------------------------------------------------- local dry-run helper
def start_local_nodes(tick):
    wn = os.path.join(HERE, "..", "worker_vms", "worker_node.py")
    procs = []
    for node_id, port, cpu, mem, peer, prom in [("edge", 5001, 4, 1024, 5002, 8101), ("core", 5002, 8, 2048, 5001, 8102)]:
        env = dict(os.environ, PROM_PORT=str(prom), SCHED_TICK=str(tick))
        procs.append(subprocess.Popen([sys.executable, wn, node_id, str(port), str(cpu), str(mem), "127.0.0.1", str(peer)],
                                      env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    for url in ("http://127.0.0.1:5001", "http://127.0.0.1:5002"):
        for _ in range(50):
            try:
                requests.get(f"{url}/api/health", timeout=0.5)
                break
            except requests.exceptions.RequestException:
                time.sleep(0.2)
    return procs


def mean_of(runs, key):
    vals = [r[key] for r in runs if r[key] == r[key]]
    return statistics.mean(vals) if vals else float("nan")


def main():
    ap = argparse.ArgumentParser(description="Milestone 8 live experiment")
    ap.add_argument("--edge-url", default=f"http://{os.environ.get('NODE1_IP', '192.168.56.104')}:5001")
    ap.add_argument("--core-url", default=f"http://{os.environ.get('NODE2_IP', '192.168.56.107')}:5002")
    ap.add_argument("--slices", default=os.path.join(HERE, "slices.csv"))
    ap.add_argument("--out", default=os.path.join(HERE, "..", "results", "milestone8"))
    ap.add_argument("--requests", type=int, default=60)
    ap.add_argument("--rate", type=float, default=8.0, help="offered requests per second (open loop)")
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--failure", action="store_true", help="also run each model with the Core node crashed for the middle third")
    ap.add_argument("--drain-timeout", type=float, default=60.0)
    ap.add_argument("--local", action="store_true", help="start two local worker_node.py processes (dry run)")
    ap.add_argument("--tick", type=float, default=0.05, help="SCHED_TICK for --local nodes")
    ap.add_argument("--tag", default="", help="suffix for output files, e.g. vm or local")
    args = ap.parse_args()

    procs = []
    if args.local:
        procs = start_local_nodes(args.tick)
        args.edge_url, args.core_url = "http://127.0.0.1:5001", "http://127.0.0.1:5002"
    try:
        cl = Cluster(args.edge_url, args.core_url)
        for i in (0, 1):
            if not cl.alive(i):
                sys.exit(f"{cl.names[i]} node not reachable at {cl.urls[i]} - start worker_node.py first "
                         f"(use SCHED_TICK=0.05) or set NODE1_IP / NODE2_IP")
        tick = cl.get(0, "/api/status").json().get("sched_tick", 1.0)
        if tick > 0.2:
            print(f"WARNING: worker scheduler tick is {tick}s. Restart workers with SCHED_TICK=0.05 or the run will be very slow.")
        workload = load_workload(args.slices, args.requests)
        scenarios = [False, True] if args.failure else [False]
        all_runs = []
        for failure in scenarios:
            for model in MODELS:
                for rep in range(args.repeat):
                    r = run_once(model, cl, workload, args.rate, failure, args.drain_timeout)
                    r["repeat"] = rep + 1
                    all_runs.append(r)
                    print(f"[{'FAIL' if failure else 'NORM'}] {model:<15} rep{rep + 1}: accept {r['accept_pct']:5.1f}%  "
                          f"unreach {r['unreachable']:3d}  p95 {r['p95_latency_s']:.2f}s  thr {r['throughput_per_s']:.1f}/s  "
                          f"cpu e/c {r['edge_cpu_util']:.2f}/{r['core_cpu_util']:.2f}")
    finally:
        for p in procs:
            p.terminate()

    os.makedirs(args.out, exist_ok=True)
    suffix = f"_{args.tag}" if args.tag else ""
    keys = ["accept_pct", "throughput_per_s", "mean_latency_s", "p95_latency_s", "p95_URLLC_s", "p95_eMBB_s",
            "p95_mMTC_s", "mean_rtt_ms", "edge_cpu_util", "core_cpu_util", "edge_mem_util", "core_mem_util",
            "rejected", "unreachable", "to_edge", "to_core"]
    summary = []
    for failure in scenarios:
        for model in MODELS:
            runs = [r for r in all_runs if r["model"] == model and r["failure_injected"] == failure]
            summary.append(dict({"scenario": "core-crash" if failure else "normal", "model": model, "repeats": len(runs)},
                                **{k: round(mean_of(runs, k), 4) for k in keys}))
    with open(os.path.join(args.out, f"live_results{suffix}.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(summary[0].keys()))
        w.writeheader()
        w.writerows(summary)
    with open(os.path.join(args.out, f"live_results{suffix}.json"), "w") as f:
        json.dump({"args": vars(args), "runs": all_runs, "summary": summary}, f, indent=2)

    print("\n=== LIVE RESULTS (mean over repeats) ===")
    hdr = f"{'scenario':<11}{'model':<16}{'accept%':>8}{'thr/s':>7}{'p95 s':>7}{'URLLC p95':>10}{'edge cpu':>9}{'core cpu':>9}{'unreach':>8}"
    print(hdr)
    for s in summary:
        print(f"{s['scenario']:<11}{s['model']:<16}{s['accept_pct']:>8.1f}{s['throughput_per_s']:>7.2f}"
              f"{s['p95_latency_s']:>7.2f}{s['p95_URLLC_s']:>10.2f}{s['edge_cpu_util']:>9.2f}{s['core_cpu_util']:>9.2f}"
              f"{s['unreachable']:>8.1f}")
    print(f"\nWritten to {os.path.abspath(args.out)}")


if __name__ == "__main__":
    main()
