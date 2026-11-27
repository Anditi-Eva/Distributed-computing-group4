"""Milestone 9/10: Baseline vs Proposed experiment.

Runs every system on the same machine, same workers, same workload, and writes raw
CSVs, summary.json and charts to --out. Each experiment follows
Hypothesis -> Design -> Implementation -> Measurement -> Analysis (see RESULTS.md).

    python host_pc/m9_m10_benchmark.py --out results/milestone9_10_proposed
    python host_pc/m9_m10_benchmark.py --quick           # smoke run, ~3 minutes

Systems
  direct    client -> worker /api/submit (no transparency; lower bound)
  B0        baseline transparency_gateway.py (reactive failover only)
  B1        B0 + Milestone 7 ft_master.py as deployed (heartbeat 1.0 s, 3 misses, 0.8 s timeout)
  B2        B0 + ft_master tuned like the proposal (0.25 s, 2 misses, 0.3 s) - isolates tuning
  P         proposed_gateway.py (heartbeat 0.25 s, 2 misses, 0.3 s probe timeout)

Units: latency in ms of one HTTP request as seen by the client; "messages" = HTTP requests
received by workers per client operation (heartbeat/meter traffic is tagged X-Probe and
excluded, and reported separately as background cost).
"""
import argparse
import csv
import json
import os
import platform
import random
import statistics
import sys
import threading
import time
import uuid

import requests

HOST_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HOST_DIR)
from local_cluster import BASELINE_PORT, PROPOSED_PORT, WORKERS, LocalCluster, url  # noqa: E402

ROOT = os.path.dirname(HOST_DIR)
GW = {"B0": url(BASELINE_PORT), "P": url(PROPOSED_PORT)}
DETECTORS = {
    "B0": None,
    "B1": {"interval": 1.0, "threshold": 3, "timeout": 0.8},
    "B2": {"interval": 0.25, "threshold": 2, "timeout": 0.3},
    "P": None,
}
SEED = 2403


# ------------------------------------------------------------------------ helpers
def stats(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return {"n": 0}

    def pct(p):
        return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))]
    return {"n": len(xs), "mean": round(statistics.fmean(xs), 3), "p50": round(pct(50), 3),
            "p95": round(pct(95), 3), "p99": round(pct(99), 3), "max": round(xs[-1], 3),
            "stdev": round(statistics.pstdev(xs), 3)}


def timed(session, method, target, **kw):
    t0 = time.perf_counter()
    try:
        r = session.request(method, target, timeout=kw.pop("timeout", 10), **kw)
    except requests.RequestException:
        return None, (time.perf_counter() - t0) * 1000
    return r, (time.perf_counter() - t0) * 1000


def worker_urls(names):
    return {n: url(WORKERS[n][0]) for n in names}


def meter(workers):
    """Total client/gateway requests received by all workers (excludes X-Probe traffic)."""
    total = 0
    for base in workers.values():
        r = requests.get(f"{base}/api/status", headers={"X-Probe": "meter"}, timeout=3).json()
        total += r["request_counts"]["api"]
    return total


def reset(workers):
    for base in workers.values():
        try:
            requests.post(f"{base}/api/reset", timeout=3)
        except requests.RequestException:
            pass


def proc(tag, i, burst=100000, cpu=0, mem=0, slice_type="eMBB"):
    return {"pid": f"{tag}-{i}-{uuid.uuid4().hex[:6]}", "name": f"{tag}{i}", "cpu_needed": cpu,
            "memory_needed": mem, "remaining_burst": burst, "slice_type": slice_type}


def write_csv(path, rows):
    if not rows:
        return
    keys = list(dict.fromkeys(k for row in rows for k in row))
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


# ------------------------------------------------------------- E1 admission overhead
def exp_admission(workers, n, tag="n2"):
    """H1: P admits a process with lower latency and fewer messages than B0."""
    sessions = {k: requests.Session() for k in ("direct", "B0", "P")}
    edge = workers["edge"]
    rows = []
    order = ["direct", "B0", "P"]
    for i in range(n):
        for system in order[i % 3:] + order[:i % 3]:            # rotate to cancel drift
            p = proc(f"adm{system}", i)
            if system == "direct":
                body = {k: v for k, v in p.items() if k != "slice_type"}
                r, ms = timed(sessions[system], "POST", f"{edge}/api/submit", json=body)
            else:
                r, ms = timed(sessions[system], "POST", f"{GW[system]}/api/processes", json=p)
            rows.append({"nodes": tag, "system": system, "i": i, "latency_ms": round(ms, 3),
                         "ok": bool(r is not None and r.status_code in (200, 201))})
    reset(workers)
    messages = {}
    for system in ("B0", "P"):
        before = meter(workers)
        for i in range(20):
            sessions[system].post(f"{GW[system]}/api/processes", json=proc(f"msg{system}", i), timeout=10)
        messages[system] = (meter(workers) - before) / 20
    reset(workers)
    summary = {s: stats([r["latency_ms"] for r in rows if r["system"] == s and r["ok"]]) for s in order}
    for s in order:
        summary[s]["success"] = sum(r["ok"] for r in rows if r["system"] == s)
        summary[s]["attempts"] = sum(1 for r in rows if r["system"] == s)
    summary["messages_per_create"] = messages
    return rows, summary


# --------------------------------------------------- E2 locate + terminate (live processes)
def exp_locate(workers, n_procs, n_lookups, tag="n2"):
    """H2: direct-owner routing answers a live-process lookup with 1 worker message instead of N."""
    sessions = {k: requests.Session() for k in ("B0", "P")}
    rids = {}
    for system in ("B0", "P"):
        rids[system] = []
        for i in range(n_procs):
            r = sessions[system].post(f"{GW[system]}/api/processes", json=proc(f"loc{system}", i), timeout=10)
            rids[system].append(r.json()["resource_id"])
    rng = random.Random(SEED)
    rows = []
    for i in range(n_lookups):
        for system in (("B0", "P") if i % 2 == 0 else ("P", "B0")):
            rid = rng.choice(rids[system])
            r, ms = timed(sessions[system], "GET", f"{GW[system]}/api/processes/process/{rid}")
            rows.append({"nodes": tag, "system": system, "op": "locate", "latency_ms": round(ms, 3),
                         "ok": bool(r is not None and r.ok and r.json().get("status") == "running"),
                         "routing": (r.headers.get("X-Routing") if r is not None else None) or "broadcast"})
    messages = {}
    for system in ("B0", "P"):
        before = meter(workers)
        sample = rids[system][:20]
        for rid in sample:
            sessions[system].get(f"{GW[system]}/api/processes/process/{rid}", timeout=10)
        locate_msgs = (meter(workers) - before) / len(sample)
        before = meter(workers)
        doomed = rids[system][-10:]
        for rid in doomed:
            r, ms = timed(sessions[system], "POST", f"{GW[system]}/api/processes/process/{rid}/terminate")
            rows.append({"nodes": tag, "system": system, "op": "terminate", "latency_ms": round(ms, 3),
                         "ok": bool(r is not None and r.ok), "routing": ""})
        messages[system] = {"locate": locate_msgs, "terminate": (meter(workers) - before) / len(doomed)}
    reset(workers)
    summary = {s: {op: stats([r["latency_ms"] for r in rows if r["system"] == s and r["op"] == op and r["ok"]])
                   for op in ("locate", "terminate")} for s in ("B0", "P")}
    for s in ("B0", "P"):
        summary[s]["locate_success"] = sum(r["ok"] for r in rows if r["system"] == s and r["op"] == "locate")
        summary[s]["locate_attempts"] = sum(1 for r in rows if r["system"] == s and r["op"] == "locate")
    summary["messages_per_op"] = messages
    return rows, summary


# --------------------------------------------------- E3 name resolution scalability
def exp_naming(sizes, n_lookups, tput_seconds):
    """H3: cached resolution stays flat as the registry grows; B0 pays a SQLite open+query per lookup."""
    run = uuid.uuid4().hex[:6]
    prefix = f"service/bench-{run}"
    sessions = {k: requests.Session() for k in ("B0", "P", "P-nocache")}
    rows, registered = [], 0
    rng = random.Random(SEED)
    summary = {}
    for size in sizes:
        new = list(range(registered, size))
        for start in range(0, len(new), 500):
            batch = [{"name": f"{prefix}/s{i}", "resource_id": f"svc-{run}-{i}", "kind": "service",
                      "logical_uri": f"/api/services/svc-{run}-{i}"} for i in new[start:start + 500]]
            for system in ("B0", "P"):
                t0 = time.perf_counter()
                r = sessions[system].post(f"{GW[system]}/api/names/register",
                                          json={"records": batch, "include_records": False}, timeout=60)
                assert r.status_code == 201, r.text
                rows.append({"size": size, "system": system, "op": "register_batch_500",
                             "latency_ms": round((time.perf_counter() - t0) * 1000, 3), "ok": True})
        registered = size
        for i in range(n_lookups):
            name = f"{prefix}/s{rng.randrange(size)}"
            for system in ("B0", "P", "P-nocache"):
                base = GW["P" if system.startswith("P") else "B0"]
                suffix = "?bypass_cache=1" if system == "P-nocache" else ""
                r, ms = timed(sessions[system], "GET", f"{base}/api/names/resolve/{name}{suffix}")
                rows.append({"size": size, "system": system, "op": "resolve", "latency_ms": round(ms, 3),
                             "ok": bool(r is not None and r.ok)})
        summary[size] = {s: stats([r["latency_ms"] for r in rows if r["size"] == size and r["system"] == s
                                   and r["op"] == "resolve" and r["ok"]]) for s in ("B0", "P", "P-nocache")}
    # Throughput at the largest size: 8 concurrent clients, closed loop.
    tput = {}
    for system in ("B0", "P"):
        done, stop = [0], threading.Event()

        def client():
            s = requests.Session()
            local_rng = random.Random()
            while not stop.is_set():
                try:
                    if s.get(f"{GW[system]}/api/names/resolve/{prefix}/s{local_rng.randrange(sizes[-1])}",
                             timeout=10).ok:
                        done[0] += 1
                except requests.RequestException:
                    pass
        threads = [threading.Thread(target=client) for _ in range(8)]
        t0 = time.perf_counter()
        for t in threads:
            t.start()
        time.sleep(tput_seconds)
        stop.set()
        for t in threads:
            t.join()
        tput[system] = round(done[0] / (time.perf_counter() - t0), 1)
    summary["throughput_resolves_per_s_8_clients"] = tput
    summary["cache"] = requests.get(f"{GW['P']}/api/metrics", timeout=5).json()["cache"]
    for system in ("B0", "P"):
        requests.delete(f"{GW[system]}/api/names", params={"prefix": prefix}, timeout=60)
    return rows, summary


# ---------------------------------------------------------------- E4 failover
def find_owner(workers, rid):
    for name, base in workers.items():
        try:
            r = requests.get(f"{base}/api/process/{rid}", headers={"X-Probe": "observer"}, timeout=1)
            if r.ok and r.json().get("status") == "running":
                return name
        except requests.RequestException:
            continue
    return None


def running_copies(workers, rid):
    count = 0
    for base in workers.values():
        try:
            r = requests.get(f"{base}/api/process/{rid}", headers={"X-Probe": "observer"}, timeout=1)
            count += bool(r.ok and r.json().get("status") == "running")
        except requests.RequestException:
            pass
    return count


def failover_trial(cluster, workers, config, fault, mode, window_s=5.0, fault_s=4.0):
    gateway = GW["P" if config == "P" else "B0"]
    created = requests.post(f"{gateway}/api/processes", json=proc("fo", 0, burst=100000, cpu=1, mem=64,
                                                                  slice_type="URLLC"), timeout=10).json()
    rid = created["resource_id"]
    owner = None
    for _ in range(20):
        time.sleep(0.1)
        owner = find_owner(workers, rid)
        if owner:
            break
    if owner is None:
        raise RuntimeError(f"{config}: created process {rid} is not running on any worker")
    survivors = {n: u for n, u in workers.items() if n != owner}
    t0 = time.time()
    if fault == "crash":
        try:
            requests.post(f"{workers[owner]}/chaos/inject", json={"type": "crash"}, timeout=2)
        except requests.RequestException:
            pass
    else:
        requests.post(f"{workers[owner]}/chaos/inject", json={"type": "omission", "probability": 1.0,
                                                               "hold_s": 2.0, "duration": fault_s}, timeout=2)
    client_rows = []
    resume_s = None
    if mode == "client":
        s = requests.Session()
        while time.time() - t0 < window_s:
            sent = time.time() - t0
            r, ms = timed(s, "GET", f"{gateway}/api/slices/{rid}", timeout=10)
            ok = bool(r is not None and r.ok and r.json().get("status") == "running")
            client_rows.append({"t_s": round(sent, 3), "latency_ms": round(ms, 3), "ok": ok})
        resume_s = next((row["t_s"] + row["latency_ms"] / 1000 for row in client_rows if row["ok"]), None)
    else:
        while time.time() - t0 < window_s:
            if find_owner(survivors, rid):
                resume_s = time.time() - t0
                break
            time.sleep(0.02)
    # recovery of the faulty node, then check for duplicate execution
    if fault == "crash":
        time.sleep(max(0.0, fault_s - (time.time() - t0)))
        cluster.start_worker(owner)
    time.sleep(max(0.0, fault_s + 3.5 - (time.time() - t0)))
    copies = running_copies(workers, rid)
    reset(workers)
    time.sleep(0.5)
    return {"config": config, "fault": fault, "mode": mode, "owner": owner,
            "resume_s": round(resume_s, 3) if resume_s is not None else None,
            "resumed_within_window": resume_s is not None, "running_copies_after_recovery": copies,
            "client_requests": len(client_rows),
            "client_ok": sum(r["ok"] for r in client_rows),
            "client_latency_ms": stats([r["latency_ms"] for r in client_rows])}, client_rows


def exp_failover(state_dir, trials, b0_silent_trials):
    rows, timelines = [], []
    for config in ("B0", "B1", "B2", "P"):
        gateways = ("proposed",) if config == "P" else ("baseline",)
        cluster = LocalCluster(os.path.join(state_dir, f"failover-{config}"))
        cluster.start(gateways=gateways, ft_master=DETECTORS[config])
        workers = worker_urls(cluster.worker_names)
        try:
            for fault in ("crash", "hang"):
                for mode in ("silent", "client"):
                    k = b0_silent_trials if (config == "B0" and mode == "silent") else trials
                    for trial in range(k):
                        result, timeline = failover_trial(cluster, workers, config, fault, mode)
                        result["trial"] = trial
                        rows.append(result)
                        for row in timeline:
                            timelines.append({"config": config, "fault": fault, "trial": trial, **row})
                        print(f"  failover {config} {fault:5s} {mode:6s} #{trial}: resume={result['resume_s']} "
                              f"copies={result['running_copies_after_recovery']} "
                              f"client_ok={result['client_ok']}/{result['client_requests']}", flush=True)
        finally:
            cluster.stop()
    summary = {}
    for config in ("B0", "B1", "B2", "P"):
        for fault in ("crash", "hang"):
            key = f"{config}/{fault}"
            silent = [r for r in rows if r["config"] == config and r["fault"] == fault and r["mode"] == "silent"]
            client = [r for r in rows if r["config"] == config and r["fault"] == fault and r["mode"] == "client"]
            lat = [t["latency_ms"] for t in timelines if t["config"] == config and t["fault"] == fault]
            summary[key] = {
                "silent_trials": len(silent),
                "silent_resumed": sum(r["resumed_within_window"] for r in silent),
                "silent_resume_s": stats([r["resume_s"] for r in silent]),
                "client_trials": len(client),
                "client_first_success_s": stats([r["resume_s"] for r in client]),
                "client_success_rate": round(sum(r["client_ok"] for r in client) /
                                             max(1, sum(r["client_requests"] for r in client)), 4),
                "client_requests_per_trial": round(statistics.fmean([r["client_requests"] for r in client]), 1)
                if client else 0,
                "client_latency_ms": stats(lat),
                "duplicate_copies_trials": sum(r["running_copies_after_recovery"] > 1 for r in silent + client),
            }
    return rows, timelines, summary


# ---------------------------------------------------------------- background cost
def exp_background(workers, seconds=10):
    """Idle heartbeat traffic each detector adds (probes per second received by workers)."""
    def probes():
        total = 0
        for base in workers.values():
            r = requests.get(f"{base}/api/status", headers={"X-Probe": "meter"}, timeout=3).json()
            total += r["request_counts"]["probe"] + r["request_counts"]["api"]
        return total
    before = probes()
    time.sleep(seconds)
    return round((probes() - before - len(workers)) / seconds, 2)


# ------------------------------------------------------------------------ charts
def charts(out, summary, timelines):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    blue, orange, grey = "#2a6fdb", "#e8833a", "#8a8f98"

    def save(fig, name):
        fig.tight_layout()
        fig.savefig(os.path.join(out, name), dpi=150)
        plt.close(fig)

    adm = summary["E1_admission"]["n2"]
    fig, ax = plt.subplots(figsize=(6.4, 3.8))
    labels = ["direct\n(no transparency)", "B0 baseline\ngateway", "P proposed\ngateway"]
    keys = ["direct", "B0", "P"]
    ax.bar(labels, [adm[k]["mean"] for k in keys], color=[grey, orange, blue])
    for i, k in enumerate(keys):
        ax.errorbar(i, adm[k]["mean"], yerr=[[0], [adm[k]["p95"] - adm[k]["mean"]]], color="black", capsize=4)
        ax.text(i, adm[k]["mean"] / 2, f"{adm[k]['mean']:.1f} ms", ha="center", color="white", fontsize=9)
    ax.set_ylabel("admission latency, ms (bar=mean, whisker=p95)")
    ax.set_title("E1  Process admission through the logical gateway (2 workers)")
    save(fig, "fig1_admission_latency.png")

    fig, ax = plt.subplots(figsize=(6.4, 3.8))
    ops = ["create", "locate", "terminate"]
    width = 0.2
    for j, (tag, nodes) in enumerate((("n2", 2), ("n4", 4))):
        if tag not in summary["E1_admission"]:
            continue
        m_adm = summary["E1_admission"][tag]["messages_per_create"]
        m_loc = summary["E2_locate"][tag]["messages_per_op"]
        for k, (system, color) in enumerate((("B0", orange), ("P", blue))):
            vals = [m_adm[system], m_loc[system]["locate"], m_loc[system]["terminate"]]
            xs = [x + (j * 2 + k) * width - 1.5 * width for x in range(3)]
            bars = ax.bar(xs, vals, width, color=color, alpha=1.0 if nodes == 4 else 0.55,
                          label=f"{'B0' if system == 'B0' else 'P'}  N={nodes} workers")
            for x, v in zip(xs, vals):
                ax.text(x, v + 0.1, f"{v:g}", ha="center", fontsize=7)
    ax.set_xticks(range(3))
    ax.set_xticklabels(ops)
    ax.set_ylabel("worker messages per operation")
    ax.set_title("E2/E5  Coordination messages: O(N) broadcast vs O(1) direct-owner")
    ax.legend(fontsize=8)
    save(fig, "fig2_messages_per_operation.png")

    nm = summary["E3_naming"]
    sizes = [s for s in nm if isinstance(s, int)]
    fig, ax = plt.subplots(figsize=(6.4, 3.8))
    for system, color, style in (("B0", orange, "-o"), ("P-nocache", grey, "--s"), ("P", blue, "-o")):
        ax.plot(sizes, [nm[s][system]["p50"] for s in sizes], style, color=color, label=f"{system} p50")
        ax.plot(sizes, [nm[s][system]["p95"] for s in sizes], style, color=color, alpha=0.35,
                label=f"{system} p95")
    ax.set_xscale("log")
    ax.set_xlabel("registered names (log scale)")
    ax.set_ylabel("resolve latency, ms")
    ax.set_title("E3  Name resolution vs registry size")
    ax.legend(fontsize=7, ncol=3)
    save(fig, "fig3_resolution_scalability.png")

    fo = summary["E4_failover"]
    fig, axes = plt.subplots(1, 2, figsize=(9, 3.8), sharey=True)
    for ax, fault in zip(axes, ("crash", "hang")):
        configs = ["B0", "B1", "B2", "P"]
        vals = [fo[f"{c}/{fault}"]["silent_resume_s"].get("p50") for c in configs]
        resumed = [fo[f"{c}/{fault}"]["silent_resumed"] for c in configs]
        trials = [fo[f"{c}/{fault}"]["silent_trials"] for c in configs]
        colors = [orange, orange, orange, blue]
        for i, (v, r, t) in enumerate(zip(vals, resumed, trials)):
            if v is None:
                ax.bar(i, 5.0, color="white", edgecolor=orange, hatch="//")
                ax.text(i, 2.5, f"never\n0/{t}", ha="center", fontsize=8)
            else:
                ax.bar(i, v, color=colors[i])
                ax.text(i, v + 0.08, f"{v:.2f} s\n{r}/{t}", ha="center", fontsize=8)
        ax.set_xticks(range(4))
        ax.set_xticklabels(["B0\nreactive", "B1\n+M7 1s×3", "B2\n+M7 0.25s×2", "P\nproposed"], fontsize=8)
        ax.set_title(f"{fault}: work resumed on survivor, no client traffic")
    axes[0].set_ylabel("median time from fault to resume, s")
    save(fig, "fig4_failover_resume.png")

    fig, axes = plt.subplots(1, 2, figsize=(9, 3.8), sharey=True)
    for ax, fault in zip(axes, ("crash", "hang")):
        for config, color in (("B0", "#c0392b"), ("B1", orange), ("B2", "#d4ac0d"), ("P", blue)):
            pts = [t for t in timelines if t["config"] == config and t["fault"] == fault and t["trial"] == 0]
            ax.plot([p["t_s"] for p in pts], [p["latency_ms"] for p in pts], ".-", color=color,
                    label=config, markersize=4, linewidth=0.8)
        ax.set_yscale("log")
        ax.set_xlabel("time since fault, s")
        ax.set_title(f"{fault}: client GET latency (trial 0)")
    axes[0].set_ylabel("request latency, ms (log)")
    axes[0].legend(fontsize=8)
    save(fig, "fig5_client_latency_during_failure.png")


# --------------------------------------------------------------------------- main
def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", default=os.path.join(ROOT, "results", "milestone9_10_proposed"))
    parser.add_argument("--state-dir", default=os.path.join(ROOT, ".bench_state"))
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--skip-failover", action="store_true")
    args = parser.parse_args()
    q = args.quick
    cfg = {"admission_n": 15 if q else 60, "locate_procs": 10 if q else 30, "lookups": 30 if q else 150,
           "naming_sizes": [100, 1000] if q else [100, 1000, 10000], "naming_lookups": 50 if q else 300,
           "tput_seconds": 2 if q else 5, "failover_trials": 2 if q else 8, "b0_silent_trials": 1 if q else 3}
    os.makedirs(args.out, exist_ok=True)
    random.seed(SEED)
    summary = {"config": cfg, "seed": SEED, "started": time.strftime("%Y-%m-%d %H:%M:%S"),
               "environment": {"python": platform.python_version(), "os": platform.platform(),
                               "machine": platform.machine(), "processor": platform.processor(),
                               "cpu_count": os.cpu_count(), "transport": "HTTP/1.1 over 127.0.0.1",
                               "worker_capacity": {n: {"cpu": WORKERS[n][1], "memory_mb": WORKERS[n][2]}
                                                   for n in WORKERS}},
               "E1_admission": {}, "E2_locate": {}}
    all_rows = {"admission": [], "locate": [], "naming": []}

    for tag, names in (("n2", ("edge", "core")), ("n4", ("edge", "core", "cloud1", "cloud2"))):
        print(f"== E1/E2/E3 on {tag} ({', '.join(names)})", flush=True)
        with LocalCluster(os.path.join(args.state_dir, f"steady-{tag}"), workers=names) as cluster:
            workers = worker_urls(cluster.worker_names)
            if tag == "n2":
                summary["background_msgs_per_s"] = {"P_heartbeat_idle": exp_background(workers)}
            rows, s = exp_admission(workers, cfg["admission_n"] if tag == "n2" else cfg["admission_n"] // 2, tag)
            all_rows["admission"] += rows
            summary["E1_admission"][tag] = s
            print(json.dumps(s, indent=1), flush=True)
            rows, s = exp_locate(workers, cfg["locate_procs"], cfg["lookups"] if tag == "n2" else cfg["lookups"] // 2,
                                 tag)
            all_rows["locate"] += rows
            summary["E2_locate"][tag] = s
            print(json.dumps(s, indent=1), flush=True)
            if tag == "n2":
                rows, s = exp_naming(cfg["naming_sizes"], cfg["naming_lookups"], cfg["tput_seconds"])
                all_rows["naming"] += rows
                summary["E3_naming"] = s
                print(json.dumps(s, indent=1, default=str), flush=True)

    print("== background cost of the M7 detector (B1 defaults)", flush=True)
    cluster =LocalCluster(os.path.join(args.state_dir, "bg-b1")).start(gateways=("baseline",),
                                                                        ft_master=DETECTORS["B1"])
    try:
        summary["background_msgs_per_s"]["B1_ft_master_idle"] = exp_background(worker_urls(cluster.worker_names))
    finally:
        cluster.stop()

    timelines = []
    if not args.skip_failover:
        print("== E4 failover", flush=True)
        rows, timelines, s = exp_failover(args.state_dir, cfg["failover_trials"], cfg["b0_silent_trials"])
        write_csv(os.path.join(args.out, "failover_trials.csv"),
                  [{**{k: v for k, v in r.items() if k != "client_latency_ms"},
                    **{f"client_latency_{k}": v for k, v in r["client_latency_ms"].items()}} for r in rows])
        write_csv(os.path.join(args.out, "failover_client_timeline.csv"), timelines)
        summary["E4_failover"] = s

    write_csv(os.path.join(args.out, "admission_requests.csv"), all_rows["admission"])
    write_csv(os.path.join(args.out, "locate_terminate_requests.csv"), all_rows["locate"])
    write_csv(os.path.join(args.out, "naming_requests.csv"), all_rows["naming"])
    summary["finished"] = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(os.path.join(args.out, "summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2, default=str)
    if not args.skip_failover:
        charts(args.out, summary, timelines)
    print(f"wrote {args.out}", flush=True)


if __name__ == "__main__":
    main()
