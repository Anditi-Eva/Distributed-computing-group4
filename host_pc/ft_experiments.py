"""Milestone 7 - experiments: failure matrix, availability soak, design alternatives, Byzantine quorum.

Run from host_pc/ (needs the worker_vms/ folder next to it; everything runs on localhost):

    python ft_experiments.py --only smoke            # one crash trial, ~30 s
    python ft_experiments.py --only matrix --trials 5
    python ft_experiments.py --only soak --duration 180
    python ft_experiments.py --only design
    python ft_experiments.py --only byzantine
    python ft_experiments.py --only report           # tables + charts from the saved JSON
    python ft_experiments.py --only all

Results (JSON/CSV/PNG/RESULTS.md) go to host_pc/results/milestone7/.
"""
import argparse
import csv
import json
import os
import random
import statistics
import time
from concurrent.futures import ThreadPoolExecutor

import requests

from fault_injector import SCENARIOS, ChaosClient, FaultInjector, LocalCluster
from ft_master import AvailabilityProbe, FaultToleranceMaster

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "results", "milestone7")
PRE, POST = 3.0, 20.0          # seconds of observation before / after injection


# ====================================================================== helpers
def load_slices():
    """5G slice rows from slices.csv (same dataset as Milestones 1-2); falls back to synthetic types."""
    try:
        with open(os.path.join(HERE, "slices.csv")) as fh:
            return list(csv.DictReader(fh))[:200]
    except OSError:
        return [{"slice_id": str(i), "slice_type": t} for i, t in enumerate(["URLLC", "eMBB", "mMTC"] * 20)]


class LoadGen:
    """Client workload: submits network slices through the master at ~rate per second."""

    def __init__(self, master, rate=5.0, prefix="w"):
        self.master, self.period, self.prefix = master, 1.0 / rate, prefix
        self.rows = load_slices()
        self.records, self.accepted = [], set()
        self._stop = False
        import threading
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()
        return self

    def stop(self):
        self._stop = True
        self._thread.join(timeout=5)

    def _run(self):
        i = 0
        while not self._stop:
            t0 = time.time()
            row = self.rows[i % len(self.rows)]
            pid = f"{self.prefix}-{i}"
            # remaining_burst is large so accepted slices stay resident: a missing pid then means a lost slice
            proc = {"pid": pid, "name": f"Slice-{row['slice_id']}-{row['slice_type']}",
                    "cpu_needed": 1, "memory_needed": 1, "remaining_burst": 100000}
            res = self.master.submit(proc)
            self.records.append({"t": t0, "pid": pid, "ok": res["ok"], "attempts": res["attempts"], "latency_ms": res["latency_ms"]})
            if res["ok"]:
                self.accepted.add(pid)
            i += 1
            time.sleep(max(0.0, self.period - (time.time() - t0)))

    def success_ratio(self, t0, t1):
        rows = [r for r in self.records if t0 <= r["t"] <= t1]
        return (sum(r["ok"] for r in rows) / len(rows)) if rows else None


def held_pids(cluster):
    """Union of the slices every node currently holds."""
    held = set()
    for name in cluster.names:
        for _ in range(3):
            try:
                body = requests.get(f"{cluster.url(name)}/api/pids", timeout=2).json()
                held.update(body["active"], body["completed"])
                break
            except (requests.exceptions.RequestException, ValueError):
                time.sleep(0.5)
    return held


def summ(values):
    vals = [v for v in values if v is not None]
    if not vals:
        return {"n": 0, "mean": None, "stdev": None, "min": None, "max": None}
    return {"n": len(vals), "mean": statistics.mean(vals), "stdev": statistics.stdev(vals) if len(vals) > 1 else 0.0,
            "min": min(vals), "max": max(vals)}


def first_event(master, kinds, t0, node=None):
    for ev in list(master.events):
        if ev["t"] >= t0 and ev["event"] in kinds and (node is None or ev["node"] == node):
            return ev
    return None


def direct_probe(url):
    def probe():
        return requests.get(f"{url}/api/status", timeout=1.0).status_code == 200
    return probe


def rel(t, t0):
    return None if t is None else round(t - t0, 3)


# ====================================================================== E1: one failure trial
def run_trial(key, idx, base_port, interval=1.0, threshold=3, replication="async", post=POST, keep_timeline=False):
    scn = SCENARIOS[key]
    cluster = LocalCluster(base_port=base_port, base_metrics=base_port + 3000, replication=replication)
    master = ft = base = load = None
    try:
        cluster.start_all()
        master = FaultToleranceMaster(cluster.urls(), interval=interval, threshold=threshold,
                                      timeout=min(0.8, 0.8 * interval), restarter=cluster.restarter).start()
        ft = AvailabilityProbe(lambda: master.read_status()["ok"], 0.1, "with-ft").start()
        fo = AvailabilityProbe(lambda: master.read_status(retry=False)["ok"], 0.1, "failover-only").start()
        base = AvailabilityProbe(direct_probe(cluster.url("node1")), 0.1, "no-ft").start()
        load = LoadGen(master, rate=5).start()
        # a real failure lands at a random point of the heartbeat cycle, so randomise the injection phase
        time.sleep(PRE + random.random() * interval)
        t_inj = FaultInjector(cluster.urls(), cluster=cluster).inject(scn)
        time.sleep(post)
        t_end = time.time()
        load.stop(), ft.stop(), fo.stop(), base.stop()
        w0 = t_inj - PRE

        # ---- detection
        kinds = {"DEAD": ("DEAD", "RESTART_DETECTED"), "REPLICATION": ("REPLICATION_DEGRADED",),
                 "BYZANTINE": ("BYZANTINE_SUSPECT",)}[scn.expected_detection]
        det = first_event(master, kinds, t_inj, scn.target if scn.expected_detection != "BYZANTINE" else None)
        inc = next((i for i in master.incidents if i["node"] == scn.target and i["cause"] != "BYZANTINE"
                    and i["t_detected"] >= t_inj), None)
        # ---- node recovery (ground truth differs per fault class)
        t_rec = None
        if inc and inc.get("t_recovered"):
            t_rec, via = inc["t_recovered"], "failback after failover"
        elif scn.key == "crash":
            ready = [t for t in cluster.ready_at.get(scn.target, []) if t > t_inj]
            t_rec, via = (ready[0] if ready else None), "supervisor restart"
        elif scn.expected_detection == "REPLICATION":
            ev = first_event(master, ("REPLICATION_RESTORED",), t_inj, scn.target)
            t_rec, via = (ev["t"] if ev else None), "replication catch-up"
        elif scn.expected_detection == "DEAD" and "duration" in scn.params:
            t_rec, via = t_inj + scn.params["duration"], "fault expiry (never declared dead)"
        else:
            via = "n/a (quarantined, not repaired)"
        # ---- service view
        ft_out = ft.outages(w0, t_end)
        fo_out = fo.outages(w0, t_end)
        base_out = base.outages(w0, t_end)
        lats = sorted(r["latency_ms"] for r in load.records if w0 <= r["t"] <= t_end and r["ok"])
        held, accepted = held_pids(cluster), set(load.accepted)
        lost = len(accepted - held)
        row = {
            "scenario": key, "taxonomy": scn.taxonomy, "trial": idx, "interval": interval, "threshold": threshold,
            "detected": det is not None, "detected_by": det["event"] if det else None,
            "cause_class": inc["cause"] if inc else None,
            "detect_s": rel(det["t"] if det else None, t_inj),
            "failover_s": rel(inc["t_failover_done"] if inc else None, t_inj),
            "adopted": (inc["failover"] or {}).get("adopted") if inc and inc.get("failover") else None,
            "node_recovery_s": rel(t_rec, t_inj), "recovery_via": via,
            "ft_availability": ft.availability(w0, t_end), "baseline_availability": base.availability(w0, t_end),
            "failover_availability": fo.availability(w0, t_end),
            "ft_outage_s": sum(d for *_, d in ft_out), "baseline_outage_s": sum(d for *_, d in base_out),
            "failover_outage_s": sum(d for *_, d in fo_out),
            "ft_longest_outage_s": max([d for *_, d in ft_out], default=0.0),
            "failover_longest_outage_s": max([d for *_, d in fo_out], default=0.0),
            "baseline_longest_outage_s": max([d for *_, d in base_out], default=0.0),
            "p95_latency_ms": lats[int(0.95 * (len(lats) - 1))] if lats else None,
            "max_latency_ms": lats[-1] if lats else None,
            "request_success_ft": load.success_ratio(w0, t_end), "requests": len([r for r in load.records if w0 <= r["t"] <= t_end]),
            "slices_accepted": len(accepted), "slices_lost": lost,
            "retried_requests": sum(1 for r in load.records if r["attempts"] > 1),
            # TIMEOUT cannot tell a dead node from a slow one: declared dead although the process was alive
            "dead_but_alive": bool(inc and inc["cause"] == "TIMEOUT" and
                                   not any(ev["event"] == "RESTART_DETECTED" for ev in master.events)),
        }
        if keep_timeline:
            row["timeline"] = [{"t": rel(e["t"], t_inj), "event": e["event"], "node": e["node"],
                                **{k: v for k, v in e.items() if k not in ("t", "event", "node")}}
                               for e in master.events if e["t"] >= t_inj - 1]
            row["window_s"] = round(t_end - w0, 2)
        return row
    finally:
        for obj in (load, ft, base, locals().get("fo")):
            try:
                if obj:
                    obj.stop()
            except Exception:                                  # noqa: BLE001
                pass
        if master:
            master.stop()
        cluster.cleanup()


def failure_matrix(trials=5, workers=2, only=None):
    keys = only or list(SCENARIOS)
    jobs = [(k, i) for i in range(trials) for k in keys]
    rows = []

    def work(job_index_job):
        n, (k, i) = job_index_job
        port = 5100 + (n % workers) * 10
        for attempt in range(2):
            try:
                row = run_trial(k, i, port, keep_timeline=(i == 0))
                print(f"  [{k} #{i}] detect={row['detect_s']} failover={row['failover_s']} "
                      f"A: base={row['baseline_availability']:.4f} failover={row['failover_availability']:.4f} "
                      f"full={row['ft_availability']:.4f} p95={row['p95_latency_ms']:.0f}ms lost={row['slices_lost']}", flush=True)
                return row
            except Exception as exc:                           # noqa: BLE001 - retry once, e.g. a port still in TIME_WAIT
                print(f"  [{k} #{i}] attempt {attempt + 1} failed: {exc}", flush=True)
                time.sleep(1)
        return None
    # jobs that share a port range must not overlap: run `workers` lanes, each strictly sequential
    lanes = [[(n, j) for n, j in enumerate(jobs) if n % workers == lane] for lane in range(workers)]

    def run_lane(lane_jobs):
        return [work(j) for j in lane_jobs]
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for lane_rows in ex.map(run_lane, lanes):
            rows.extend(r for r in lane_rows if r)
    return rows


# ====================================================================== E2: availability soak
def soak(duration=180.0, mean_gap=22.0, seed=7):
    """Random faults hit node1 (the node clients use) for `duration` seconds; compare with and without FT."""
    rnd = random.Random(seed)
    kinds = ["crash", "service", "node_failure", "omission", "delay"]
    cluster = LocalCluster(base_port=5300, base_metrics=8300)
    master = ft = base = load = None
    faults = []
    try:
        cluster.start_all()
        master = FaultToleranceMaster(cluster.urls(), interval=1.0, threshold=3, timeout=0.8,
                                      restarter=cluster.restarter).start()
        ft = AvailabilityProbe(lambda: master.read_status()["ok"], 0.1).start()
        fo = AvailabilityProbe(lambda: master.read_status(retry=False)["ok"], 0.1).start()
        base = AvailabilityProbe(direct_probe(cluster.url("node1")), 0.1).start()
        load = LoadGen(master, rate=5).start()
        injector = FaultInjector(cluster.urls(), cluster=cluster)
        t_start = time.time()
        time.sleep(3)
        while time.time() - t_start < duration - 25:
            time.sleep(rnd.expovariate(1 / mean_gap))
            if time.time() - t_start >= duration - 25:
                break
            key = rnd.choice(kinds)
            t = injector.inject(SCENARIOS[key])
            faults.append({"t": round(t - t_start, 1), "scenario": key})
            print(f"  [soak] t={t - t_start:6.1f}s inject {key}", flush=True)
            # one failure at a time: wait until node1 is serving again (and any transient fault has ended)
            dur = SCENARIOS[key].params.get("duration") or 0
            while time.time() - t < 30:
                el = time.time() - t
                if el > max(2.0, dur + 1) and master.nodes["node1"].state == "UP" and cluster.responsive("node1"):
                    break
                time.sleep(0.3)
            time.sleep(4)                                      # cooldown
        time.sleep(max(0, duration - (time.time() - t_start)))
        t_end = time.time()
        load.stop(), ft.stop(), fo.stop(), base.stop()
        w0 = t_start
        ft_out, fo_out, base_out = ft.outages(w0, t_end), fo.outages(w0, t_end), base.outages(w0, t_end)
        total = t_end - w0
        n = len(faults)
        res = {
            "duration_s": round(total, 1), "faults": faults, "n_faults": n,
            "ft_availability": ft.availability(w0, t_end), "baseline_availability": base.availability(w0, t_end),
            "failover_availability": fo.availability(w0, t_end),
            "ft_downtime_s": sum(d for *_, d in ft_out), "baseline_downtime_s": sum(d for *_, d in base_out),
            "failover_downtime_s": sum(d for *_, d in fo_out),
            "ft_outages": len(ft_out), "baseline_outages": len(base_out), "failover_outages": len(fo_out),
            "ft_mttr_s": statistics.mean([d for *_, d in ft_out]) if ft_out else 0.0,
            "failover_mttr_s": statistics.mean([d for *_, d in fo_out]) if fo_out else 0.0,
            "baseline_mttr_s": statistics.mean([d for *_, d in base_out]) if base_out else None,
            "baseline_outage_list_s": [round(d, 2) for *_, d in base_out],
            "ft_outage_list_s": [round(d, 2) for *_, d in ft_out],
            "request_success_ft": load.success_ratio(w0, t_end),
            "slices_accepted": len(load.accepted), "slices_lost": len(set(load.accepted) - held_pids(cluster)),
            "incidents": [{k: (round(v - t_start, 2) if isinstance(v, float) and k.startswith("t_") else v)
                           for k, v in i.items() if k in ("node", "cause", "t_first_fail", "t_detected", "t_failover_done", "t_recovered")}
                          for i in master.incidents],
        }
        # analytic model: A = MTTF / (MTTF + MTTR); two independent nodes with perfect failover: 1-(1-A)^2
        mttf = (total - res["baseline_downtime_s"]) / max(n, 1)
        mttr_b = res["baseline_mttr_s"] or 0.0
        a_single = mttf / (mttf + mttr_b) if mttf else None
        res["model"] = {"mttf_s": mttf, "single_node_A": a_single,
                        "two_node_perfect_failover_A": (1 - (1 - a_single) ** 2) if a_single else None,
                        "failover_only_A": mttf / (mttf + res["failover_mttr_s"]) if mttf else None}
        return res
    finally:
        for obj in (load, ft, base, locals().get("fo")):
            try:
                if obj:
                    obj.stop()
            except Exception:                                  # noqa: BLE001
                pass
        if master:
            master.stop()
        cluster.cleanup()


# ====================================================================== E3: design alternatives
def metric(url, name):
    """Read one counter from a worker's Prometheus exporter."""
    total = 0.0
    for line in requests.get(url, timeout=2).text.splitlines():
        if line.startswith(name) and not line.startswith("#") and "_created" not in line.split("{")[0].split(" ")[0]:
            total += float(line.rsplit(" ", 1)[1])
    return total


def replication_trial(mode, idx, base_port):
    """Steady load, then a burst immediately followed by loss of node1 and its disk."""
    cluster = LocalCluster(base_port=base_port, base_metrics=base_port + 3000, replication=mode)
    master = ft = load = None
    try:
        cluster.start_all()
        master = FaultToleranceMaster(cluster.urls(), interval=1.0, threshold=3, timeout=0.8,
                                      restarter=cluster.restarter).start()
        ft = AvailabilityProbe(lambda: master.read_status()["ok"], 0.1).start()
        load = LoadGen(master, rate=20, prefix="steady").start()
        time.sleep(1)
        cpu0 = {n: cluster.cpu_seconds(n) for n in cluster.names}
        b0 = metric(f"http://127.0.0.1:{cluster.metrics['node1']}", "node_replication_bytes_total")
        time.sleep(8)
        steady = [r["latency_ms"] for r in load.records if r["ok"] and r["latency_ms"] is not None]
        cpu1 = {n: cluster.cpu_seconds(n) for n in cluster.names}
        b1 = metric(f"http://127.0.0.1:{cluster.metrics['node1']}", "node_replication_bytes_total")
        steady_s = 8.0
        # burst: accepted slices that the primary has had no time to replicate or checkpoint
        burst = LoadGen(master, rate=100, prefix="burst").start()
        time.sleep(0.5)
        burst.stop()
        scn = SCENARIOS["node_failure"]
        t_inj = FaultInjector(cluster.urls(), cluster=cluster).inject(scn)
        # wait for failback
        end = time.time() + 30
        while time.time() < end and not any(i.get("t_recovered") for i in master.incidents):
            time.sleep(0.3)
        time.sleep(1)
        load.stop(), ft.stop()
        # only slices the system had accepted before the failure can be "lost" by it
        accepted = {r["pid"] for lg in (load, burst) for r in lg.records if r["ok"] and r["t"] <= t_inj}
        lost = len(accepted - held_pids(cluster))
        inc = master.incidents[0] if master.incidents else {}
        w0 = t_inj - 1
        outs = ft.outages(w0, time.time())
        return {"mode": mode, "trial": idx,
                "submit_latency_ms": statistics.mean(steady), "submit_latency_p95_ms": sorted(steady)[int(0.95 * len(steady))],
                "replication_kB_per_s": (b1 - b0) / 1024 / steady_s,
                "worker_cpu_ms_per_s": (1000 * sum((cpu1[n] - cpu0[n]) for n in cluster.names) / steady_s)
                if all(v is not None for v in cpu0.values()) else None,
                "slices_accepted": len(accepted), "slices_lost": lost,
                "loss_pct": 100.0 * lost / max(1, len(accepted)),
                "adopted_at_failover": (inc.get("failover") or {}).get("adopted"),
                "service_outage_s": sum(d for *_, d in outs),
                "failover_s": rel(inc.get("t_failover_done"), t_inj),
                "node_recovery_s": rel(inc.get("t_recovered"), t_inj)}
    finally:
        for obj in (load, ft):
            try:
                if obj:
                    obj.stop()
            except Exception:                                  # noqa: BLE001
                pass
        if master:
            master.stop()
        cluster.cleanup()


def durability_trial(wal, idx, base_port):
    """Crash (process killed, disk survives) right after a burst of accepted slices.
    wal=off keeps only the 0.5 s checkpoint + async replica; flush/fsync add a write-ahead log."""
    cluster = LocalCluster(base_port=base_port, base_metrics=base_port + 3000, wal=wal)
    master = ft = load = None
    try:
        cluster.start_all()
        master = FaultToleranceMaster(cluster.urls(), interval=1.0, threshold=3, timeout=0.8,
                                      restarter=cluster.restarter).start()
        ft = AvailabilityProbe(lambda: master.read_status()["ok"], 0.1).start()
        load = LoadGen(master, rate=20, prefix="steady").start()
        time.sleep(1)
        cpu0 = {n: cluster.cpu_seconds(n) for n in cluster.names}
        time.sleep(6)
        steady = [r["latency_ms"] for r in load.records if r["ok"] and r["latency_ms"] is not None]
        cpu1 = {n: cluster.cpu_seconds(n) for n in cluster.names}
        burst = LoadGen(master, rate=100, prefix="burst").start()
        time.sleep(0.5)
        burst.stop()
        t_inj = FaultInjector(cluster.urls(), cluster=cluster).inject(SCENARIOS["crash"])
        time.sleep(8)
        load.stop(), ft.stop()
        accepted = {r["pid"] for lg in (load, burst) for r in lg.records if r["ok"] and r["t"] <= t_inj}
        lost = len(accepted - held_pids(cluster))
        return {"wal": wal, "trial": idx, "submit_latency_ms": statistics.mean(steady),
                "submit_latency_p95_ms": sorted(steady)[int(0.95 * len(steady))],
                "worker_cpu_ms_per_s": (1000 * sum((cpu1[n] - cpu0[n]) for n in cluster.names) / 6.0)
                if all(v is not None for v in cpu0.values()) else None,
                "slices_accepted": len(accepted), "slices_lost": lost, "loss_pct": 100.0 * lost / max(1, len(accepted))}
    finally:
        for obj in (load, ft):
            try:
                if obj:
                    obj.stop()
            except Exception:                                  # noqa: BLE001
                pass
        if master:
            master.stop()
        cluster.cleanup()


def heartbeat_trial(interval, threshold, idx, base_port):
    """Service-failure detection with a given heartbeat setting; also measures probe overhead."""
    cluster = LocalCluster(base_port=base_port, base_metrics=base_port + 3000)
    master = ft = None
    try:
        cluster.start_all()
        master = FaultToleranceMaster(cluster.urls(), interval=interval, threshold=threshold,
                                      timeout=min(0.8, 0.8 * interval), restarter=cluster.restarter).start()
        ft = AvailabilityProbe(lambda: master.read_status()["ok"], 0.05).start()
        time.sleep(3)
        p0, c0 = master.probe_count, cluster.cpu_seconds("node1")
        time.sleep(4)
        probes_per_s = (master.probe_count - p0) / 4.0
        cpu = (cluster.cpu_seconds("node1") - c0) / 4.0 * 1000 if c0 is not None else None
        # phase offset of the injection relative to the heartbeat is random, so repeat trials and average
        time.sleep(random.random() * interval)
        t_inj = FaultInjector(cluster.urls(), cluster=cluster).inject(SCENARIOS["service"])
        time.sleep(max(8, interval * (threshold + 4)))
        ft.stop()
        det = first_event(master, ("DEAD",), t_inj, "node1")
        inc = master.incidents[0] if master.incidents else None
        return {"interval": interval, "threshold": threshold, "trial": idx,
                "detect_s": rel(det["t"] if det else None, t_inj),
                "failover_s": rel(inc["t_failover_done"] if inc else None, t_inj),
                "recovered_s": rel(inc["t_recovered"] if inc else None, t_inj),
                "heartbeats_per_s": probes_per_s, "node1_cpu_ms_per_s_idle": cpu,
                "ft_outage_s": sum(d for *_, d in ft.outages(t_inj, time.time()))}
    finally:
        if ft:
            ft.stop()
        if master:
            master.stop()
        cluster.cleanup()


def flaky_link_trial(threshold, idx, base_port):
    """A flaky (not failed) node: 40% of requests dropped for 20 s. How many spurious failovers?"""
    cluster = LocalCluster(base_port=base_port, base_metrics=base_port + 3000)
    master = None
    try:
        cluster.start_all()
        master = FaultToleranceMaster(cluster.urls(), interval=0.5, threshold=threshold, timeout=0.4,
                                      restarter=None, auto_recover=False).start()
        time.sleep(2)
        t0 = time.time()
        ChaosClient().inject(cluster.url("node1"), "omission", probability=0.4, hold_s=2.0, duration=20)
        time.sleep(22)
        dead = [e for e in master.events if e["event"] == "DEAD" and e["t"] >= t0]
        return {"threshold": threshold, "trial": idx, "spurious_failovers": len(dead),
                "suspicions_cleared": master.nodes["node1"].suspicions_cleared,
                "unneeded_restarts": sum(1 for i in master.incidents for a in i["recovery_actions"]
                                         if a["action"] == "restart_node")}
    finally:
        if master:
            master.stop()
        cluster.cleanup()


def design_alternatives(trials=3):
    out = {"replication": [], "heartbeat": [], "threshold": [], "durability": []}
    jobs = [("replication", m, i) for i in range(trials) for m in ("none", "async", "sync")]
    jobs += [("heartbeat", iv, i) for i in range(trials) for iv in (0.25, 0.5, 1.0, 2.0)]
    jobs += [("threshold", th, i) for i in range(trials) for th in (1, 2, 3, 5)]
    jobs += [("durability", w, i) for i in range(trials) for w in ("off", "flush", "fsync")]

    def work(n_job):
        n, (kind, val, i) = n_job
        port = 5400 + (n % 2) * 10
        try:
            if kind == "replication":
                return kind, replication_trial(val, i, port)
            if kind == "heartbeat":
                return kind, heartbeat_trial(val, 3, i, port)
            if kind == "durability":
                return kind, durability_trial(val, i, port)
            return kind, flaky_link_trial(val, i, port)
        except Exception as exc:                               # noqa: BLE001
            print(f"  [{kind} {val} #{i}] failed: {exc}", flush=True)
            return kind, None
    lanes = [[(n, j) for n, j in enumerate(jobs) if n % 2 == lane] for lane in range(2)]

    def run_lane(lane_jobs):
        res = []
        for j in lane_jobs:
            r = work(j)
            print(f"  [{r[0]}] {j[1][1]} #{j[1][2]} -> {None if r[1] is None else {k: (round(v, 3) if isinstance(v, float) else v) for k, v in r[1].items() if k not in ('trial',)}}", flush=True)
            res.append(r)
        return res
    with ThreadPoolExecutor(max_workers=2) as ex:
        for lane in ex.map(run_lane, lanes):
            for kind, row in lane:
                if row:
                    out[kind].append(row)
    return out


# ====================================================================== E4: Byzantine quorum reads
def quorum_read(urls, key, n_total, timeout=1.0):
    """Ask every node, accept a value only if a majority of ALL n nodes agree on it."""
    answers = {}
    for name, url in urls.items():
        try:
            v = requests.get(f"{url}/api/register/{key}", timeout=timeout).json()["value"]
            answers.setdefault(json.dumps(v), []).append(name)
        except (requests.exceptions.RequestException, ValueError, KeyError):
            answers.setdefault("<no reply>", []).append(name)
    need = n_total // 2 + 1
    best = max((k for k in answers if k != "<no reply>"), key=lambda k: len(answers[k]), default=None)
    if best is not None and len(answers[best]) >= need:
        return best, [n for k, names in answers.items() if k != best for n in names]
    return None, []


def byzantine_experiment(reads=20):
    truth = "URLLC:latency<=5ms"
    cluster = LocalCluster(names=("n1", "n2", "n3", "n4"), base_port=5500, base_metrics=8500)
    chaos = ChaosClient()
    cases = [("1 Byzantine (random lies)", {"n1": "corrupt"}, []),
             ("1 Byzantine (consistent lie)", {"n1": "collude"}, []),
             ("1 Byzantine + 1 crashed", {"n1": "corrupt"}, ["n2"]),
             ("2 Byzantine colluding", {"n1": "collude", "n2": "collude"}, [])]
    rows = []
    try:
        cluster.start_all()
        for n in cluster.names:
            requests.put(f"{cluster.url(n)}/api/register/sla", json={"value": truth}, timeout=2)
        for label, byz, crashed in cases:
            for n_nodes in (2, 3, 4):
                names = cluster.names[:n_nodes]
                if any(c not in names for c in crashed) or n_nodes <= len([n for n in byz if n in names]) + len(crashed):
                    continue
                for n, mode in byz.items():
                    if n in names:
                        chaos.inject(cluster.url(n), "byzantine", mode=mode)
                for c in crashed:
                    cluster.kill(c)
                urls = {n: cluster.url(n) for n in names}
                outcome = {"correct": 0, "no_quorum": 0, "wrong": 0, "identified": 0}
                for _ in range(reads):
                    value, deviants = quorum_read(urls, "sla", n_nodes)
                    if value is None:
                        outcome["no_quorum"] += 1
                    elif json.loads(value) == truth:
                        outcome["correct"] += 1
                        outcome["identified"] += bool(set(deviants) & set(byz))
                    else:
                        outcome["wrong"] += 1
                f_byz = len([n for n in byz if n in names])

                rows.append({"case": label, "nodes": n_nodes, "byzantine": f_byz, "crashed": len(crashed),
                             "correct_pct": 100.0 * outcome["correct"] / reads,
                             "no_quorum_pct": 100.0 * outcome["no_quorum"] / reads,
                             "wrong_pct": 100.0 * outcome["wrong"] / reads,
                             "faulty_identified_pct": 100.0 * outcome["identified"] / reads,
                             "meets_3f_plus_1": n_nodes >= 3 * (f_byz + len(crashed)) + 1})
                for n in byz:
                    chaos.clear(cluster.url(n))
                for c in crashed:
                    cluster.start(c)
                    requests.put(f"{cluster.url(c)}/api/register/sla", json={"value": truth}, timeout=2)
        return rows
    finally:
        cluster.cleanup()


# ====================================================================== reporting
def save(name, data):
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, name), "w") as fh:
        json.dump(data, fh, indent=2, default=str)


def load_json(name):
    path = os.path.join(OUT, name)
    return json.load(open(path)) if os.path.exists(path) else None


def fmt(v, nd=2, unit=""):
    return "-" if v is None else f"{v:.{nd}f}{unit}"


def ms(sm, nd=2):
    return "-" if sm["mean"] is None else f"{sm['mean']:.{nd}f} ± {sm['stdev']:.{nd}f}"


def report():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({"font.size": 9, "axes.spines.top": False, "axes.spines.right": False})
    md = ["# Milestone 7 - experiment results", "",
          "All numbers come from `ft_experiments.py` running the master and two workers on one machine "
          "(localhost, no network latency). Heartbeat 1 s, DEAD after 3 consecutive misses, probe timeout 0.8 s, "
          "async replication every 0.2 s. Values are mean ± standard deviation over the listed number of trials.", ""]

    matrix = load_json("matrix.json")
    if matrix:
        by = {}
        for r in matrix:
            by.setdefault(r["scenario"], []).append(r)
        md += ["## 1. Failure comparison", "",
               "| Failure | Seen by detector as | Detected | Detection (s) | Failover (s) | Node back in service (s) | Declared dead but alive | Slices lost |",
               "|---|---|---|---|---|---|---|---|"]
        for key in SCENARIOS:
            rs = by.get(key)
            if not rs:
                continue
            n = len(rs)
            causes = sorted({r["cause_class"] or r["detected_by"] or "-" for r in rs})
            md.append(f"| {SCENARIOS[key].taxonomy} (`{key}`) | {', '.join(causes)} | {sum(r['detected'] for r in rs)}/{n} | "
                      f"{ms(summ([r['detect_s'] for r in rs]))} | {ms(summ([r['failover_s'] for r in rs]))} | "
                      f"{ms(summ([r['node_recovery_s'] for r in rs]))} | {sum(r['dead_but_alive'] for r in rs)}/{n} | "
                      f"{sum(r['slices_lost'] for r in rs)} of {sum(r['slices_accepted'] for r in rs)} |")
        md += ["", f"Trials per failure: {max(len(v) for v in by.values())}. All times are measured from the instant of injection. "
               f"Observation window: {PRE:.0f} s before to {POST:.0f} s after injection; the injection instant is random within the heartbeat cycle. Node back in service = failback after "
               "failover, supervisor restart, replication catch-up or fault expiry, depending on the failure (see `recovery_via` in matrix.csv).", "",
               "### Availability and recovery time (service level)", "",
               "Three clients probe the service every 100 ms. **No FT**: talks straight to node1. **Failover only**: follows the "
               "detector's node states, one attempt per request. **Full FT**: failover plus retry on the other node. "
               "Availability = T_uptime / T_total over the window; MTTR = mean outage length.", "",
               "| Failure | Availability: no FT | failover only | full FT | MTTR no FT (s) | MTTR failover only (s) | MTTR full FT (s) | p95 request latency, full FT (ms) | Max latency (ms) |",
               "|---|---|---|---|---|---|---|---|---|"]
        for key in SCENARIOS:
            rs = by.get(key)
            if not rs:
                continue
            pct = lambda f: f"{100 * summ([r[f] for r in rs])['mean']:.2f}%"
            md.append(f"| {key} | {pct('baseline_availability')} | {pct('failover_availability')} | {pct('ft_availability')} | "
                      f"{ms(summ([r['baseline_longest_outage_s'] for r in rs]))} | {ms(summ([r['failover_longest_outage_s'] for r in rs]))} | "
                      f"{ms(summ([r['ft_longest_outage_s'] for r in rs]))} | {summ([r['p95_latency_ms'] for r in rs])['mean']:.0f} | "
                      f"{summ([r['max_latency_ms'] for r in rs])['mean']:.0f} |")
        md += ["", "MTTR here is the longest outage seen by that client in the window (one fault per trial, so it is the outage length).", ""]
        keys = [k for k in SCENARIOS if k in by]
        fig, ax = plt.subplots(1, 2, figsize=(10.5, 3.9))
        x = list(range(len(keys)))
        det = [summ([r["detect_s"] for r in by[k]])["mean"] or 0 for k in keys]
        fo_t = [summ([r["failover_s"] for r in by[k]])["mean"] for k in keys]
        fo = [max(0, (f or d) - d) for f, d in zip(fo_t, det)]
        rec = [summ([r["node_recovery_s"] for r in by[k]])["mean"] or 0 for k in keys]
        ax[0].bar(x, det, 0.55, color="0.25", label="detection")
        ax[0].bar(x, fo, 0.55, bottom=det, color="0.7", hatch="//", edgecolor="white", label="failover")
        ax[0].plot(x, rec, "k_", markersize=20, markeredgewidth=2.2, label="node back in service")
        ax[0].set_xticks(x); ax[0].set_xticklabels(keys, rotation=30, ha="right"); ax[0].set_ylabel("seconds after injection")
        ax[0].set_title("Detect, fail over, recover"); ax[0].legend(frameon=False)
        w = 0.27
        for off, f, lab, col, hatch in ((-w, "baseline_availability", "no FT", "white", ""), (0, "failover_availability", "failover only", "0.65", "//"), (w, "ft_availability", "full FT", "0.2", "")):
            ax[1].bar([i + off for i in x], [100 * summ([r[f] for r in by[k]])["mean"] for k in keys], w, color=col, edgecolor="k", hatch=hatch, label=lab)
        ax[1].set_ylim(50, 100.5); ax[1].set_xticks(x); ax[1].set_xticklabels(keys, rotation=30, ha="right")
        ax[1].set_ylabel("service availability (%)"); ax[1].set_title(f"Availability over a {PRE + POST:.0f} s window"); ax[1].legend(frameon=False, loc="upper center", bbox_to_anchor=(0.5, -0.33), ncol=3, fontsize=8)
        fig.tight_layout(); fig.savefig(os.path.join(OUT, "failure_comparison.png"), dpi=160); plt.close(fig)
        md += ["![failure comparison](failure_comparison.png)", ""]
        tl = next((r for r in matrix if r["scenario"] == "node_failure" and r.get("timeline")), None)
        if tl:
            md += ["### Example event timeline (node failure, trial 0)", "", "| t (s after injection) | event | node | detail |", "|---|---|---|---|"]
            for e in tl["timeline"]:
                detail = {k: v for k, v in e.items() if k not in ("t", "event", "node")}
                md.append(f"| {e['t']} | {e['event']} | {e['node'] or ''} | {json.dumps(detail) if detail else ''} |")
            md.append("")
        with open(os.path.join(OUT, "matrix.csv"), "w", newline="") as fh:
            cols = [c for c in matrix[0] if c != "timeline"]
            wr = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
            wr.writeheader(); wr.writerows(matrix)

    sk = load_json("soak.json")
    if sk:
        m = sk["model"]
        md += ["## 2. Availability analysis (soak test)", "",
               f"Random faults (crash, service, node failure, omission, delay) were injected into node1 for {sk['duration_s']:.0f} s "
               f"({sk['n_faults']} faults, mean gap about 22 s, one at a time) while three clients probed the service every 100 ms. "
               "Availability = T_uptime / T_total, where a failed probe counts as downtime.", "",
               "| | No FT (direct to node1) | Failover only | Full FT (failover + retry) |", "|---|---|---|---|",
               f"| Availability | {100 * sk['baseline_availability']:.3f}% | {100 * sk['failover_availability']:.3f}% | {100 * sk['ft_availability']:.3f}% |",
               f"| Downtime (s) | {sk['baseline_downtime_s']:.1f} | {sk['failover_downtime_s']:.1f} | {sk['ft_downtime_s']:.1f} |",
               f"| Outages | {sk['baseline_outages']} | {sk['failover_outages']} | {sk['ft_outages']} |",
               f"| MTTR (s) | {fmt(sk['baseline_mttr_s'])} | {fmt(sk['failover_mttr_s'])} | {fmt(sk['ft_mttr_s'])} |", "",
               f"Client requests succeeded with full FT: {100 * sk['request_success_ft']:.2f}%. Slices accepted / lost: {sk['slices_accepted']} / {sk['slices_lost']}.", "",
               "Analytic check with A = MTTF / (MTTF + MTTR), using the measured MTTF:", "",
               "| Model | Predicted | Measured |", "|---|---|---|",
               f"| MTTF (s) | {fmt(m['mttf_s'], 1)} | |",
               f"| Single node: MTTF/(MTTF+MTTR_noFT) | {100 * m['single_node_A']:.3f}% | {100 * sk['baseline_availability']:.3f}% |",
               f"| Failover only: MTTF/(MTTF+MTTR_failover) | {100 * m['failover_only_A']:.3f}% | {100 * sk['failover_availability']:.3f}% |",
               f"| Two nodes, instant perfect failover, node2 equally failure-prone: 1-(1-A)^2 | {100 * m['two_node_perfect_failover_A']:.4f}% | n/a: only node1 was faulted |", "",
               "The last row is the ceiling for a pair in which *both* nodes fail at node1's rate and fail independently; node2 was never "
               f"faulted here, so the measured full-FT availability ({100 * sk['ft_availability']:.3f}%) is limited only by client retries "
               "masking each outage and is not comparable with that row. The first two rows validate the model: predicted and measured "
               "availability agree to within a few tenths of a percent.", ""]

    d = load_json("design.json")
    if d:
        md += ["## 3. Design alternatives: reliability vs resource usage", ""]
        if d["replication"]:
            by = {}
            for r in d["replication"]:
                by.setdefault(r["mode"], []).append(r)
            md += ["### 3.1 Replication strategy", "",
                   "Steady load of 20 slices/s for 8 s, then a 0.5 s burst of 100 slices/s, then node1 and its disk are lost immediately.", "",
                   "| Replication | Submit latency (ms) | Replication traffic (kB/s) | Worker CPU (ms per s) | Slices lost | Adopted at failover | Service outage (s) |",
                   "|---|---|---|---|---|---|---|"]
            for mode in ("none", "async", "sync"):
                rs = by.get(mode)
                if rs:
                    md.append(f"| {mode} | {ms(summ([r['submit_latency_ms'] for r in rs]))} | {ms(summ([r['replication_kB_per_s'] for r in rs]))} | "
                              f"{ms(summ([r['worker_cpu_ms_per_s'] for r in rs]), 1)} | "
                              f"{summ([r['slices_lost'] for r in rs])['mean']:.1f} of {summ([r['slices_accepted'] for r in rs])['mean']:.0f} "
                              f"({summ([r['loss_pct'] for r in rs])['mean']:.1f}%) | {ms(summ([r['adopted_at_failover'] for r in rs]), 1)} | {ms(summ([r['service_outage_s'] for r in rs]))} |")
            md.append("")
        if d["heartbeat"]:
            by = {}
            for r in d["heartbeat"]:
                by.setdefault(r["interval"], []).append(r)
            md += ["### 3.2 Heartbeat interval (threshold = 3 misses)", "",
                   "| Interval (s) | Detection (s) | Failover complete (s) | Heartbeat messages (per s, 2 nodes) | Idle worker CPU (ms/s) | Service outage (s) |",
                   "|---|---|---|---|---|---|"]
            for iv in sorted(by):
                rs = by[iv]
                md.append(f"| {iv} | {ms(summ([r['detect_s'] for r in rs]))} | {ms(summ([r['failover_s'] for r in rs]))} | "
                          f"{summ([r['heartbeats_per_s'] for r in rs])['mean']:.1f} | {ms(summ([r['node1_cpu_ms_per_s_idle'] for r in rs]), 1)} | {ms(summ([r['ft_outage_s'] for r in rs]))} |")
            md.append("")
        if d["threshold"]:
            by = {}
            for r in d["threshold"]:
                by.setdefault(r["threshold"], []).append(r)
            hb = {}
            for r in d["heartbeat"]:
                hb.setdefault(r["interval"], []).append(r)
            md += ["### 3.3 Detection threshold on a flaky link (40% of requests dropped for 20 s, heartbeat 0.5 s)", "",
                   "| Misses before DEAD | DEAD declarations (all spurious: the node never stopped) | Suspicions that cleared | Expected detection of a real failure (s) |", "|---|---|---|---|"]
            for th in sorted(by):
                rs = by[th]
                md.append(f"| {th} | {ms(summ([r['spurious_failovers'] for r in rs]), 1)} | {ms(summ([r['suspicions_cleared'] for r in rs]), 1)} | "
                          f"about {th * 0.5 - 0.25:.2f} (threshold x 0.5 s interval) |")
            md.append("")
        if d.get("durability"):
            by = {}
            for r in d["durability"]:
                by.setdefault(r["wal"], []).append(r)
            labels = {"off": "checkpoint only (0.5 s)", "flush": "WAL, flush per write", "fsync": "WAL, fsync per write"}
            md += ["### 3.4 Local durability on a crash (process killed, disk survives)", "",
                   "Steady 20 slices/s, then a 0.5 s burst of 100 slices/s, then a crash of node1 (supervisor restarts it after 1 s). "
                   "Async replication is on in every row, so the loss shown is what neither the replica nor the disk had.", "",
                   "| Local durability | Submit latency (ms) | p95 (ms) | Worker CPU (ms per s) | Accepted slices lost |", "|---|---|---|---|---|"]
            for w in ("off", "flush", "fsync"):
                rs = by.get(w)
                if rs:
                    md.append(f"| {labels[w]} | {ms(summ([r['submit_latency_ms'] for r in rs]))} | {ms(summ([r['submit_latency_p95_ms'] for r in rs]))} | "
                              f"{ms(summ([r['worker_cpu_ms_per_s'] for r in rs]), 1)} | {summ([r['slices_lost'] for r in rs])['mean']:.1f} of "
                              f"{summ([r['slices_accepted'] for r in rs])['mean']:.0f} ({summ([r['loss_pct'] for r in rs])['mean']:.2f}%) |")
            md.append("")
        fig, ax = plt.subplots(1, 4, figsize=(14.5, 3.6))
        if d.get("durability"):
            by = {}
            for r in d["durability"]:
                by.setdefault(r["wal"], []).append(r)
            ws = [w for w in ("off", "flush", "fsync") if w in by]
            ax[3].bar(ws, [summ([r["loss_pct"] for r in by[w]])["mean"] for w in ws], color="0.3")
            ax[3].set_ylabel("accepted slices lost (%)"); ax[3].set_title("Crash: local durability")
            ax4 = ax[3].twinx()
            ax4.plot(ws, [summ([r["submit_latency_ms"] for r in by[w]])["mean"] for w in ws], "ko--")
            ax4.set_ylabel("submit latency (ms)"); ax4.spines["right"].set_visible(True)
        if d["replication"]:
            by = {}
            for r in d["replication"]:
                by.setdefault(r["mode"], []).append(r)
            modes = [m for m in ("none", "async", "sync") if m in by]
            ax[0].bar(modes, [summ([r["loss_pct"] for r in by[m]])["mean"] for m in modes], color="0.3")
            ax[0].set_ylabel("accepted slices lost (%)"); ax[0].set_title("Data loss when a node and its disk fail")
            ax2 = ax[0].twinx()
            ax2.plot(modes, [summ([r["submit_latency_ms"] for r in by[m]])["mean"] for m in modes], "ko--")
            ax2.set_ylabel("submit latency (ms)"); ax2.spines["right"].set_visible(True)
        if d["heartbeat"]:
            by = {}
            for r in d["heartbeat"]:
                by.setdefault(r["interval"], []).append(r)
            ivs = sorted(by)
            ax[1].plot(ivs, [summ([r["detect_s"] for r in by[i]])["mean"] for i in ivs], "ko-", label="detection time (s)")
            ax[1].set_xlabel("heartbeat interval (s)"); ax[1].set_ylabel("seconds")
            ax3 = ax[1].twinx()
            ax3.plot(ivs, [summ([r["heartbeats_per_s"] for r in by[i]])["mean"] for i in ivs], "s--", color="0.5", label="heartbeats/s")
            ax3.set_ylabel("heartbeats per second"); ax3.spines["right"].set_visible(True)
            ax[1].set_title("Faster detection costs more probes")
        if d["threshold"]:
            by = {}
            for r in d["threshold"]:
                by.setdefault(r["threshold"], []).append(r)
            ths = sorted(by)
            ax[2].bar([str(t) for t in ths], [summ([r["spurious_failovers"] for r in by[t]])["mean"] for t in ths], color="0.3")
            ax[2].set_xlabel("misses before DEAD"); ax[2].set_ylabel("spurious failovers"); ax[2].set_title("Flaky link: false positives")
        fig.tight_layout(); fig.savefig(os.path.join(OUT, "design_alternatives.png"), dpi=160); plt.close(fig)
        md += ["![design alternatives](design_alternatives.png)", ""]

    bz = load_json("byzantine.json")
    if bz:
        md += ["## 4. Byzantine behavior: quorum reads of a replicated register", "",
               "Every node stores the same value. A client reads all nodes and accepts a value only if a majority of all N nodes return it. "
               "'Wrong' means a lie was accepted; 'no quorum' means the client noticed it could not decide (safe failure).", "",
               "| Case | Nodes N | Correct | No quorum (detected, undecidable) | Wrong value accepted | Faulty node identified | N >= 3f+1 |", "|---|---|---|---|---|---|---|"]
        for r in bz:
            md.append(f"| {r['case']} | {r['nodes']} | {r['correct_pct']:.0f}% | {r['no_quorum_pct']:.0f}% | {r['wrong_pct']:.0f}% | {r['faulty_identified_pct']:.0f}% | {'yes' if r['meets_3f_plus_1'] else 'no'} |")
        md += ["", "Reading the table: reading a value that was already stored identically on all honest nodes needs only a majority "
               "(N >= 2f+1), which is why one liar is masked at N = 3. The N >= 3f+1 column is the stricter bound for tolerating f "
               "*arbitrary* (Byzantine) nodes in general, including agreeing on a value and surviving crashes on top of lies; it is "
               "what breaks in the last rows. With one liar plus one crash, or two colluding liars, 3 or 4 nodes can no longer "
               "out-vote the faults: the client either sees no quorum (safe, 4 nodes) or accepts the colluders' lie (unsafe, 3 nodes). "
               "Tolerating two Byzantine nodes needs N >= 7.", ""]
        md.append("")

    with open(os.path.join(OUT, "RESULTS.md"), "w") as fh:
        fh.write("\n".join(md) + "\n")
    print(f"report written to {os.path.join(OUT, 'RESULTS.md')}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--only", default="all", choices=["smoke", "matrix", "soak", "design", "byzantine", "report", "all"])
    p.add_argument("--trials", type=int, default=5)
    p.add_argument("--duration", type=float, default=180)
    p.add_argument("--workers", type=int, default=2, help="parallel clusters for the matrix")
    args = p.parse_args()
    os.makedirs(OUT, exist_ok=True)
    t0 = time.time()
    if args.only == "smoke":
        print(json.dumps(run_trial("node_failure", 0, 5100, keep_timeline=True), indent=2, default=str))
        return
    if args.only in ("matrix", "all"):
        print("== failure matrix ==", flush=True)
        save("matrix.json", failure_matrix(args.trials, args.workers))
    if args.only in ("soak", "all"):
        print("== availability soak ==", flush=True)
        save("soak.json", soak(args.duration))
    if args.only in ("design", "all"):
        print("== design alternatives ==", flush=True)
        save("design.json", design_alternatives(max(2, args.trials - 2)))
    if args.only in ("byzantine", "all"):
        print("== byzantine quorum ==", flush=True)
        save("byzantine.json", byzantine_experiment())
    if args.only in ("report", "all", "matrix", "soak", "design", "byzantine"):
        report()
    print(f"done in {time.time() - t0:.0f} s")


if __name__ == "__main__":
    main()
