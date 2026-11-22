"""Milestone 9/10 measurements on the cloud deployment (run on the core server).

    python cloud_bench.py --dash-token <token> --out ~/ics2403_demo/cloud_results

Part 1, latency (baseline :5100 vs proposed :5101, same workers core + cloud1):
  admission (create) and locate latency, worker messages per operation.
Part 2, failure (proposed, driven through the live dashboard's own buttons):
  repeated crash of the core worker and network cut to the cloud worker, recording detection
  time on the gateway's clock, promotion time, fencing, and the client's request latency.
Writes CSV + summary.json. The client runs on the same server as the gateways, so latency is
gateway + worker + inter-region network, without the presenter's own internet link.
"""
import argparse
import csv
import json
import os
import platform
import random
import statistics
import time
import uuid

import requests

CORE, CLOUD = "http://127.0.0.1:5002", "http://127.0.0.1:5003"
GW = {"baseline": "http://127.0.0.1:5100", "proposed": "http://127.0.0.1:5101"}


def pct(xs, p):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))], 2) if xs else None


def stats(xs):
    return {"n": len(xs), "mean": round(statistics.fmean(xs), 2) if xs else None,
            "p50": pct(xs, 50), "p95": pct(xs, 95), "max": round(max(xs), 2) if xs else None}


def api_count():
    total = 0
    for base in (CORE, CLOUD):
        total += requests.get(f"{base}/api/status", headers={"X-Probe": "meter"}, timeout=10).json()["request_counts"]["api"]
    return total


def reset():
    for base in (CORE, CLOUD):
        requests.post(f"{base}/api/reset", timeout=10)


def proc(tag, i):
    return {"pid": f"{tag}-{i}-{uuid.uuid4().hex[:6]}", "name": f"{tag}{i}", "cpu_needed": 0,
            "memory_needed": 0, "remaining_burst": 1000000, "slice_type": "eMBB"}


def timed(s, method, url, **kw):
    t = time.perf_counter()
    r = s.request(method, url, timeout=30, **kw)
    return r, (time.perf_counter() - t) * 1000


def latency_part(n_create, n_locate, rows):
    sessions = {k: requests.Session() for k in GW}
    reset()
    rids = {k: [] for k in GW}
    for i in range(n_create):
        for system in (("baseline", "proposed") if i % 2 == 0 else ("proposed", "baseline")):
            r, ms = timed(sessions[system], "POST", f"{GW[system]}/api/processes", json=proc(system[0], i))
            ok = r.status_code == 201
            if ok:
                rids[system].append(r.json()["resource_id"])
            rows.append({"part": "latency", "system": system, "op": "create", "i": i, "ms": round(ms, 2), "ok": ok})
    time.sleep(1)
    rng = random.Random(2403)
    for i in range(n_locate):
        for system in (("baseline", "proposed") if i % 2 == 0 else ("proposed", "baseline")):
            rid = rng.choice(rids[system])
            r, ms = timed(sessions[system], "GET", f"{GW[system]}/api/slices/{rid}")
            where = ""
            if system == "proposed":
                where = r.headers.get("X-Routing", "")
            rows.append({"part": "latency", "system": system, "op": "locate", "i": i, "ms": round(ms, 2),
                         "ok": r.ok and r.json().get("status") == "running", "routing": where})
    messages = {}
    for system in GW:
        before = api_count()
        for i in range(10):
            sessions[system].post(f"{GW[system]}/api/processes", json=proc("m" + system[0], i), timeout=30)
        mid = api_count()
        for rid in rids[system][:10]:
            sessions[system].get(f"{GW[system]}/api/slices/{rid}", timeout=30)
        messages[system] = {"create": (mid - before) / 10, "locate": (api_count() - mid) / 10}  # meter reads are X-Probe, not counted
    reset()
    out = {}
    for system in GW:
        out[system] = {op: stats([r["ms"] for r in rows if r["part"] == "latency" and r["system"] == system
                                  and r["op"] == op and r["ok"]]) for op in ("create", "locate")}
    out["messages_per_op"] = messages
    return out


def failure_part(dash, token, trials, rows, timeline):
    s = requests.Session()
    q = {"token": token}

    def state():
        return s.get(f"{dash}/api/state", params=q, timeout=30).json()

    def wait_event(kind, node, after, limit):
        deadline = time.time() + limit
        while time.time() < deadline:
            for e in state()["events"]:
                if e["event"] == kind and e["node"] == node and e["at"] > after:
                    return e
            time.sleep(0.75)
        return None

    results = []
    for t in range(trials):
        s.post(f"{dash}/api/action/create", params=q, timeout=60)
        time.sleep(4)
        st = state()
        owner = next((n for n, v in st["placement"].items() if v == "running"), None)
        rec = {"trial": t, "first_owner": owner}
        # crash the owner (normally core in Dublin)
        t_crash = time.time()
        s.post(f"{dash}/api/action/crash", params=q, timeout=30)
        ev = wait_event("suspected_and_failed_over", owner, t_crash, 30)
        rec["crash_detect_s"] = round(ev["at"] - ev["promote_ms"] / 1000 - t_crash, 3) if ev else None
        rec["crash_promote_ms"] = round(ev["promote_ms"], 1) if ev else None
        rj = wait_event("recovered", owner, t_crash, 40)
        rec["crash_rejoin_s"] = round(rj["at"] - t_crash, 2) if rj else None
        time.sleep(2)
        owner2 = next((n for n, v in state()["placement"].items() if v == "running"), None)
        rec["second_owner"] = owner2
        t_cut = time.time()
        r = s.post(f"{dash}/api/action/cut", params=q, timeout=30)
        if r.ok:
            ev = wait_event("suspected_and_failed_over", owner2, t_cut, 30)
            rec["cut_detect_s"] = round(ev["at"] - ev["promote_ms"] / 1000 - t_cut, 3) if ev else None
            rec["cut_promote_ms"] = round(ev["promote_ms"], 1) if ev else None
            rj = wait_event("recovered", owner2, t_cut, 45)
            rec["cut_fenced"] = rj["fenced_duplicates"] if rj else None
            rec["cut_reprotected"] = rj["reprotected_replicas"] if rj else None
        time.sleep(3)
        final = state()
        rec["copies_running_end"] = sum(v == "running" for v in final["placement"].values())
        for smp in final["samples"]:
            phase = ("before" if smp["at"] < t_crash else "after_crash" if smp["at"] < t_cut else "after_cut")
            timeline.append({"trial": t, "t_rel_s": round(smp["at"] - t_crash, 2), "ms": smp["ms"],
                             "status": smp["status"], "phase": phase})
        results.append(rec)
        rows.append({"part": "failure", **rec})
        print("trial", rec, flush=True)
    def col(k):
        return [r[k] for r in results if r.get(k) is not None]
    by_phase = {ph: stats([x["ms"] for x in timeline if x["phase"] == ph]) for ph in ("before", "after_crash", "after_cut")}
    ok = sum(x["status"] == "running" for x in timeline)
    return {"trials": trials, "crash_detect_s": stats(col("crash_detect_s")), "crash_promote_ms": stats(col("crash_promote_ms")),
            "cut_detect_s": stats(col("cut_detect_s")), "cut_promote_ms": stats(col("cut_promote_ms")),
            "fenced_total": sum(col("cut_fenced")), "copies_running_end": col("copies_running_end"),
            "client_ms_by_phase": by_phase, "client_requests": len(timeline),
            "client_running_share": round(ok / max(1, len(timeline)), 4)}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dash", default="http://127.0.0.1:8080")
    p.add_argument("--dash-token", required=True)
    p.add_argument("--out", default=os.path.expanduser("~/ics2403_demo/cloud_results"))
    p.add_argument("--creates", type=int, default=40)
    p.add_argument("--locates", type=int, default=100)
    p.add_argument("--trials", type=int, default=5)
    a = p.parse_args()
    os.makedirs(a.out, exist_ok=True)
    rows, timeline = [], []
    rtt = {}
    for name, base in (("core", CORE), ("cloud1", CLOUD)):
        s = requests.Session(); s.get(f"{base}/api/status", timeout=10)
        xs = []
        for _ in range(30):
            t = time.perf_counter(); s.get(f"{base}/api/status", headers={"X-Probe": "rtt"}, timeout=10)
            xs.append((time.perf_counter() - t) * 1000)
        rtt[name] = stats(xs)
    summary = {"started": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()), "host": platform.platform(),
               "python": platform.python_version(), "worker_rtt_from_gateway_ms": rtt}
    print("rtt", rtt, flush=True)
    summary["latency"] = latency_part(a.creates, a.locates, rows)
    print("latency", json.dumps(summary["latency"]), flush=True)
    summary["failure"] = failure_part(a.dash, a.dash_token, a.trials, rows, timeline)
    summary["finished"] = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    json.dump(summary, open(os.path.join(a.out, "summary.json"), "w"), indent=2)
    for name, data in (("requests.csv", [r for r in rows if r["part"] == "latency"]),
                       ("failure_trials.csv", [r for r in rows if r["part"] == "failure"]),
                       ("client_timeline.csv", timeline)):
        keys = list(dict.fromkeys(k for r in data for k in r))
        with open(os.path.join(a.out, name), "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=keys); w.writeheader(); w.writerows(data)
    print("done", a.out, flush=True)


if __name__ == "__main__":
    main()
