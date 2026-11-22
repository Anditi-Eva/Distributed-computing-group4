"""Oral defense Part E: Failure -> Detection -> Recovery -> Service Restoration, live.

    python host_pc/demo_failover.py                    # proposed gateway, crash fault
    python host_pc/demo_failover.py --fault hang       # hung worker (omission) instead of a crash
    python host_pc/demo_failover.py --system baseline  # same scenario on the baseline, for contrast
    python host_pc/demo_failover.py --scale            # also add a 3rd worker at runtime (M10)

Starts its own local cluster (edge + core workers + gateway), so it needs nothing else running.
"""
import argparse
import os
import shutil
import sys
import time

import requests

HOST_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HOST_DIR)
from local_cluster import BASELINE_PORT, PROPOSED_PORT, WORKERS, LocalCluster, url  # noqa: E402

T0 = None


def say(stage, text):
    stamp = f"t=+{time.time() - T0:5.2f}s" if T0 else "         "
    print(f"{stamp}  [{stage:<11}] {text}", flush=True)


def physical_location(rid, names):
    """Operator view behind the gateway: which worker really runs the process."""
    out = {}
    for n in names:
        try:
            r = requests.get(f"{url(WORKERS[n][0])}/api/process/{rid}", headers={"X-Probe": "demo"}, timeout=0.5)
            out[n] = r.json().get("status") if r.ok else "-"
        except requests.RequestException:
            out[n] = "DOWN"
    return out


def main():
    global T0
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--system", choices=("proposed", "baseline"), default="proposed")
    p.add_argument("--fault", choices=("crash", "hang"), default="crash")
    p.add_argument("--scale", action="store_true", help="register worker cloud1 at runtime first")
    p.add_argument("--state-dir", default=os.path.join(os.path.dirname(HOST_DIR), ".demo_state"))
    args = p.parse_args()
    gw = url(PROPOSED_PORT if args.system == "proposed" else BASELINE_PORT)
    names = ["edge", "core"]

    print(f"\n=== Milestone 9/10 failure demo: {args.system} gateway, {args.fault} fault ===\n")
    shutil.rmtree(args.state_dir, ignore_errors=True)        # fresh names and state every run
    cluster = LocalCluster(args.state_dir).start(gateways=(args.system,))
    try:
        if args.scale and args.system == "proposed":
            cluster.start_worker("cloud1")
            r = requests.post(f"{gw}/api/workers/register",
                              json={"name": "cloud1", "url": url(WORKERS["cloud1"][0]), "lease_s": 10}).json()
            names.append("cloud1")
            time.sleep(0.6)
            say("NAMING", f"registered {r['name']} at runtime (lease {r['lease_s']} s), no restart")
            say("NAMING", f"resolve worker/cloud1 -> {requests.get(f'{gw}/api/names/resolve/worker/cloud1').json()}")

        created = requests.post(f"{gw}/api/processes", json={
            "name": "urllc-robot-arm", "logical_name": "slice-urllc-robot-arm", "slice_type": "URLLC",
            "cpu_needed": 1, "memory_needed": 128, "remaining_burst": 100000}).json()
        if "resource_id" not in created:
            raise SystemExit(f"create failed: {created}")
        rid = created["resource_id"]
        say("CREATE", f"client got logical URI {created['logical_uri']}")
        say("CREATE", f"replication={created['replication']}  (client never sees a worker address)")
        time.sleep(0.4)
        where = physical_location(rid, names)
        owner = next(n for n, s in where.items() if s == "running")
        say("OPERATOR", f"behind the gateway: {where}  -> owner is '{owner}'")

        T0 = time.time()
        if args.fault == "crash":
            try:
                requests.post(f"{url(WORKERS[owner][0])}/chaos/inject", json={"type": "crash"}, timeout=1)
            except requests.RequestException:
                pass
            say("FAILURE", f"worker '{owner}' crashed (fail-stop, process exited)")
        else:
            requests.post(f"{url(WORKERS[owner][0])}/chaos/inject",
                          json={"type": "omission", "probability": 1.0, "hold_s": 2.0, "duration": 6})
            say("FAILURE", f"worker '{owner}' hung (drops every request for 6 s)")

        time.sleep(0.15)                       # the crash fires 50 ms after injection
        # Phase 1: NO client traffic. Does the system notice and recover on its own?
        say("OBSERVE", "no client requests for 3 s; watching the workers directly")
        reported = []

        def report_detection():
            if args.system != "proposed" or reported:
                return
            for ev in requests.get(f"{gw}/api/membership").json()["events"]:
                if ev["event"] == "suspected_and_failed_over" and ev["node"] == owner:
                    reported.append(ev)
                    say("DETECTION", f"heartbeat suspected '{owner}' at t=+{ev['at'] - T0 - ev['promote_ms'] / 1000:.2f}s "
                                     f"(bound: 0.25 s + 2 x 0.3 s = 0.85 s)")
                    say("RECOVERY", f"standby promoted in {ev['promote_ms']} ms, name rebound to new owner")
                    return

        while time.time() - T0 < 3.0:
            report_detection()
            where = physical_location(rid, names)
            if any(s == "running" for n, s in where.items() if n != owner):
                report_detection()
                say("OPERATOR", f"work running again on survivor: {where}")
                break
            time.sleep(0.05)
        else:
            say("OPERATOR", f"after 3 s: {physical_location(rid, names)}  -> nobody promoted the standby")

        # Phase 2: a client uses the same logical URI.
        for i in range(4):
            t = time.time()
            try:
                r = requests.get(f"{gw}/api/slices/{rid}", timeout=10)
                status = r.json().get("status")
            except requests.RequestException as exc:
                status = f"error {exc.__class__.__name__}"
            ms = (time.time() - t) * 1000
            label = "RESTORED" if status == "running" else "CLIENT"
            say(label, f"GET /api/slices/{rid[:8]}... -> {status} in {ms:.0f} ms (same logical URI)")
            time.sleep(0.2)
        if args.system == "baseline":
            say("NOTE", "baseline gateway broadcasts every lookup, so each request still waits for the dead worker")

        if args.fault == "crash":
            cluster.start_worker(owner)
            say("REPAIR", f"worker '{owner}' restarted (empty memory)")
        time.sleep(max(0, 7.5 - (time.time() - T0)))
        if args.system == "proposed":
            for ev in requests.get(f"{gw}/api/membership").json()["events"]:
                if ev["event"] == "recovered":
                    say("REPAIR", f"'{ev['node']}' rejoined: fenced {ev['fenced_duplicates']} stale copies (demoted), "
                                  f"re-protected {ev['reprotected_replicas']} processes with a new standby")
        say("OPERATOR", f"final placement: {physical_location(rid, names)}")
        running = sum(1 for s in physical_location(rid, names).values() if s == "running")
        say("CHECK", f"copies running: {running}  ({'exactly one, correct' if running == 1 else 'DUPLICATE'})")
    finally:
        cluster.stop()


if __name__ == "__main__":
    main()
