"""Multi-site failure demo: edge on this PC, core and cloud workers on remote servers.

    python host_pc/geo_demo.py --config ../geo_nodes.json                 # crash the owner
    python host_pc/geo_demo.py --config ../geo_nodes.json --fault partition  # cut its network link

Edge worker and the proposed gateway run here; each remote worker runs under
host_pc/site_worker.sh on its server and listens on 127.0.0.1 only. This script opens one
SSH tunnel per site, so the gateway's heartbeats and requests cross the real internet.
A "partition" is made by closing that site's tunnel: the remote worker keeps running but
the gateway can no longer reach it (the split-brain case that fencing must handle).

Config format: host_pc/geo_nodes.example.json (keep the real one out of the repository).
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import time

import requests

HOST_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HOST_DIR)
from local_cluster import PROPOSED_PORT, LocalCluster, url  # noqa: E402

GW = url(PROPOSED_PORT)
T0 = None


def say(stage, text):
    stamp = f"t=+{time.time() - T0:5.1f}s" if T0 else "         "
    print(f"{stamp}  [{stage:<11}] {text}", flush=True)


class Tunnel:
    def __init__(self, site):
        self.site, self.proc = site, None

    def remote(self, command):
        s = self.site
        return subprocess.run(["ssh", "-i", s["key"], "-o", "BatchMode=yes", s["ssh"],
                               f"cd ~/ics2403_demo && ./site_worker.sh {command}"],
                              capture_output=True, text=True, timeout=60).stdout.strip()

    def open(self, start_worker=True):
        s = self.site
        if start_worker:                      # idempotent: "already running" if it is up
            reply = self.remote(f"start {s['name']} {s['port']} 8 2048")
            print(f"           [SETUP      ] {s['label']}: {reply}", flush=True)
        self.proc = subprocess.Popen(
            ["ssh", "-i", s["key"], "-o", "BatchMode=yes", "-o", "ExitOnForwardFailure=yes",
             "-o", "ServerAliveInterval=5", "-o", "StrictHostKeyChecking=accept-new", "-N",
             "-L", f"{s['port']}:127.0.0.1:{s['port']}", s["ssh"]],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                if requests.get(f"{url(s['port'])}/api/health", timeout=5).ok:
                    return
            except requests.RequestException:
                time.sleep(0.5)
        raise SystemExit(f"tunnel to {s['label']} is up but its worker does not answer "
                         f"(start it: ssh ... './ics2403_demo/site_worker.sh start {s['name']} {s['port']}')")

    def close(self):
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=5)


def where(rid, nodes):
    out = {}
    for name, port in nodes.items():
        try:
            r = requests.get(f"{url(port)}/api/process/{rid}", headers={"X-Probe": "demo"}, timeout=6)
            out[name] = r.json().get("status") if r.ok else "-"
        except requests.RequestException:
            out[name] = "UNREACHABLE"
    return out


def membership():
    return requests.get(f"{GW}/api/membership", timeout=10).json()


def main():
    global T0
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", required=True)
    p.add_argument("--fault", choices=("crash", "partition"), default="crash")
    p.add_argument("--outage", type=float, default=15.0, help="seconds a partition lasts")
    p.add_argument("--keep-remote", action="store_true", help="leave remote workers running afterwards")
    p.add_argument("--state-dir", default=os.path.join(os.path.dirname(HOST_DIR), ".geo_state"))
    args = p.parse_args()
    cfg = json.load(open(args.config))
    local, sites = cfg["local"], cfg["sites"]
    labels = {local["name"]: local["label"], **{s["name"]: s["label"] for s in sites}}
    ports = {local["name"]: local["port"], **{s["name"]: s["port"] for s in sites}}

    print(f"\n=== Multi-site failure demo: {args.fault} ===\n")
    tunnels = {s["name"]: Tunnel(s) for s in sites}
    for t in tunnels.values():
        t.open()
    shutil.rmtree(args.state_dir, ignore_errors=True)
    cluster = LocalCluster(args.state_dir, workers=(local["name"],),
                           proposed_env={"HEARTBEAT_INTERVAL": "0.5"})
    try:
        for s in sites:                                   # clear any earlier demo state remotely
            requests.post(f"{url(s['port'])}/api/reset", timeout=10)
        cluster.start_worker(local["name"])
        cluster.start_gateways(gateways=("proposed",), static_nodes=list(ports))
        deadline = time.time() + 30
        while time.time() < deadline:
            nodes = membership()["nodes"]
            if all(n["alive"] and n["srtt_ms"] is not None for n in nodes.values()):
                break
            time.sleep(0.5)
        time.sleep(3)                                     # a few RTT samples per site
        say("MAP", "site                    worker   heartbeat RTT   adaptive timeout   detection bound")
        for name, n in membership()["nodes"].items():
            say("MAP", f"{labels[name]:<23} {name:<8} {n['srtt_ms']:>9.0f} ms {n['rto_s']:>14.2f} s"
                       f" {n['detection_bound_s']:>14.1f} s")

        t = time.time()
        created = requests.post(f"{GW}/api/processes", json={
            "name": "urllc-robot-arm", "logical_name": "slice-urllc-robot-arm", "slice_type": "URLLC",
            "cpu_needed": 1, "memory_needed": 128, "remaining_burst": 100000}, timeout=30).json()
        if "resource_id" not in created:
            raise SystemExit(f"create failed: {created}")
        rid = created["resource_id"]
        say("CREATE", f"client got {created['logical_uri']} in {(time.time() - t) * 1000:.0f} ms, "
                      f"replication={created['replication']}")
        placed = where(rid, ports)
        owner = next(n for n, s in placed.items() if s == "running")
        say("OPERATOR", "behind the gateway: " + ", ".join(f"{labels[n]}={s}" for n, s in placed.items()))
        t = time.time()
        requests.get(f"{GW}/api/slices/{rid}", timeout=30)
        say("CLIENT", f"GET same URI -> running in {(time.time() - t) * 1000:.0f} ms (served from {labels[owner]})")

        T0 = time.time()
        if args.fault == "crash":
            try:
                requests.post(f"{url(ports[owner])}/chaos/inject", json={"type": "crash"}, timeout=10)
            except requests.RequestException:
                pass
            say("FAILURE", f"worker in {labels[owner]} crashed (process exited; it reboots in 10 s)")
        else:
            tunnels[owner].close()
            say("FAILURE", f"network link {labels[local['name']]} <-> {labels[owner]} cut "
                           f"(the worker there is still running)")

        event = None
        while time.time() - T0 < 30 and event is None:
            for ev in membership()["events"]:
                if ev["event"] == "suspected_and_failed_over" and ev["node"] == owner:
                    event = ev
            time.sleep(0.25)
        if event is None:
            raise SystemExit("no failover within 30 s")
        detect_at = event["at"] - T0 - event["promote_ms"] / 1000
        say("DETECTION", f"heartbeat suspected {owner} ({labels[owner]}) at t=+{detect_at:.1f}s")
        say("RECOVERY", f"standby promoted in {event['promote_ms']:.0f} ms; name rebound, no client involved")
        say("OPERATOR", "behind the gateway: " + ", ".join(f"{labels[n]}={s}" for n, s in where(rid, ports).items()))
        for _ in range(2):
            t = time.time()
            status = requests.get(f"{GW}/api/slices/{rid}", timeout=30).json().get("status")
            say("RESTORED", f"GET same URI -> {status} in {(time.time() - t) * 1000:.0f} ms")

        if args.fault == "partition":
            time.sleep(max(0.0, args.outage - (time.time() - T0)))
            tunnels[owner].open(start_worker=False)
            say("REPAIR", f"link to {labels[owner]} restored")
        else:
            say("REPAIR", f"waiting for {labels[owner]} to reboot and rejoin")
        rejoined = None
        while time.time() - T0 < 60 and rejoined is None:
            for ev in membership()["events"]:
                if ev["event"] == "recovered" and ev["node"] == owner:
                    rejoined = ev
            time.sleep(0.5)
        if rejoined:
            say("REPAIR", f"{owner} rejoined: fenced {rejoined['fenced_duplicates']} stale copy(ies) by demotion, "
                          f"re-protected {rejoined['reprotected_replicas']}")
        final = where(rid, ports)
        say("OPERATOR", "final: " + ", ".join(f"{labels[n]}={s}" for n, s in final.items()))
        running = sum(s == "running" for s in final.values())
        say("CHECK", f"copies running: {running} ({'exactly one, correct' if running == 1 else 'NOT ONE'})")
    finally:
        for port in ports.values():
            try:
                requests.post(f"{url(port)}/api/reset", timeout=10)
            except requests.RequestException:
                pass
        cluster.stop()
        for t in tunnels.values():
            t.close()
            if not args.keep_remote:          # leave nothing running on the remote servers
                print(f"           [CLEANUP    ] {t.site['label']}: {t.remote('stop ' + t.site['name'])}")


if __name__ == "__main__":
    main()
