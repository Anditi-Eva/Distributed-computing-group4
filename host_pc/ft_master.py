"""Milestone 7 - Fault-tolerance master (runs on the Host PC).

Responsibilities
----------------
1. Failure detection   heartbeat GET /health on every worker each ``interval`` seconds;
                       a node is declared DEAD after ``threshold`` consecutive failed probes.
                       The failure is classified from what the probe saw:
                         UNREACHABLE     connection refused/reset  -> crash or node failure
                         SERVICE_FAILED  HTTP 503 from /health     -> service failure
                         TIMEOUT         no answer in ``timeout``  -> omission, delay or partition
2. Failover            the surviving node is told to adopt the replicated work of the dead node
                       (POST /api/failover/promote) and the router stops sending it traffic.
3. Recovery            service-level failures: POST /admin/restart_service.
                       Process/node failures: run the configured restarter (e.g. ssh + systemctl).
                       When the node answers health checks again it is resynchronised
                       (duplicates dropped, lost state restored) and then failed back.
4. Replica audit       detects a Byzantine backup that acknowledges replication without storing it.
5. Measurement         every incident is timestamped (first failed probe, detection, failover,
                       node recovered) for the availability and MTTR analysis.

Usage (real VMs):
    python ft_master.py --node node1=http://192.168.56.104:5001 --node node2=http://192.168.56.107:5002 \
        --restart-cmd "ssh student@{host} sudo systemctl restart worker-{name}"
"""
import argparse
import json
import subprocess
import threading
import time
from urllib.parse import urlparse

import requests
from prometheus_client import Counter, Gauge, Histogram, start_http_server

UP, SUSPECT, DEAD, RECOVERING, QUARANTINED = "UP", "SUSPECT", "DEAD", "RECOVERING", "QUARANTINED"
STATE_CODE = {UP: 0, SUSPECT: 1, DEAD: 2, RECOVERING: 3, QUARANTINED: 4}
UNREACHABLE, SERVICE_FAILED, TIMEOUT, BAD_RESPONSE, OK = "UNREACHABLE", "SERVICE_FAILED", "TIMEOUT", "BAD_RESPONSE", "OK"

NODE_STATE = Gauge("ft_node_state", "0=UP 1=SUSPECT 2=DEAD 3=RECOVERING 4=QUARANTINED", ["node"])
NODE_AVAIL = Gauge("ft_node_availability_ratio", "Observed Tuptime/Ttotal of the node since monitoring began", ["node"])
FAILURES = Counter("ft_failures_detected_total", "Failures declared by the detector", ["node", "cause"])
FAILOVERS = Counter("ft_failovers_total", "Failovers executed", ["node"])
RECOVERIES = Counter("ft_recoveries_total", "Nodes recovered and failed back", ["node"])
DETECTION = Histogram("ft_detection_seconds", "First failed probe to DEAD declaration", buckets=(0.5, 1, 2, 3, 4, 6, 10))
NODE_MTTR = Histogram("ft_node_mttr_seconds", "First failed probe to node recovered", buckets=(1, 2, 4, 8, 15, 30, 60))
ROUTED = Counter("ft_requests_total", "Client requests routed by the master", ["result"])
PROBE_LATENCY = Gauge("ft_probe_rtt_seconds", "Last heartbeat round-trip time", ["node"])
REPL_LAG = Gauge("ft_replication_lag_seconds", "Replication lag reported by the node", ["node"])


class Node:
    def __init__(self, name, url):
        self.name, self.url = name, url.rstrip("/")
        self.state = UP
        self.fails = 0
        self.oks = 0
        self.cause = None
        self.incarnation = None
        self.t_first_fail = None
        self.penalty_until = 0.0          # router hint: recently failed a real request
        self.incident = None
        self.audit_mismatch = 0
        self.repl_mode = None
        self.lag_alert_since = None
        self.suspicions_cleared = 0
        self.t_start = time.time()
        self.state_since = self.t_start
        self.time_up = 0.0
        self.probes = 0


class FaultToleranceMaster:
    def __init__(self, nodes, interval=1.0, threshold=3, timeout=0.8, recovery_ok=2,
                 restarter=None, audit_interval=2.0, lag_alert_s=1.5, request_timeout=1.0, verbose=False,
                 auto_recover=True):
        self.nodes = {name: Node(name, url) for name, url in nodes.items()}
        self.order = list(nodes)
        self.interval, self.threshold, self.timeout, self.recovery_ok = interval, threshold, timeout, recovery_ok
        self.restarter = restarter
        self.audit_interval, self.lag_alert_s = audit_interval, lag_alert_s
        self.request_timeout = request_timeout
        self.verbose = verbose
        self.auto_recover = auto_recover      # False = detect and fail over only (used to study the detector alone)
        self.lock = threading.RLock()
        self.events, self.incidents = [], []
        self.probe_count = 0
        self._stop = threading.Event()
        self._threads = []

    # ------------------------------------------------------------------ lifecycle
    def start(self):
        for node in self.nodes.values():
            self._threads.append(threading.Thread(target=self._heartbeat_loop, args=(node,), daemon=True))
        self._threads.append(threading.Thread(target=self._audit_loop, daemon=True))
        for t in self._threads:
            t.start()
        return self

    def stop(self):
        self._stop.set()
        for t in self._threads:
            t.join(timeout=3)

    def _event(self, kind, node=None, **detail):
        ev = {"t": time.time(), "event": kind, "node": node, **detail}
        with self.lock:
            self.events.append(ev)
        if self.verbose:
            print(f"[master {time.strftime('%H:%M:%S')}] {kind:<22} {node or '':<7} {detail or ''}", flush=True)
        return ev

    def _set_state(self, node, new):
        with self.lock:
            now = time.time()
            if node.state in (UP, SUSPECT):
                node.time_up += now - node.state_since
            node.state, node.state_since = new, now
            NODE_STATE.labels(node=node.name).set(STATE_CODE[new])

    def availability(self, node):
        now = time.time()
        up = node.time_up + ((now - node.state_since) if node.state in (UP, SUSPECT) else 0)
        return up / (now - node.t_start) if now > node.t_start else 1.0

    # ------------------------------------------------------------------ 1. failure detection
    def _probe(self, node):
        t0 = time.time()
        try:
            r = requests.get(f"{node.url}/health", timeout=self.timeout)
        except requests.exceptions.Timeout:
            return TIMEOUT, time.time() - t0, None
        except requests.exceptions.RequestException:
            return UNREACHABLE, time.time() - t0, None
        rtt = time.time() - t0
        if r.status_code == 503:
            return SERVICE_FAILED, rtt, None
        if r.status_code != 200:
            return BAD_RESPONSE, rtt, None
        try:
            return OK, rtt, r.json()
        except ValueError:
            return BAD_RESPONSE, rtt, None

    def _heartbeat_loop(self, node):
        while not self._stop.is_set():
            started = time.time()
            result, rtt, body = self._probe(node)
            with self.lock:
                self.probe_count += 1
                node.probes += 1
            PROBE_LATENCY.labels(node=node.name).set(rtt)
            self._on_probe(node, result, body)
            NODE_AVAIL.labels(node=node.name).set(self.availability(node))
            self._stop.wait(max(0.0, self.interval - (time.time() - started)))

    def _on_probe(self, node, result, body):
        with self.lock:
            if result == OK:
                self._on_ok(node, body)
                return
            node.fails += 1
            node.oks = 0
            node.cause = result
            if node.fails == 1:
                node.t_first_fail = time.time()
            if node.state == UP:
                self._set_state(node, SUSPECT)
                self._event("SUSPECT", node.name, cause=result)
            if node.state in (UP, SUSPECT) and node.fails >= self.threshold:
                self._declare_dead(node, result)
            elif node.state == RECOVERING:               # flapped while rejoining
                self._set_state(node, DEAD)
                self._event("REJOIN_FAILED", node.name, cause=result)

    def _on_ok(self, node, body):
        node.fails = 0
        if node.state == SUSPECT:
            node.suspicions_cleared += 1
            self._set_state(node, UP)
            self._event("SUSPICION_CLEARED", node.name)
        if node.state == UP:
            if body:
                if node.incarnation and body.get("incarnation") != node.incarnation:
                    self._event("RESTART_DETECTED", node.name, note="node restarted without being declared dead")
                node.incarnation = body.get("incarnation")
                node.repl_mode = body.get("replication", {}).get("mode")
                lag = body.get("replication", {}).get("lag_s", 0) or 0
                REPL_LAG.labels(node=node.name).set(lag)
                if lag >= self.lag_alert_s and node.lag_alert_since is None:
                    node.lag_alert_since = time.time()
                    self._event("REPLICATION_DEGRADED", node.name, lag_s=lag)
                elif lag < 0.5 and node.lag_alert_since is not None:
                    self._event("REPLICATION_RESTORED", node.name, degraded_for_s=round(time.time() - node.lag_alert_since, 3))
                    node.lag_alert_since = None
            node.t_first_fail = None
        elif node.state == DEAD:
            node.oks += 1
            if node.oks >= self.recovery_ok:
                self._set_state(node, RECOVERING)
                self._event("NODE_ANSWERING", node.name)
                if node.incident is not None:
                    node.incident["t_node_up"] = time.time()
                threading.Thread(target=self._failback, args=(node, body), daemon=True).start()

    def _declare_dead(self, node, cause):
        now = time.time()
        self._set_state(node, DEAD)
        node.oks = 0
        inc = {"id": len(self.incidents) + 1, "node": node.name, "cause": cause,
               "t_first_fail": node.t_first_fail, "t_detected": now, "t_failover_done": None,
               "failover": None, "recovery_actions": [], "t_node_up": None, "t_recovered": None,
               "incarnation_before": node.incarnation}
        self.incidents.append(inc)
        node.incident = inc
        FAILURES.labels(node=node.name, cause=cause).inc()
        DETECTION.observe(now - (node.t_first_fail or now))
        self._event("DEAD", node.name, cause=cause, detect_s=round(now - (node.t_first_fail or now), 3))
        threading.Thread(target=self._failover, args=(node, inc), daemon=True).start()
        if self.auto_recover:
            threading.Thread(target=self._recover, args=(node, inc), daemon=True).start()

    # ------------------------------------------------------------------ 2. failover
    def _failover(self, node, inc):
        backups = [n for n in self.nodes.values() if n is not node and n.state in (UP, SUSPECT)]
        if not backups:
            self._event("NO_BACKUP", node.name)
            return
        backup = backups[0]
        t0 = time.time()
        try:
            r = requests.post(f"{backup.url}/api/failover/promote", json={"origin": node.name}, timeout=3)
            result = r.json()
        except (requests.exceptions.RequestException, ValueError) as exc:
            self._event("FAILOVER_FAILED", node.name, backup=backup.name, error=str(exc))
            return
        inc["t_failover_done"] = time.time()
        inc["failover"] = {"backup": backup.name, "adopted": result.get("adopted"),
                           "promote_ms": result.get("took_ms"), "rpc_ms": round((time.time() - t0) * 1000, 1)}
        FAILOVERS.labels(node=node.name).inc()
        self._event("FAILOVER_DONE", node.name, **inc["failover"])

    # ------------------------------------------------------------------ 3. recovery
    def _recover(self, node, inc):
        """Pick the cheapest repair for the failure class and escalate if it does not work."""
        cause = inc["cause"]
        escalate_at = time.time() + 5.0
        attempts = 0
        while not self._stop.is_set() and node.state == DEAD and node.incident is inc and attempts < 4:
            attempts += 1
            soft = cause in (SERVICE_FAILED, TIMEOUT) and time.time() < escalate_at
            if soft:
                action = "restart_service"
                try:
                    requests.post(f"{node.url}/admin/restart_service", timeout=2)
                    ok = True
                except requests.exceptions.RequestException:
                    ok = False
            elif self.restarter:
                action = "restart_node"
                try:
                    self.restarter(node.name, node)
                    ok = True
                except Exception as exc:                  # noqa: BLE001 - recovery must never kill the master
                    ok = False
                    self._event("RESTART_ERROR", node.name, error=str(exc))
            else:
                self._event("NO_RESTARTER", node.name, note="manual repair required")
                return
            inc["recovery_actions"].append({"t": time.time(), "action": action, "ok": ok})
            self._event("RECOVERY_ACTION", node.name, action=action, ok=ok, attempt=attempts)
            # give the heartbeat time to confirm before trying again
            deadline = time.time() + (self.recovery_ok + 3) * self.interval + 2
            while time.time() < deadline and node.state == DEAD and not self._stop.is_set():
                time.sleep(0.1)

    def _failback(self, node, body):
        restarted = body and node.incident and body.get("incarnation") != node.incident.get("incarnation_before")
        try:
            r = requests.post(f"{node.url}/api/resync", json={"restore": bool(restarted)}, timeout=4)
            resync = r.json()
        except (requests.exceptions.RequestException, ValueError) as exc:
            resync = {"ok": False, "error": str(exc)}
        with self.lock:
            if node.state != RECOVERING:
                return
            node.t_first_fail = None
            node.incarnation = body.get("incarnation") if body else node.incarnation
            self._set_state(node, UP)
            inc = node.incident
            if inc:
                inc["t_recovered"], inc["resync"] = time.time(), resync
                NODE_MTTR.observe(inc["t_recovered"] - (inc["t_first_fail"] or inc["t_detected"]))
            RECOVERIES.labels(node=node.name).inc()
            self._event("FAILBACK", node.name, resync=resync)
            node.incident = None

    # ------------------------------------------------------------------ 4. Byzantine replica audit
    def _audit_loop(self):
        while not self._stop.wait(self.audit_interval):
            for node in list(self.nodes.values()):
                peers = [n for n in self.nodes.values() if n is not node]
                if node.state != UP or any(p.state != UP for p in peers) or node.repl_mode in (None, "none"):
                    continue
                try:
                    res = requests.post(f"{node.url}/api/audit", timeout=3).json()
                except (requests.exceptions.RequestException, ValueError):
                    continue
                if not res.get("verified"):
                    continue
                if res.get("match"):
                    node.audit_mismatch = 0
                    continue
                node.audit_mismatch += 1
                self._event("REPLICA_DIVERGED", node.name, owned=res.get("owned"), replica=res.get("replica"))
                if node.audit_mismatch >= 2:
                    self._repair_or_quarantine(node, peers[0])

    def _repair_or_quarantine(self, node, backup):
        """Re-seed the backup from the primary; if it still disagrees the backup is lying -> quarantine."""
        try:
            requests.post(f"{node.url}/api/reseed", timeout=3)
            again = requests.post(f"{node.url}/api/audit", timeout=3).json()
        except (requests.exceptions.RequestException, ValueError):
            return
        node.audit_mismatch = 0
        if again.get("verified") and not again.get("match"):
            self._set_state(backup, QUARANTINED)
            self._event("BYZANTINE_SUSPECT", backup.name, note="acknowledges replication but does not store it; quarantined")
            self.incidents.append({"id": len(self.incidents) + 1, "node": backup.name, "cause": "BYZANTINE",
                                   "t_first_fail": None, "t_detected": time.time(), "t_recovered": None,
                                   "recovery_actions": [{"action": "quarantine"}]})
        else:
            self._event("REPLICA_REPAIRED", node.name)

    def release_quarantine(self, name):
        node = self.nodes[name]
        if node.state == QUARANTINED:
            node.fails = node.oks = 0
            self._set_state(node, UP)
            self._event("QUARANTINE_RELEASED", name)

    # ------------------------------------------------------------------ routing (service continuity)
    def route_order(self, hints=True):
        """Healthy nodes first (configured order), then suspects; dead/quarantined nodes are skipped.
        hints=True also demotes nodes whose last real request failed (passive failure detection)."""
        now = time.time()
        usable = [self.nodes[n] for n in self.order if self.nodes[n].state in (UP, SUSPECT)]
        return sorted(usable, key=lambda n: (n.state != UP, hints and n.penalty_until > now))

    def call(self, method, path, retry=True, **kwargs):
        """Send a request to the best node. retry=True also tries the other nodes when the first attempt fails,
        which masks transient, omission and not-yet-detected failures. retry=False is detector-driven failover
        only: the client follows the node states and gets one attempt, so an outage lasts until the detector reacts."""
        attempts, last = 0, "no usable node"
        order = self.route_order(hints=retry)
        for node in (order if retry else order[:1]):
            attempts += 1
            t0 = time.time()
            try:
                r = requests.request(method, f"{node.url}{path}", timeout=self.request_timeout, **kwargs)
                if r.status_code == 200:
                    ROUTED.labels(result="ok").inc()
                    return {"ok": True, "node": node.name, "attempts": attempts,
                            "latency_ms": (time.time() - t0) * 1000, "body": r.json()}
                last = f"{node.name} HTTP {r.status_code}"
            except (requests.exceptions.RequestException, ValueError) as exc:
                last = f"{node.name} {type(exc).__name__}"
            if retry:
                node.penalty_until = time.time() + 2.0
        ROUTED.labels(result="failed").inc()
        return {"ok": False, "node": None, "attempts": attempts, "latency_ms": None, "error": last}

    def submit(self, process):
        return self.call("POST", "/api/submit", json=process)

    def read_status(self, retry=True):
        return self.call("GET", "/api/status", retry=retry)

    # ------------------------------------------------------------------ reporting
    def snapshot(self):
        with self.lock:
            return {n.name: {"state": n.state, "fails": n.fails, "cause": n.cause,
                             "availability": round(self.availability(n), 4)} for n in self.nodes.values()}

    def export(self, path):
        with self.lock:
            data = {"config": {"interval": self.interval, "threshold": self.threshold, "timeout": self.timeout,
                               "recovery_ok": self.recovery_ok},
                    "incidents": self.incidents, "events": self.events, "probes": self.probe_count}
        with open(path, "w") as fh:
            json.dump(data, fh, indent=2, default=str)


class AvailabilityProbe:
    """Samples a function every ``period`` seconds and computes Availability = Tuptime / Ttotal."""

    def __init__(self, fn, period=0.1, name="probe"):
        self.fn, self.period, self.name = fn, period, name
        self.samples = []                      # (t, ok)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=5)

    def _run(self):
        while not self._stop.is_set():
            t0 = time.time()
            try:
                ok = bool(self.fn())
            except Exception:                  # noqa: BLE001
                ok = False
            self.samples.append((t0, ok))
            self._stop.wait(max(0.0, self.period - (time.time() - t0)))

    def window(self, t0, t1):
        pts = [(t, ok) for t, ok in self.samples if t0 <= t <= t1]
        return pts

    def availability(self, t0, t1):
        """Each sample stands for the time until the next sample; uptime is the sum for successful samples."""
        pts = self.window(t0, t1)
        if len(pts) < 2:
            return None
        up = sum(b[0] - a[0] for a, b in zip(pts, pts[1:]) if a[1])
        total = pts[-1][0] - pts[0][0]
        return up / total if total > 0 else None

    def outages(self, t0, t1):
        """Contiguous failed stretches as (start, end, duration)."""
        out, start = [], None
        pts = self.window(t0, t1)
        for t, ok in pts:
            if not ok and start is None:
                start = t
            elif ok and start is not None:
                out.append((start, t, t - start))
                start = None
        if start is not None and pts:
            out.append((start, pts[-1][0], pts[-1][0] - start))
        return out


def shell_restarter(template):
    """Build a restarter from a shell template, e.g. 'ssh student@{host} sudo systemctl restart worker-{name}'."""
    def restart(name, node):
        u = urlparse(node.url)
        cmd = template.format(name=name, url=node.url, host=u.hostname, port=u.port)
        subprocess.run(cmd, shell=True, check=True, timeout=120)
    return restart


def main():
    try:
        import host_client
        defaults = [f"node1={host_client.NODE1_URL}", f"node2={host_client.NODE2_URL}"]
    except ImportError:
        defaults = ["node1=http://127.0.0.1:5001", "node2=http://127.0.0.1:5002"]
    p = argparse.ArgumentParser(description="Milestone 7 fault-tolerance master")
    p.add_argument("--node", action="append", help="name=url (repeat); default: the VMs in host_client.py")
    p.add_argument("--interval", type=float, default=1.0, help="heartbeat period, seconds")
    p.add_argument("--threshold", type=int, default=3, help="consecutive failed probes before DEAD")
    p.add_argument("--timeout", type=float, default=0.8, help="heartbeat timeout, seconds")
    p.add_argument("--restart-cmd", help="shell template used to restart a crashed node")
    p.add_argument("--metrics-port", type=int, default=9100, help="Prometheus exporter port (0 disables)")
    p.add_argument("--export", default="ft_incidents.json", help="where to write the incident log on exit")
    args = p.parse_args()
    nodes = dict(item.split("=", 1) for item in (args.node or defaults))
    if args.metrics_port:
        start_http_server(args.metrics_port)
    master = FaultToleranceMaster(nodes, interval=args.interval, threshold=args.threshold, timeout=args.timeout,
                                  restarter=shell_restarter(args.restart_cmd) if args.restart_cmd else None,
                                  verbose=True).start()
    print(f"[master] monitoring {nodes} every {args.interval}s, DEAD after {args.threshold} misses. Ctrl+C to stop.")
    try:
        while True:
            time.sleep(5)
            print(f"[master] {json.dumps(master.snapshot())}", flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        master.stop()
        master.export(args.export)
        print(f"[master] incident log written to {args.export}")


if __name__ == "__main__":
    main()
