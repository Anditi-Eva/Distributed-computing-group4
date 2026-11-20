
"""Milestone 7 - Fault-tolerance master (runs on the Host PC).

Responsibilities
----------------
1. Failure detection:
   Heartbeat GET /health on every worker every interval seconds.
   A node is declared DEAD after threshold consecutive failed probes.

   Failure classifications:
       UNREACHABLE     Connection refused/reset
       SERVICE_FAILED  HTTP 503 from /health
       TIMEOUT         No answer within timeout
       BAD_RESPONSE    Unexpected HTTP response or invalid JSON

2. Failover:
   The surviving node is told to adopt the replicated work of the
   dead node.

3. Recovery:
   Service-level failures use /admin/restart_service.
   Process-level failures use the configured restarter, if provided.
   Recovered nodes are resynchronised and failed back.

4. Replica audit:
   Detects Byzantine replicas that acknowledge replication without
   actually storing the replicated data.

5. Measurement:
   Records failure, detection, failover and recovery timestamps for
   availability and MTTR analysis.
"""

import argparse
import json
import os
import subprocess
import threading
import time
from urllib.parse import urlparse

import requests
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

UP, SUSPECT, DEAD, RECOVERING, QUARANTINED = (
    "UP", "SUSPECT", "DEAD", "RECOVERING", "QUARANTINED"
)

STATE_CODE = {
    UP: 0,
    SUSPECT: 1,
    DEAD: 2,
    RECOVERING: 3,
    QUARANTINED: 4,
}

UNREACHABLE = "UNREACHABLE"
SERVICE_FAILED = "SERVICE_FAILED"
TIMEOUT = "TIMEOUT"
BAD_RESPONSE = "BAD_RESPONSE"
OK = "OK"


NODE_STATE = Gauge(
    "ft_node_state",
    "0=UP 1=SUSPECT 2=DEAD 3=RECOVERING 4=QUARANTINED",
    ["node"],
)

NODE_AVAIL = Gauge(
    "ft_node_availability_ratio",
    "Observed uptime divided by total monitoring time",
    ["node"],
)

FAILURES = Counter(
    "ft_failures_detected_total",
    "Failures declared by the detector",
    ["node", "cause"],
)

FAILOVERS = Counter(
    "ft_failovers_total",
    "Failovers executed",
    ["node"],
)

RECOVERIES = Counter(
    "ft_recoveries_total",
    "Nodes recovered and failed back",
    ["node"],
)

DETECTION = Histogram(
    "ft_detection_seconds",
    "First failed probe to DEAD declaration",
    buckets=(0.5, 1, 2, 3, 4, 6, 10),
)

NODE_MTTR = Histogram(
    "ft_node_mttr_seconds",
    "First failed probe to node recovered",
    buckets=(1, 2, 4, 8, 15, 30, 60),
)

ROUTED = Counter(
    "ft_requests_total",
    "Client requests routed by the master",
    ["result"],
)

PROBE_LATENCY = Gauge(
    "ft_probe_rtt_seconds",
    "Last heartbeat round-trip time",
    ["node"],
)

REPL_LAG = Gauge(
    "ft_replication_lag_seconds",
    "Replication lag reported by the node",
    ["node"],
)


class Node:

    def __init__(self, name, url):

        self.name = name
        self.url = url.rstrip("/")

        self.state = UP
        self.fails = 0
        self.oks = 0

        self.cause = None
        self.incarnation = None

        self.t_first_fail = None

        self.penalty_until = 0.0
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

    def __init__(
        self,
        nodes,
        interval=1.0,
        threshold=3,
        timeout=0.8,
        recovery_ok=2,
        restarter=None,
        audit_interval=2.0,
        lag_alert_s=1.5,
        request_timeout=1.0,
        verbose=False,
        auto_recover=True,
    ):

        self.nodes = {
            name: Node(name, url)
            for name, url in nodes.items()
        }

        self.order = list(nodes)

        self.interval = interval
        self.threshold = threshold
        self.timeout = timeout
        self.recovery_ok = recovery_ok

        self.restarter = restarter

        self.audit_interval = audit_interval
        self.lag_alert_s = lag_alert_s

        self.request_timeout = request_timeout
        self.verbose = verbose

        self.auto_recover = auto_recover

        self.lock = threading.RLock()

        self.events = []
        self.incidents = []

        self.probe_count = 0

        self._stop = threading.Event()
        self._threads = []

    # ---------------------------------------------------------
    # Lifecycle
    # ---------------------------------------------------------

    def start(self):

        for node in self.nodes.values():

            self._threads.append(
                threading.Thread(
                    target=self._heartbeat_loop,
                    args=(node,),
                    daemon=True,
                )
            )

        self._threads.append(
            threading.Thread(
                target=self._audit_loop,
                daemon=True,
            )
        )

        for thread in self._threads:
            thread.start()

        return self

    def stop(self):

        self._stop.set()

        for thread in self._threads:
            thread.join(timeout=3)

    def _event(self, kind, node=None, **detail):

        event = {
            "t": time.time(),
            "event": kind,
            "node": node,
            **detail,
        }

        with self.lock:
            self.events.append(event)

        if self.verbose:

            print(
                f"[master {time.strftime('%H:%M:%S')}] "
                f"{kind:<22} {node or '':<7} {detail or ''}",
                flush=True,
            )

        return event

    def _set_state(self, node, new):

        with self.lock:

            now = time.time()

            if node.state in (UP, SUSPECT):
                node.time_up += now - node.state_since

            node.state = new
            node.state_since = now

            NODE_STATE.labels(node=node.name).set(
                STATE_CODE[new]
            )

    def availability(self, node):

        now = time.time()

        up = node.time_up

        if node.state in (UP, SUSPECT):
            up += now - node.state_since

        total = now - node.t_start

        return up / total if total > 0 else 1.0

    # ---------------------------------------------------------
    # 1. Failure detection
    # ---------------------------------------------------------

    def _probe(self, node):

        start = time.time()

        try:

            response = requests.get(
                f"{node.url}/health",
                timeout=self.timeout,
            )

        except requests.exceptions.Timeout:

            return TIMEOUT, time.time() - start, None

        except requests.exceptions.RequestException:

            return UNREACHABLE, time.time() - start, None

        rtt = time.time() - start

        if response.status_code == 503:
            return SERVICE_FAILED, rtt, None

        if response.status_code != 200:
            return BAD_RESPONSE, rtt, None

        try:

            return OK, rtt, response.json()

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

            NODE_AVAIL.labels(node=node.name).set(
                self.availability(node)
            )

            self._stop.wait(
                max(
                    0.0,
                    self.interval - (time.time() - started),
                )
            )

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

                self._event(
                    "SUSPECT",
                    node.name,
                    cause=result,
                )

            if (
                node.state in (UP, SUSPECT)
                and node.fails >= self.threshold
            ):

                self._declare_dead(node, result)

            elif node.state == RECOVERING:

                self._set_state(node, DEAD)

                self._event(
                    "REJOIN_FAILED",
                    node.name,
                    cause=result,
                )

    def _on_ok(self, node, body):

        node.fails = 0

        if node.state == SUSPECT:

            node.suspicions_cleared += 1

            self._set_state(node, UP)

            node.cause = None
            node.t_first_fail = None

            self._event(
                "SUSPICION_CLEARED",
                node.name,
            )

        if node.state == UP:

            # Clear stale failure information after recovery.
            node.cause = None

            if body:

                if (
                    node.incarnation
                    and body.get("incarnation") != node.incarnation
                ):

                    self._event(
                        "RESTART_DETECTED",
                        node.name,
                        note="node restarted without being declared dead",
                    )

                node.incarnation = body.get("incarnation")

                node.repl_mode = body.get(
                    "replication", {}
                ).get("mode")

                lag = (
                    body.get("replication", {}).get("lag_s", 0)
                    or 0
                )

                REPL_LAG.labels(node=node.name).set(lag)

                if (
                    lag >= self.lag_alert_s
                    and node.lag_alert_since is None
                ):

                    node.lag_alert_since = time.time()

                    self._event(
                        "REPLICATION_DEGRADED",
                        node.name,
                        lag_s=lag,
                    )

                elif (
                    lag < 0.5
                    and node.lag_alert_since is not None
                ):

                    self._event(
                        "REPLICATION_RESTORED",
                        node.name,
                        degraded_for_s=round(
                            time.time() - node.lag_alert_since,
                            3,
                        ),
                    )

                    node.lag_alert_since = None

            node.t_first_fail = None

        elif node.state == DEAD:

            node.oks += 1

            if node.oks >= self.recovery_ok:

                self._set_state(node, RECOVERING)

                self._event(
                    "NODE_ANSWERING",
                    node.name,
                )

                if node.incident is not None:
                    node.incident["t_node_up"] = time.time()

                threading.Thread(
                    target=self._failback,
                    args=(node, body),
                    daemon=True,
                ).start()

    def _declare_dead(self, node, cause):

        now = time.time()

        self._set_state(node, DEAD)

        node.oks = 0

        incident = {
            "id": len(self.incidents) + 1,
            "node": node.name,
            "cause": cause,
            "t_first_fail": node.t_first_fail,
            "t_detected": now,
            "t_failover_done": None,
            "failover": None,
            "recovery_actions": [],
            "t_node_up": None,
            "t_recovered": None,
            "incarnation_before": node.incarnation,
        }

        self.incidents.append(incident)
        node.incident = incident

        FAILURES.labels(
            node=node.name,
            cause=cause,
        ).inc()

        DETECTION.observe(
            now - (node.t_first_fail or now)
        )

        self._event(
            "DEAD",
            node.name,
            cause=cause,
            detect_s=round(
                now - (node.t_first_fail or now),
                3,
            ),
        )

        threading.Thread(
            target=self._failover,
            args=(node, incident),
            daemon=True,
        ).start()

        if self.auto_recover:

            threading.Thread(
                target=self._recover,
                args=(node, incident),
                daemon=True,
            ).start()

    # ---------------------------------------------------------
    # 2. Failover
    # ---------------------------------------------------------

    def _failover(self, node, incident):

        backups = [
            n for n in self.nodes.values()
            if n is not node
            and n.state in (UP, SUSPECT)
        ]

        if not backups:

            self._event(
                "NO_BACKUP",
                node.name,
            )

            return

        backup = backups[0]

        start = time.time()

        try:

            response = requests.post(
                f"{backup.url}/api/failover/promote",
                json={"origin": node.name},
                timeout=3,
            )

            result = response.json()

        except (
            requests.exceptions.RequestException,
            ValueError,
        ) as exc:

            self._event(
                "FAILOVER_FAILED",
                node.name,
                backup=backup.name,
                error=str(exc),
            )

            return

        incident["t_failover_done"] = time.time()

        incident["failover"] = {
            "backup": backup.name,
            "adopted": result.get("adopted"),
            "promote_ms": result.get("took_ms"),
            "rpc_ms": round(
                (time.time() - start) * 1000,
                1,
            ),
        }

        FAILOVERS.labels(node=node.name).inc()

        self._event(
            "FAILOVER_DONE",
            node.name,
            **incident["failover"],
        )

    # ---------------------------------------------------------
    # 3. Recovery
    # ---------------------------------------------------------

    def _recover(self, node, incident):

        cause = incident["cause"]

        escalate_at = time.time() + 5.0

        attempts = 0

        while (
            not self._stop.is_set()
            and node.state == DEAD
            and node.incident is incident
            and attempts < 4
        ):

            attempts += 1

            soft = (
                cause in (SERVICE_FAILED, TIMEOUT)
                and time.time() < escalate_at
            )

            if soft:

                action = "restart_service"

                try:

                    requests.post(
                        f"{node.url}/admin/restart_service",
                        timeout=2,
                    )

                    ok = True

                except requests.exceptions.RequestException:

                    ok = False

            elif self.restarter:

                action = "restart_node"

                try:

                    self.restarter(node.name, node)
                    ok = True

                except Exception as exc:

                    ok = False

                    self._event(
                        "RESTART_ERROR",
                        node.name,
                        error=str(exc),
                    )

            else:

                self._event(
                    "NO_RESTARTER",
                    node.name,
                    note="manual repair or external supervisor required",
                )

                return

            incident["recovery_actions"].append(
                {
                    "t": time.time(),
                    "action": action,
                    "ok": ok,
                    "attempt": attempts,
                }
            )

            self._event(
                "RECOVERY_ACTION",
                node.name,
                action=action,
                ok=ok,
                attempt=attempts,
            )

            deadline = (
                time.time()
                + (self.recovery_ok + 3) * self.interval
                + 2
            )

            while (
                time.time() < deadline
                and node.state == DEAD
                and not self._stop.is_set()
            ):

                time.sleep(0.1)

    def _failback(self, node, body):

        restarted = (
            body
            and node.incident
            and body.get("incarnation")
            != node.incident.get("incarnation_before")
        )

        try:

            response = requests.post(
                f"{node.url}/api/resync",
                json={"restore": bool(restarted)},
                timeout=4,
            )

            resync = response.json()

        except (
            requests.exceptions.RequestException,
            ValueError,
        ) as exc:

            resync = {
                "ok": False,
                "error": str(exc),
            }

        with self.lock:

            if node.state != RECOVERING:
                return

            incident = node.incident

            # Record recovery time before clearing the failure timestamp.
            if incident:

                incident["t_recovered"] = time.time()
                incident["resync"] = resync

                recovery_start = (
                    incident.get("t_first_fail")
                    or incident["t_detected"]
                )

                NODE_MTTR.observe(
                    incident["t_recovered"] - recovery_start
                )

            node.cause = None
            node.fails = 0
            node.oks = 0
            node.t_first_fail = None

            self._set_state(node, UP)

            RECOVERIES.labels(
                node=node.name
            ).inc()

            self._event(
                "FAILBACK",
                node.name,
                resync=resync,
            )

            node.incident = None

    # ---------------------------------------------------------
    # 4. Byzantine replica audit
    # ---------------------------------------------------------

    def _audit_loop(self):

        while not self._stop.wait(self.audit_interval):

            for node in list(self.nodes.values()):

                peers = [
                    n for n in self.nodes.values()
                    if n is not node
                ]

                if (
                    node.state != UP
                    or any(p.state != UP for p in peers)
                    or node.repl_mode in (None, "none")
                ):

                    continue

                try:

                    result = requests.post(
                        f"{node.url}/api/audit",
                        timeout=3,
                    ).json()

                except (
                    requests.exceptions.RequestException,
                    ValueError,
                ):

                    continue

                if not result.get("verified"):
                    continue

                if result.get("match"):

                    node.audit_mismatch = 0
                    continue

                node.audit_mismatch += 1

                self._event(
                    "REPLICA_DIVERGED",
                    node.name,
                    owned=result.get("owned"),
                    replica=result.get("replica"),
                )

                if node.audit_mismatch >= 2:

                    self._repair_or_quarantine(
                        node,
                        peers[0],
                    )

    def _repair_or_quarantine(self, node, backup):

        try:

            requests.post(
                f"{node.url}/api/reseed",
                timeout=3,
            )

            again = requests.post(
                f"{node.url}/api/audit",
                timeout=3,
            ).json()

        except (
            requests.exceptions.RequestException,
            ValueError,
        ):

            return

        node.audit_mismatch = 0

        if again.get("verified") and not again.get("match"):

            self._set_state(backup, QUARANTINED)

            self._event(
                "BYZANTINE_SUSPECT",
                backup.name,
                note="replica mismatch persisted after reseeding",
            )

            self.incidents.append(
                {
                    "id": len(self.incidents) + 1,
                    "node": backup.name,
                    "cause": "BYZANTINE",
                    "t_first_fail": None,
                    "t_detected": time.time(),
                    "t_recovered": None,
                    "recovery_actions": [
                        {"action": "quarantine"}
                    ],
                }
            )

        else:

            self._event(
                "REPLICA_REPAIRED",
                node.name,
            )

    def release_quarantine(self, name):

        node = self.nodes[name]

        if node.state == QUARANTINED:

            node.fails = 0
            node.oks = 0
            node.cause = None

            self._set_state(node, UP)

            self._event(
                "QUARANTINE_RELEASED",
                name,
            )

    # ---------------------------------------------------------
    # Routing
    # ---------------------------------------------------------

    def route_order(self, hints=True):

        now = time.time()

        usable = [
            self.nodes[name]
            for name in self.order
            if self.nodes[name].state in (UP, SUSPECT)
        ]

        return sorted(
            usable,
            key=lambda node: (
                node.state != UP,
                hints and node.penalty_until > now,
            ),
        )

    def call(self, method, path, retry=True, **kwargs):

        attempts = 0
        last = "no usable node"

        order = self.route_order(hints=retry)

        selected = order if retry else order[:1]

        for node in selected:

            attempts += 1

            start = time.time()

            try:

                response = requests.request(
                    method,
                    f"{node.url}{path}",
                    timeout=self.request_timeout,
                    **kwargs,
                )

                if response.status_code == 200:

                    ROUTED.labels(result="ok").inc()

                    return {
                        "ok": True,
                        "node": node.name,
                        "attempts": attempts,
                        "latency_ms": (
                            time.time() - start
                        ) * 1000,
                        "body": response.json(),
                    }

                last = (
                    f"{node.name} HTTP {response.status_code}"
                )

            except (
                requests.exceptions.RequestException,
                ValueError,
            ) as exc:

                last = (
                    f"{node.name} {type(exc).__name__}"
                )

            if retry:

                node.penalty_until = time.time() + 2.0

        ROUTED.labels(result="failed").inc()

        return {
            "ok": False,
            "node": None,
            "attempts": attempts,
            "latency_ms": None,
            "error": last,
        }

    def submit(self, process):

        return self.call(
            "POST",
            "/api/submit",
            json=process,
        )

    def read_status(self, retry=True):

        return self.call(
            "GET",
            "/api/status",
            retry=retry,
        )

    # ---------------------------------------------------------
    # Reporting
    # ---------------------------------------------------------

    def snapshot(self):

        with self.lock:

            return {
                node.name: {
                    "state": node.state,
                    "fails": node.fails,
                    "cause": node.cause,
                    "availability": round(
                        self.availability(node),
                        4,
                    ),
                }
                for node in self.nodes.values()
            }

    def export(self, path):

        # Create the destination directory if necessary.
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)

        with self.lock:

            data = {
                "config": {
                    "interval": self.interval,
                    "threshold": self.threshold,
                    "timeout": self.timeout,
                    "recovery_ok": self.recovery_ok,
                },
                "incidents": self.incidents,
                "events": self.events,
                "probes": self.probe_count,
            }

        # Write to a temporary file before replacing the old file.
        temp_path = path + ".tmp"

        with open(temp_path, "w", encoding="utf-8") as file:

            json.dump(
                data,
                file,
                indent=2,
                default=str,
            )

        os.replace(temp_path, path)


class AvailabilityProbe:

    """Samples a function and computes availability."""

    def __init__(self, fn, period=0.1, name="probe"):

        self.fn = fn
        self.period = period
        self.name = name

        self.samples = []

        self._stop = threading.Event()

        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
        )

    def start(self):

        self._thread.start()
        return self

    def stop(self):

        self._stop.set()
        self._thread.join(timeout=5)

    def _run(self):

        while not self._stop.is_set():

            start = time.time()

            try:

                ok = bool(self.fn())

            except Exception:

                ok = False

            self.samples.append((start, ok))

            self._stop.wait(
                max(
                    0.0,
                    self.period - (time.time() - start),
                )
            )

    def window(self, t0, t1):

        return [
            (t, ok)
            for t, ok in self.samples
            if t0 <= t <= t1
        ]

    def availability(self, t0, t1):

        points = self.window(t0, t1)

        if len(points) < 2:
            return None

        uptime = sum(
            b[0] - a[0]
            for a, b in zip(points, points[1:])
            if a[1]
        )

        total = points[-1][0] - points[0][0]

        return uptime / total if total > 0 else None

    def outages(self, t0, t1):

        outages = []
        start = None

        points = self.window(t0, t1)

        for timestamp, ok in points:

            if not ok and start is None:

                start = timestamp

            elif ok and start is not None:

                outages.append(
                    (
                        start,
                        timestamp,
                        timestamp - start,
                    )
                )

                start = None

        if start is not None and points:

            outages.append(
                (
                    start,
                    points[-1][0],
                    points[-1][0] - start,
                )
            )

        return outages


def shell_restarter(template):

    """Build a restarter from a shell command template."""

    def restart(name, node):

        parsed = urlparse(node.url)

        command = template.format(
            name=name,
            url=node.url,
            host=parsed.hostname,
            port=parsed.port,
        )

        subprocess.run(
            command,
            shell=True,
            check=True,
            timeout=120,
        )

    return restart


def main():

    try:

        import host_client

        defaults = [
            f"node1={host_client.NODE1_URL}",
            f"node2={host_client.NODE2_URL}",
        ]

    except ImportError:

        defaults = [
            "node1=http://127.0.0.1:5001",
            "node2=http://127.0.0.1:5002",
        ]

    parser = argparse.ArgumentParser(
        description="Milestone 7 fault-tolerance master"
    )

    parser.add_argument(
        "--node",
        action="append",
        help="name=url (repeat); default: the VMs in host_client.py",
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="heartbeat period, seconds",
    )

    parser.add_argument(
        "--threshold",
        type=int,
        default=3,
        help="consecutive failed probes before DEAD",
    )

    parser.add_argument(
        "--timeout",
        type=float,
        default=0.8,
        help="heartbeat timeout, seconds",
    )

    parser.add_argument(
        "--restart-cmd",
        help="shell template used to restart a crashed node",
    )

    parser.add_argument(
        "--metrics-port",
        type=int,
        default=9100,
        help="Prometheus exporter port (0 disables)",
    )

    parser.add_argument(
        "--export",
        default="ft_incidents.json",
        help="where to write the incident log periodically and on exit",
    )

    args = parser.parse_args()

    nodes = dict(
        item.split("=", 1)
        for item in (args.node or defaults)
    )

    if args.metrics_port:
        start_http_server(args.metrics_port)

    master = FaultToleranceMaster(
        nodes,
        interval=args.interval,
        threshold=args.threshold,
        timeout=args.timeout,
        restarter=(
            shell_restarter(args.restart_cmd)
            if args.restart_cmd
            else None
        ),
        verbose=True,
    ).start()

    print(
        f"[master] monitoring {nodes} every {args.interval}s, "
        f"DEAD after {args.threshold} misses. Ctrl+C to stop.",
        flush=True,
    )

    try:

        while True:

            time.sleep(5)

            print(
                f"[master] {json.dumps(master.snapshot())}",
                flush=True,
            )

            # Export the latest incident log every five seconds.
            master.export(args.export)

    except KeyboardInterrupt:

        pass

    finally:

        master.stop()

        # Perform a final export when monitoring stops.
        master.export(args.export)

        print(
            f"[master] incident log written to {args.export}",
            flush=True,
        )


if __name__ == "__main__":
    main()
