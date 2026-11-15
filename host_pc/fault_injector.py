"""Milestone 7 - Fault-injection framework (runs on the Host PC).

Pieces
------
ChaosClient   talks to the /chaos/* control endpoints that every worker exposes.
Scenario      one controlled failure: what to inject, what a supervisor does about it
              (restart delay, whether the disk survives) and how it maps to the failure taxonomy.
SCENARIOS     the library used in the experiments: crash, node failure, service failure,
              omission, communication delay, network partition, Byzantine backup.
LocalCluster  starts/kills/restarts worker_node.py processes on this machine so the whole
              framework can be exercised without VMs. On the real VMs the same scenarios work
              through ChaosClient; only the restart step is replaced by ft_master's --restart-cmd.
"""
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
WORKER_DIR = os.path.join(os.path.dirname(HERE), "worker_vms")


class ChaosClient:
    def __init__(self, timeout=2.0):
        self.timeout = timeout

    def inject(self, url, fault_type, **params):
        r = requests.post(f"{url}/chaos/inject", json={"type": fault_type, **params}, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def clear(self, url, fault_type=None):
        body = {"type": fault_type} if fault_type else {}
        return requests.post(f"{url}/chaos/clear", json=body, timeout=self.timeout).json()

    def status(self, url):
        return requests.get(f"{url}/chaos/status", timeout=self.timeout).json()


class LocalCluster:
    """N worker_node.py processes on localhost; node i replicates to node i+1 (ring)."""

    def __init__(self, names=("node1", "node2"), base_port=5001, base_metrics=8001, replication="async",
                 repl_interval=0.2, cpu=10000, memory=10 ** 7, workdir=None, wal="flush"):
        self.names = list(names)
        self.ports = {n: base_port + i for i, n in enumerate(self.names)}
        self.metrics = {n: base_metrics + i for i, n in enumerate(self.names)}
        self.replication, self.repl_interval, self.cpu, self.memory = replication, repl_interval, cpu, memory
        self.wal = wal
        self.workdir = workdir or tempfile.mkdtemp(prefix="ft_cluster_")
        self.ckpt = os.path.join(self.workdir, "checkpoints")
        os.makedirs(self.ckpt, exist_ok=True)
        self.procs, self.policy = {}, {}
        self.kills = {}                                   # name -> time of last kill (ground truth)
        self.ready_at = {}                                # name -> times at which the node became ready
        self.lock = threading.Lock()

    def url(self, name):
        return f"http://127.0.0.1:{self.ports[name]}"

    def urls(self):
        return {n: self.url(n) for n in self.names}

    def start(self, name, wait=True):
        i = self.names.index(name)
        peer = self.names[(i + 1) % len(self.names)]
        log = open(os.path.join(self.workdir, f"{name}.log"), "ab")
        self.procs[name] = subprocess.Popen(
            [sys.executable, "worker_node.py", name, str(self.ports[name]), str(self.cpu), str(self.memory),
             "127.0.0.1", str(self.ports[peer]), "--metrics-port", str(self.metrics[name]),
             "--replication", self.replication, "--repl-interval", str(self.repl_interval), "--wal", self.wal,
             "--checkpoint-dir", self.ckpt],
            cwd=WORKER_DIR, stdout=log, stderr=subprocess.STDOUT)
        if wait:
            self.wait_ready(name)

    def start_all(self):
        for n in self.names:
            self.start(n, wait=False)
        for n in self.names:
            self.wait_ready(n)

    def wait_ready(self, name, timeout=15.0):
        end = time.time() + timeout
        while time.time() < end:
            try:
                if requests.get(f"{self.url(name)}/health", timeout=0.5).status_code in (200, 503):
                    self.ready_at.setdefault(name, []).append(time.time())
                    return True
            except requests.exceptions.RequestException:
                time.sleep(0.1)
        raise RuntimeError(f"{name} did not come up; see {self.workdir}/{name}.log")

    def alive(self, name):
        p = self.procs.get(name)
        return p is not None and p.poll() is None

    def kill(self, name):
        """SIGKILL: the process vanishes with no chance to clean up (node power-off)."""
        p = self.procs.get(name)
        if p and p.poll() is None:
            self.kills[name] = time.time()
            p.kill()
            p.wait(timeout=5)

    def responsive(self, name):
        try:
            return requests.get(f"{self.url(name)}/health", timeout=0.5).status_code == 200
        except requests.exceptions.RequestException:
            return False

    def ensure_up(self, name):
        with self.lock:
            if not self.alive(name):
                self.start(name)

    def supervise(self, name, delay):
        """Model of systemd Restart=always: relaunch the process ``delay`` seconds after it exits."""
        proc = self.procs[name]

        def run():
            proc.wait()
            time.sleep(delay)
            self.ensure_up(name)
        threading.Thread(target=run, daemon=True).start()

    def restart(self, name, delay=None, wipe_disk=None):
        """Recovery action requested by the master. delay/wipe_disk default to the scenario's policy
        (VM reboot time, whether local disk survives). Does nothing if the node is already serving."""
        if self.alive(name) and self.responsive(name):
            return
        pol = self.policy.get(name, {})
        delay = pol.get("delay", 0.0) if delay is None else delay
        wipe_disk = pol.get("wipe", False) if wipe_disk is None else wipe_disk
        self.kill(name)
        time.sleep(delay)                                  # time the node takes to come back
        if wipe_disk:
            for fname in (f"checkpoint_{name}.json", f"wal_{name}.log"):
                path = os.path.join(self.ckpt, fname)
                if os.path.exists(path):
                    os.remove(path)
        self.ensure_up(name)

    def restarter(self, name, node=None):
        """Callable handed to FaultToleranceMaster."""
        self.restart(name)

    def stop_all(self):
        for n in self.names:
            p = self.procs.get(n)
            if p and p.poll() is None:
                p.send_signal(signal.SIGTERM) if os.name != "nt" else p.terminate()
        for n in self.names:
            p = self.procs.get(n)
            if p:
                try:
                    p.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    p.kill()

    def cleanup(self):
        self.stop_all()
        shutil.rmtree(self.workdir, ignore_errors=True)

    def cpu_seconds(self, name):
        """User+system CPU time of a worker process (resource-usage comparison); None if unavailable."""
        try:
            import psutil
            t = psutil.Process(self.procs[name].pid).cpu_times()
            return t.user + t.system
        except Exception:                                   # noqa: BLE001
            return None


@dataclass
class Scenario:
    key: str
    taxonomy: str                       # failure class from the assignment
    description: str
    target: str = "node1"
    fault: str = None                   # fault type for /chaos/inject; None = out-of-band (process kill)
    params: dict = field(default_factory=dict)
    restart_delay: float = 0.0          # how long the supervisor/VM takes to bring the node back
    wipe_disk: bool = False             # node failure loses local checkpoints
    expected_detection: str = "DEAD"    # what the master should notice: DEAD | REPLICATION | BYZANTINE
    supervised: bool = False            # a local supervisor (systemd Restart=always) relaunches the process
    recovery_via: str = "failback"      # how the node is expected to return to service


SCENARIOS = {s.key: s for s in [
    Scenario("crash", "Crash failure",
             "Worker process exits abruptly (fail-stop). A supervisor restarts it after 1 s; the local "
             "checkpoint and write-ahead log survive.", fault="crash", restart_delay=1.0, supervised=True, recovery_via="supervisor restart"),
    Scenario("node_failure", "Node failure",
             "The whole node disappears (SIGKILL) and needs 6 s to reboot; local disk (checkpoint + WAL) is lost, so the "
             "state must come back from the replica.", restart_delay=6.0, wipe_disk=True),
    Scenario("service", "Service failure",
             "Process stays up but the service is broken: /health returns 503 and the API returns 500 until the "
             "service is restarted.", fault="service"),
    Scenario("omission", "Omission failure",
             "70% of incoming requests are silently dropped for 10 s (transient).", fault="omission",
             params={"probability": 0.7, "duration": 10}, recovery_via="service restart / fault expiry"),
    Scenario("delay", "Communication failure (latency)",
             "All traffic to the node is delayed by 1.5 s for 8 s (congested or degraded link).", fault="delay",
             params={"delay_ms": 1500, "duration": 8}, recovery_via="service restart / fault expiry"),
    Scenario("partition", "Communication failure (partition)",
             "The node-to-node link is cut for 8 s: master and clients still reach both nodes, "
             "replication cannot.", fault="partition", params={"duration": 8}, expected_detection="REPLICATION",
             recovery_via="replication catch-up"),
    Scenario("byzantine_backup", "Byzantine failure",
             "The backup acknowledges replication messages but silently stores nothing.", target="node2",
             fault="byzantine", params={"mode": "lying_ack"}, expected_detection="BYZANTINE",
             recovery_via="quarantine"),
]}


class FaultInjector:
    """Applies a Scenario to a cluster and returns the ground-truth injection time."""

    def __init__(self, urls, chaos=None, cluster=None):
        self.urls, self.chaos, self.cluster = urls, chaos or ChaosClient(), cluster
        self.log = []

    def inject(self, scn):
        if self.cluster is not None:
            self.cluster.policy[scn.target] = {"delay": scn.restart_delay, "wipe": scn.wipe_disk}
        if scn.supervised and self.cluster is not None:
            self.cluster.supervise(scn.target, scn.restart_delay)
        t = time.time()
        if scn.fault is None:
            if self.cluster is None:
                raise RuntimeError(f"scenario {scn.key} needs a LocalCluster (process kill)")
            self.cluster.kill(scn.target)
        else:
            try:
                self.chaos.inject(self.urls[scn.target], scn.fault, **scn.params)
            except requests.exceptions.RequestException:
                if scn.fault != "crash":                    # a crash may cut the reply short
                    raise
        self.log.append({"t": t, "scenario": scn.key, "target": scn.target})
        return t
