"""Start the Milestone 9/10 cluster as plain local processes (no Docker needed).

    python host_pc/local_cluster.py            # edge + core + baseline :5100 + proposed :5101
    python host_pc/local_cluster.py --extra 1  # also start worker 'cloud1' on :5003 (unregistered)

Used by m9_m10_benchmark.py, the integration tests and demo_failover.py. Every process
gets its own state files under --state-dir, so runs are reproducible and isolated.
"""
import argparse
import os
import signal
import subprocess
import sys
import time

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKER_DIR = os.path.join(ROOT, "worker_vms")
HOST_DIR = os.path.join(ROOT, "host_pc")

# name -> (api port, cpu units, memory MB, peer name). Matches docker-compose.yml capacities.
WORKERS = {
    "edge": (5001, 4, 1024, "core"),
    "core": (5002, 8, 2048, "edge"),
    "cloud1": (5003, 8, 2048, "core"),
    "cloud2": (5004, 8, 2048, "core"),
}
BASELINE_PORT, PROPOSED_PORT = 5100, 5101


def url(port):
    return f"http://127.0.0.1:{port}"


class LocalCluster:
    def __init__(self, state_dir, workers=("edge", "core"), sched_tick="1.0", quiet=True,
                 proposed_env=None):
        self.state_dir = os.path.abspath(state_dir)
        os.makedirs(self.state_dir, exist_ok=True)
        self.worker_names = list(workers)
        self.sched_tick = sched_tick
        self.quiet = quiet
        self.proposed_env = dict(proposed_env or {})
        self.procs = {}
        self.logs = {}

    def _spawn(self, key, args, cwd, env):
        full_env = dict(os.environ, PYTHONUNBUFFERED="1", **env)
        if key in self.logs:
            self.logs.pop(key).close()
        log = self.logs[key] = open(os.path.join(self.state_dir, f"{key}.log"), "ab")
        kwargs = {}
        if os.name == "nt":
            kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        self.procs[key] = subprocess.Popen([sys.executable, *args], cwd=cwd, env=full_env,
                                           stdout=log, stderr=subprocess.STDOUT, **kwargs)

    def start_worker(self, name):
        port, cpu, mem, peer = WORKERS[name]
        env = {"NODE_ID": name, "NODE_PORT": str(port), "NODE_CPU": str(cpu),
               "NODE_MEMORY": str(mem), "PEER_HOST": "127.0.0.1", "PEER_PORT": str(WORKERS[peer][0]),
               "PROM_PORT": str(port + 4100), "SCHED_TICK": self.sched_tick,
               "KAFKA_BOOTSTRAP_SERVERS": "",
               "SHARED_STATE_DB": os.path.join(self.state_dir, f"{name}-state.sqlite3"),
               "FILE_STORE_DB": os.path.join(self.state_dir, f"{name}-files.sqlite3")}
        self._spawn(name, ["worker_node.py"], WORKER_DIR, env)
        self.wait(url(port) + "/api/health")

    def _nodes_env(self, names):
        return ",".join(f"{n}={url(WORKERS[n][0])}" for n in names)

    def start_gateways(self, gateways=("baseline", "proposed"), static_nodes=None):
        nodes = self._nodes_env(static_nodes or self.worker_names)
        if "baseline" in gateways:
            self._spawn("baseline", ["transparency_gateway.py"], HOST_DIR, {
                "TRANSPARENCY_NODES": nodes, "PORT": str(BASELINE_PORT),
                "NAMING_DB": os.path.join(self.state_dir, "baseline-naming.sqlite3")})
            self.wait(url(BASELINE_PORT) + "/health")
        if "proposed" in gateways:
            self._spawn("proposed", ["proposed_gateway.py"], HOST_DIR, {
                "TRANSPARENCY_NODES": nodes, "PORT": str(PROPOSED_PORT),
                "NAMING_DB": os.path.join(self.state_dir, "proposed-naming.sqlite3"), **self.proposed_env})
            self.wait(url(PROPOSED_PORT) + "/health")

    def start_ft_master(self, interval=1.0, threshold=3, timeout=0.8):
        """The Milestone 7 heartbeat master (part of the deployed baseline in docker-compose.yml)."""
        args = ["ft_master.py", "--interval", str(interval), "--threshold", str(threshold),
                "--timeout", str(timeout), "--metrics-port", "0",
                "--export", os.path.join(self.state_dir, "ft_incidents.json")]
        for name in self.worker_names:
            args += ["--node", f"{name}={url(WORKERS[name][0])}"]
        self._spawn("ft_master", args, HOST_DIR, {})

    def start(self, gateways=("baseline", "proposed"), ft_master=None):
        for name in self.worker_names:
            self.start_worker(name)
        self.start_gateways(gateways)
        if ft_master:
            self.start_ft_master(**ft_master)
        time.sleep(1.0)            # let heartbeat detectors take their first samples
        return self

    def kill(self, key):
        proc = self.procs.pop(key, None)
        if proc and proc.poll() is None:
            proc.kill()
        if proc:
            proc.wait(timeout=5)
        if key in self.logs:
            self.logs.pop(key).close()

    def stop(self):
        for key in list(self.procs):
            self.kill(key)

    @staticmethod
    def wait(health_url, timeout=30.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if requests.get(health_url, timeout=0.5).ok:
                    return
            except requests.RequestException:
                pass
            time.sleep(0.1)
        raise RuntimeError(f"{health_url} did not become healthy within {timeout}s")

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--state-dir", default=os.path.join(ROOT, ".local_cluster"))
    parser.add_argument("--extra", type=int, default=0, help="also start cloud1..cloudN (not registered)")
    args = parser.parse_args()
    cluster = LocalCluster(args.state_dir).start()
    for i in range(args.extra):
        cluster.start_worker(f"cloud{i + 1}")
    print("edge     ", url(5001), "\ncore     ", url(5002))
    for i in range(args.extra):
        print(f"cloud{i + 1}   ", url(WORKERS[f'cloud{i + 1}'][0]), "(register via POST /api/workers/register)")
    print("baseline ", url(BASELINE_PORT), "\nproposed ", url(PROPOSED_PORT), "\nCtrl+C to stop.")
    try:
        signal.signal(signal.SIGINT, signal.default_int_handler)
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        cluster.stop()


if __name__ == "__main__":
    main()
