"""Concurrent resource simulator with a resource-allocation graph and recovery."""
import sys
import threading
import time
from collections import defaultdict, deque

import networkx as nx
from flask import Flask, jsonify, request
from prometheus_client import Counter, Gauge, start_http_server

app = Flask(__name__)
CPU_UTILIZATION = Gauge("node_cpu_utilization", "Allocated simulated CPU capacity percent")
MEM_UTILIZATION = Gauge("node_memory_utilization", "Allocated simulated memory capacity percent")
DEADLOCK_COUNT = Counter("node_deadlocks_detected_total", "Detected resource-allocation graph cycles")
RECOVERY_COUNT = Counter("node_deadlocks_recovered_total", "Deadlocks recovered by aborting a victim")
TASKS_COMPLETED = Counter("node_concurrent_tasks_completed_total", "Concurrent tasks completed")
TASKS_ABORTED = Counter("node_concurrent_tasks_aborted_total", "Concurrent tasks aborted by deadlock recovery")


class TaskAborted(Exception):
    pass


class ResourceManager:
    """Each CPU and memory unit is a distinct single-instance graph resource."""
    def __init__(self, cpu, memory):
        if cpu < 1 or memory < 1:
            raise ValueError("CPU and memory capacity must be positive")
        self.capacities = {"CPU": cpu, "MEMORY": memory}
        self.units = {name: [f"{name}#{i + 1}" for i in range(count)]
                      for name, count in self.capacities.items()}
        self.condition = threading.Condition(threading.RLock())
        self.allocations = defaultdict(set)   # pid -> concrete resource instances
        self.owners = {}                      # resource instance -> pid
        self.waiting = {}                     # pid -> (resource type, units needed, enqueue time)
        self.wait_queues = {name: deque() for name in self.capacities}
        self.tasks = {}
        self.graph = nx.DiGraph()
        self.graph_lock = self.condition
        self.total_deadlocks = 0
        self.total_recoveries = 0
        self.deadlock_events = []
        self.wait_samples = []

    def _refresh_metrics(self):
        for label, gauge in (("CPU", CPU_UTILIZATION), ("MEMORY", MEM_UTILIZATION)):
            used = sum(unit in self.owners for unit in self.units[label])
            gauge.set(100.0 * used / self.capacities[label])

    def _rebuild_graph(self):
        graph = nx.DiGraph()
        for pid, held in self.allocations.items():
            proc = f"P:{pid}"
            graph.add_node(proc, kind="process")
            for unit in held:
                graph.add_edge(f"R:{unit}", proc, relation="allocated")
        for pid, (kind, count, _enqueued) in self.waiting.items():
            proc = f"P:{pid}"
            graph.add_node(proc, kind="process")
            free = [unit for unit in self.units[kind] if unit not in self.owners]
            needed_from_owners = max(0, count - len(free))
            owners = sorted({self.owners[u] for u in self.units[kind] if u in self.owners})
            blockers = owners[:needed_from_owners] if needed_from_owners else []
            for blocker in blockers:
                for unit in self.allocations.get(blocker, ()):
                    if unit.startswith(f"{kind}#"):
                        graph.add_edge(proc, f"R:{unit}", relation="waiting")
        self.graph = graph

    def _find_deadlock(self):
        try:
            cycle = nx.find_cycle(self.graph)
            return [edge[0] for edge in cycle] + [cycle[0][0]]
        except nx.NetworkXNoCycle:
            return None

    def _recover_cycle(self, cycle):
        victims = sorted({node[2:] for node in cycle if node.startswith("P:")})
        if not victims:
            return None
        # Deterministically abort the youngest process involved in the cycle.
        victim = max(victims, key=lambda pid: self.tasks.get(pid, {}).get("submitted_at", 0))
        task = self.tasks.get(victim)
        if task and task["status"] not in ("COMPLETED", "ABORTED"):
            task["status"] = "ABORTED"
            task["error"] = "Deadlock victim selected by recovery policy"
            task["finished_at"] = time.time()
            self.waiting.pop(victim, None)
            for queue in self.wait_queues.values():
                try:
                    queue.remove(victim)
                except ValueError:
                    pass
            for unit in self.allocations.pop(victim, set()):
                self.owners.pop(unit, None)
            self.total_recoveries += 1
            self.deadlock_events.append({"time": time.time(), "cycle": list(cycle), "victim": victim,
                                         "recovery": "abort_and_release_resources"})
            TASKS_ABORTED.inc()
            RECOVERY_COUNT.inc()
            self._refresh_metrics()
            self._rebuild_graph()
            self.condition.notify_all()
            return victim
        return None

    def acquire(self, pid, kind, count, wait_timeout):
        started = time.monotonic()
        deadline = started + wait_timeout
        with self.condition:
            enqueued = time.time()
            self.waiting[pid] = (kind, count, enqueued)
            self.wait_queues[kind].append(pid)
            self._rebuild_graph()
            cycle = self._find_deadlock()
            if cycle:
                self.total_deadlocks += 1
                DEADLOCK_COUNT.inc()
                victim = self._recover_cycle(cycle)
                print(f"[{node_id}] DEADLOCK {cycle}; recovered by aborting {victim}", flush=True)
            while True:
                task = self.tasks[pid]
                if task["status"] == "ABORTED":
                    self.waiting.pop(pid, None)
                    try:
                        self.wait_queues[kind].remove(pid)
                    except ValueError:
                        pass
                    self._rebuild_graph()
                    raise TaskAborted(task.get("error", "aborted"))
                free = [unit for unit in self.units[kind] if unit not in self.owners]
                queue = self.wait_queues[kind]
                if len(free) >= count and queue and queue[0] == pid:
                    acquired = set(free[:count])
                    self.waiting.pop(pid, None)
                    queue.popleft()
                    self.allocations[pid].update(acquired)
                    for unit in acquired:
                        self.owners[unit] = pid
                    task["held_resources"] = sorted(self.allocations[pid])
                    task["status"] = "HOLDING"
                    self.wait_samples.append(time.monotonic() - started)
                    self._refresh_metrics()
                    self._rebuild_graph()
                    self.condition.notify_all()
                    return
                remaining = deadline - time.monotonic()
                task["status"] = "WAITING"
                if remaining <= 0:
                    self.waiting.pop(pid, None)
                    try:
                        self.wait_queues[kind].remove(pid)
                    except ValueError:
                        pass
                    self._rebuild_graph()
                    raise TimeoutError(f"Timed out waiting for {count} {kind} unit(s)")
                self.condition.wait(min(remaining, 0.2))
                self._rebuild_graph()
                cycle = self._find_deadlock()
                if cycle:
                    self.total_deadlocks += 1
                    DEADLOCK_COUNT.inc()
                    victim = self._recover_cycle(cycle)
                    print(f"[{node_id}] DEADLOCK {cycle}; recovered by aborting {victim}", flush=True)

    def release_kind(self, pid, kind):
        with self.condition:
            held = self.allocations.get(pid, set())
            releasing = {unit for unit in held if unit.startswith(f"{kind}#")}
            for unit in releasing:
                held.remove(unit)
                self.owners.pop(unit, None)
            self.tasks[pid]["held_resources"] = sorted(held)
            self._refresh_metrics()
            self._rebuild_graph()
            self.condition.notify_all()

    def finish(self, pid, status, error=None):
        with self.condition:
            task = self.tasks[pid]
            if task["status"] != "ABORTED":
                task["status"] = status
                task["error"] = error
            task["finished_at"] = time.time()
            self.waiting.pop(pid, None)
            for queue in self.wait_queues.values():
                try:
                    queue.remove(pid)
                except ValueError:
                    pass
            for unit in self.allocations.pop(pid, set()):
                self.owners.pop(unit, None)
            task["held_resources"] = []
            self._refresh_metrics()
            self._rebuild_graph()
            self.condition.notify_all()

    def snapshot(self):
        with self.condition:
            self._rebuild_graph()
            allocated = {kind: sum(unit in self.owners for unit in units)
                         for kind, units in self.units.items()}
            return {
                "node_id": node_id,
                "capacity": self.capacities,
                "allocated": allocated,
                "available": {kind: self.capacities[kind] - allocated[kind] for kind in self.capacities},
                "utilization_percent": {kind: 100.0 * allocated[kind] / self.capacities[kind] for kind in self.capacities},
                "waiting": {pid: {"resource": kind, "units": count} for pid, (kind, count, _) in self.waiting.items()},
                "tasks": [dict(task) for task in self.tasks.values()],
                "deadlocks_detected": self.total_deadlocks,
                "deadlocks_recovered": self.total_recoveries,
                "deadlock_events": list(self.deadlock_events),
                "mean_resource_wait_ms": (1000 * sum(self.wait_samples) / len(self.wait_samples)) if self.wait_samples else 0,
                "resource_allocation_graph": [{"from": a, "to": b, "relation": self.graph.edges[a, b]["relation"]}
                                              for a, b in self.graph.edges],
            }


manager = None
node_id = ""


def run_task(pid, request_cpu, request_mem, order, hold_seconds, wait_timeout):
    acquired_kinds = []
    try:
        for index, kind in enumerate(order):
            units = request_cpu if kind == "CPU" else request_mem
            if units:
                manager.acquire(pid, kind, units, wait_timeout)
                acquired_kinds.append(kind)
            # A short deliberate gap makes the opposite-order experiment reproducible.
            if index == 0:
                time.sleep(0.25)
        with manager.condition:
            if manager.tasks[pid]["status"] == "ABORTED":
                raise TaskAborted("Deadlock victim selected by recovery policy")
            manager.tasks[pid]["status"] = "RUNNING"
            manager.tasks[pid]["started_at"] = time.time()
        time.sleep(hold_seconds)
        manager.finish(pid, "COMPLETED")
        TASKS_COMPLETED.inc()
    except TaskAborted:
        manager.finish(pid, "ABORTED", "Deadlock recovery aborted this process")
    except TimeoutError as exc:
        manager.finish(pid, "TIMED_OUT", str(exc))
    except Exception as exc:
        manager.finish(pid, "FAILED", str(exc))


@app.post("/api/execute_concurrent")
def execute_concurrent():
    data = request.get_json(silent=True) or {}
    pid = str(data.get("pid", "")).strip()
    try:
        cpu, mem = int(data.get("cpu", 0)), int(data.get("mem", 0))
        order = [str(x).upper() for x in data.get("resource_order", ["CPU", "MEMORY"])]
        hold = float(data.get("hold_seconds", 1.0))
        wait_timeout = float(data.get("wait_timeout", 15.0))
        if not pid or cpu < 0 or mem < 0 or (cpu == 0 and mem == 0):
            raise ValueError("pid and at least one positive resource request are required")
        if not order or any(x not in ("CPU", "MEMORY") for x in order) or len(set(order)) != len(order):
            raise ValueError("resource_order must contain unique values from CPU and MEMORY")
        if any((kind == "CPU" and cpu > manager.capacities["CPU"]) or
               (kind == "MEMORY" and mem > manager.capacities["MEMORY"]) for kind in order):
            raise ValueError("requested resource exceeds node capacity")
        if hold < 0 or wait_timeout <= 0:
            raise ValueError("hold_seconds must be nonnegative and wait_timeout positive")
    except (TypeError, ValueError) as exc:
        return jsonify({"status": "ERROR", "error": str(exc)}), 400
    with manager.condition:
        if pid in manager.tasks and manager.tasks[pid]["status"] not in ("COMPLETED", "ABORTED", "TIMED_OUT", "FAILED"):
            return jsonify({"status": "ERROR", "error": "pid is already active"}), 409
        manager.tasks[pid] = {"pid": pid, "node_id": node_id, "status": "QUEUED",
                              "requested": {"CPU": cpu, "MEMORY": mem}, "resource_order": order,
                              "submitted_at": time.time(), "started_at": None, "finished_at": None,
                              "held_resources": [], "error": None}
    threading.Thread(target=run_task, args=(pid, cpu, mem, order, hold, wait_timeout), daemon=True).start()
    return jsonify({"status": "QUEUED", "pid": pid, "node_id": node_id}), 202


@app.get("/api/tasks")
def tasks():
    with manager.condition:
        return jsonify({"node_id": node_id, "tasks": list(manager.tasks.values())})


@app.get("/api/status")
def status():
    return jsonify(manager.snapshot())


@app.get("/api/resources")
def resources():
    return jsonify(manager.snapshot())


@app.get("/api/deadlock_check")
def deadlock_check():
    with manager.condition:
        manager._rebuild_graph()
        cycle = manager._find_deadlock()
        return jsonify({"status": "DEADLOCK DETECTED" if cycle else "NO DEADLOCK",
                        "cycle": cycle, "resource_allocation_graph": manager.snapshot()["resource_allocation_graph"],
                        "recovered_count": manager.total_recoveries})


@app.post("/api/recover")
def recover():
    """Run cycle detection and abort a victim if a deadlock is currently present."""
    with manager.condition:
        manager._rebuild_graph()
        cycle = manager._find_deadlock()
        if not cycle:
            return jsonify({"status": "NO DEADLOCK", "recovered": False})
        manager.total_deadlocks += 1
        DEADLOCK_COUNT.inc()
        victim = manager._recover_cycle(cycle)
        return jsonify({"status": "DEADLOCK RECOVERED" if victim else "RECOVERY FAILED",
                        "cycle": cycle, "victim": victim, "recovered": bool(victim)})


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python concurrent_worker.py <node_id> <cpu_units> <memory_units> [api_port] [metrics_port]")
        raise SystemExit(1)
    node_id = sys.argv[1]
    manager = ResourceManager(int(sys.argv[2]), int(sys.argv[3]))
    api_port = int(sys.argv[4]) if len(sys.argv) > 4 else 7001
    metrics_port = int(sys.argv[5]) if len(sys.argv) > 5 else 8000
    start_http_server(metrics_port)
    print(f"[{node_id}] API on {api_port}; Prometheus metrics on {metrics_port}", flush=True)
    app.run(host="0.0.0.0", port=api_port, threaded=True)
