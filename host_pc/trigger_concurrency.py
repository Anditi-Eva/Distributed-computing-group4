"""Run distributed contention/deadlock experiments against edge and core VMs."""
import argparse
import json
import threading
import time
import uuid

import requests

DEFAULT_NODES = {
    "Mode1-edge": "http://192.168.56.104:7001",
    "node2-core": "http://192.168.56.107:7001",
}
TERMINAL = {"COMPLETED", "ABORTED", "TIMED_OUT", "FAILED"}


def post_task(name, base_url, pid, cpu, mem, order, hold):
    response = requests.post(f"{base_url}/api/execute_concurrent", json={
        "pid": pid, "cpu": cpu, "mem": mem, "resource_order": order,
        "hold_seconds": hold, "wait_timeout": max(hold * 4, 5),
    }, timeout=5)
    response.raise_for_status()
    return {"node": name, "pid": pid, "response": response.json()}


def fetch_json(url):
    response = requests.get(url, timeout=3)
    response.raise_for_status()
    return response.json()


def run(args):
    nodes = {"Mode1-edge": args.edge.rstrip("/"), "node2-core": args.core.rstrip("/")}
    suffix = uuid.uuid4().hex[:8]
    submissions = []
    errors = []
    lock = threading.Lock()

    def submit(name, url, index):
        if args.scenario == "deadlock":
            order = ["CPU", "MEMORY"] if index % 2 == 0 else ["MEMORY", "CPU"]
        else:
            order = ["CPU", "MEMORY"]
        pid = f"{name}-{suffix}-T{index}"
        try:
            result = post_task(name, url, pid, args.cpu, args.memory, order, args.hold)
            with lock:
                submissions.append(result)
        except Exception as exc:
            with lock:
                errors.append({"node": name, "pid": pid, "error": str(exc)})

    threads = []
    for name, url in nodes.items():
        for index in range(args.tasks):
            thread = threading.Thread(target=submit, args=(name, url, index), daemon=True)
            threads.append(thread)
            thread.start()
    for thread in threads:
        thread.join()

    # Poll both nodes while work is running to capture a utilization peak.
    deadline = time.monotonic() + args.timeout
    peak = {name: {"CPU": 0.0, "MEMORY": 0.0} for name in nodes}
    latest = {}
    while time.monotonic() < deadline:
        active = False
        for name, url in nodes.items():
            try:
                snapshot = fetch_json(f"{url}/api/resources")
                latest[name] = snapshot
                for kind in ("CPU", "MEMORY"):
                    peak[name][kind] = max(peak[name][kind], snapshot["utilization_percent"][kind])
                if any(task["pid"].startswith(f"{name}-{suffix}-") and task["status"] not in TERMINAL
                       for task in snapshot["tasks"]):
                    active = True
            except Exception as exc:
                errors.append({"node": name, "error": f"poll: {exc}"})
        if submissions and not active:
            break
        time.sleep(0.1)

    per_node = {}
    all_tasks = []
    for name, url in nodes.items():
        try:
            task_data = fetch_json(f"{url}/api/tasks").get("tasks", [])
            mine = [task for task in task_data if task["pid"].startswith(f"{name}-{suffix}-")]
            all_tasks.extend(mine)
            snapshot = fetch_json(f"{url}/api/resources")
            per_node[name] = {
                "capacity": snapshot["capacity"],
                "current_utilization_percent": snapshot["utilization_percent"],
                "peak_utilization_percent": peak[name],
                "deadlocks_detected": snapshot["deadlocks_detected"],
                "deadlocks_recovered": snapshot["deadlocks_recovered"],
                "deadlock_events": snapshot["deadlock_events"],
                "mean_resource_wait_ms": snapshot["mean_resource_wait_ms"],
                "graph": snapshot["resource_allocation_graph"],
                "task_counts": {status: sum(t["status"] == status for t in mine)
                                for status in ("COMPLETED", "ABORTED", "TIMED_OUT", "FAILED", "RUNNING", "WAITING")},
            }
        except Exception as exc:
            errors.append({"node": name, "error": f"final snapshot: {exc}"})

    elapsed = sum((task.get("finished_at") or time.time()) - task["submitted_at"] for task in all_tasks)
    completed = sum(task["status"] == "COMPLETED" for task in all_tasks)
    result = {
        "scenario": args.scenario,
        "nodes": per_node,
        "tasks_submitted": len(submissions),
        "tasks_observed": len(all_tasks),
        "completed": completed,
        "aborted": sum(task["status"] == "ABORTED" for task in all_tasks),
        "timed_out": sum(task["status"] == "TIMED_OUT" for task in all_tasks),
        "failed": sum(task["status"] == "FAILED" for task in all_tasks),
        "mean_task_elapsed_seconds": elapsed / len(all_tasks) if all_tasks else None,
        "aggregate_completion_rate_tasks_per_second": completed / elapsed if elapsed else 0,
        "errors": errors,
        "task_results": all_tasks,
    }
    print(json.dumps(result, indent=2))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=("contention", "deadlock"), default="deadlock")
    parser.add_argument("--edge", default=DEFAULT_NODES["Mode1-edge"])
    parser.add_argument("--core", default=DEFAULT_NODES["node2-core"])
    parser.add_argument("--tasks", type=int, default=2, help="Tasks per VM; deadlock scenario uses at least two")
    parser.add_argument("--cpu", type=int, default=1, help="CPU resource units requested per task")
    parser.add_argument("--memory", type=int, default=1, help="Memory resource units requested per task")
    parser.add_argument("--hold", type=float, default=1.0, help="Seconds to hold resources after acquisition")
    parser.add_argument("--timeout", type=float, default=30.0, help="Maximum experiment polling time")
    args = parser.parse_args()
    if args.tasks < (2 if args.scenario == "deadlock" else 1):
        parser.error("deadlock scenario needs at least two tasks per VM")
    run(args)


if __name__ == "__main__":
    main()
