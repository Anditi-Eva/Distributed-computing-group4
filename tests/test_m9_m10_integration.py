"""Milestone 9/10 integration tests against REAL worker processes (no mocks).

    python -m unittest tests/test_m9_m10_integration.py -v

Starts edge + core workers and both gateways on 127.0.0.1 (ports 5001-5003, 5100, 5101).
"""
import json
import os
import shutil
import sys
import tempfile
import time
import unittest

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "host_pc"))
from local_cluster import BASELINE_PORT, PROPOSED_PORT, WORKERS, LocalCluster, url  # noqa: E402

B0, P = url(BASELINE_PORT), url(PROPOSED_PORT)
EDGE, CORE = url(WORKERS["edge"][0]), url(WORKERS["core"][0])


def process(name, burst=100000):
    return {"name": name, "cpu_needed": 1, "memory_needed": 64, "remaining_burst": burst, "slice_type": "URLLC"}


def owner_of(rid, workers):
    for name, base in workers.items():
        r = requests.get(f"{base}/api/process/{rid}", timeout=1)
        if r.ok and r.json().get("status") == "running":
            return name
    return None


class M9M10Integration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.state = tempfile.mkdtemp(prefix="m9m10-")
        cls.cluster = LocalCluster(cls.state).start()

    @classmethod
    def tearDownClass(cls):
        cls.cluster.stop()
        shutil.rmtree(cls.state, ignore_errors=True)

    def setUp(self):
        for base in (EDGE, CORE):
            requests.post(f"{base}/api/reset", timeout=3)

    def test_lifecycle_on_both_gateways(self):
        for gw in (B0, P):
            created = requests.post(f"{gw}/api/processes", json=process("life")).json()
            rid = created["resource_id"]
            self.assertEqual(created["replication"], "synchronous", gw)
            located = requests.get(f"{gw}/api/processes/process/{rid}").json()
            self.assertEqual(located["status"], "running", gw)
            done = requests.post(f"{gw}/api/processes/process/{rid}/terminate").json()
            self.assertEqual(done["status"], "terminated", gw)
            self.assertEqual(done["coordination_acks"], 2, gw)     # owner + standby
            after = requests.get(f"{gw}/api/names/resolve/process/{rid}").json()
            self.assertEqual(after["status"], "terminated", gw)   # cache stays coherent

    def test_location_is_hidden(self):
        created = requests.post(f"{P}/api/slices", json=process("hidden"))
        located = requests.get(f"{P}/api/slices/{created.json()['resource_id']}")
        for body in (created.text, located.text):
            self.assertNotIn("127.0.0.1", body)
            self.assertNotIn(":5001", body)
            self.assertNotIn(":5002", body)

    def test_direct_owner_routing_uses_one_message(self):
        rid = requests.post(f"{P}/api/processes", json=process("route")).json()["resource_id"]
        r = requests.get(f"{P}/api/slices/{rid}")
        self.assertEqual(r.headers["X-Routing"], "direct_owner")
        self.assertEqual(r.headers["X-Worker-Messages"], "1")

    def test_targeted_promotion_only_activates_failed_nodes_replicas(self):
        base = {"cpu_needed": 0, "memory_needed": 0, "remaining_burst": 1000}
        requests.post(f"{EDGE}/api/replica", json={"process": {"pid": "from-a", "name": "a", **base},
                                                   "primary": "node-a"})
        requests.post(f"{EDGE}/api/replica", json={"process": {"pid": "from-b", "name": "b", **base},
                                                   "primary": "node-b"})
        promoted = requests.post(f"{EDGE}/api/replicas/promote", json={"failed_node": "node-a"}).json()
        self.assertEqual(promoted["promoted"], ["from-a"])
        self.assertEqual(requests.get(f"{EDGE}/api/process/from-b").json()["status"], "replicated")

    def test_proactive_failover_and_rejoin(self):
        rid = requests.post(f"{P}/api/processes", json=process("failover")).json()["resource_id"]
        time.sleep(0.3)
        workers = {"edge": EDGE, "core": CORE}
        owner = owner_of(rid, workers)
        survivor = "edge" if owner == "core" else "core"
        t0 = time.time()
        try:
            requests.post(f"{workers[owner]}/chaos/inject", json={"type": "crash"}, timeout=1)
        except requests.RequestException:
            pass
        resumed = None
        while time.time() - t0 < 4:                      # observer polls the survivor, NOT the gateway
            if owner_of(rid, {survivor: workers[survivor]}):
                resumed = time.time() - t0
                break
            time.sleep(0.02)
        self.assertIsNotNone(resumed, "standby was not promoted without client traffic")
        self.assertLess(resumed, 2.0)
        self.assertEqual(requests.get(f"{P}/api/slices/{rid}").json()["status"], "running")
        self.cluster.start_worker(owner)
        deadline = time.time() + 5
        while time.time() < deadline and not requests.get(f"{P}/api/membership").json()["nodes"][owner]["alive"]:
            time.sleep(0.1)
        time.sleep(0.5)
        events = [e for e in requests.get(f"{P}/api/membership").json()["events"] if e["event"] == "recovered"]
        self.assertTrue(events and events[-1]["reprotected_replicas"] >= 1, json.dumps(events))
        self.assertEqual(requests.get(f"{workers[owner]}/api/process/{rid}").json()["status"], "replicated")

    def test_dynamic_worker_registration_and_lease_expiry(self):
        self.cluster.start_worker("cloud1")
        cloud = url(WORKERS["cloud1"][0])
        r = requests.post(f"{P}/api/workers/register", json={"name": "cloud1", "url": cloud, "lease_s": 1.5})
        self.assertEqual(r.status_code, 201)
        self.assertEqual(requests.get(f"{P}/api/names/resolve/worker/cloud1").json()["status"], "resolved")
        time.sleep(0.6)
        self.assertEqual(requests.get(f"{P}/api/nodes").json()["available_nodes"], 3)
        self.cluster.kill("cloud1")
        time.sleep(3.5)                                   # suspicion + 1.5 s lease
        self.assertNotIn("cloud1", requests.get(f"{P}/api/membership").json()["nodes"])
        self.assertEqual(requests.get(f"{P}/api/names/resolve/worker/cloud1").status_code, 404)


if __name__ == "__main__":
    unittest.main()
