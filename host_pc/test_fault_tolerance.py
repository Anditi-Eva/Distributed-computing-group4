"""Milestone 7 test-suite.  Run from host_pc/:   python -m unittest test_fault_tolerance -v

Unit tests need nothing; integration tests start real worker_node.py processes on localhost
(ports 5700-5799, about 1 minute in total).
"""
import os
import sys
import time
import unittest

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "worker_vms"))

from fault_injection import FaultInjector as WorkerFaults        # noqa: E402  (worker side)
from fault_injector import SCENARIOS, ChaosClient, FaultInjector, LocalCluster   # noqa: E402
from ft_experiments import LoadGen, held_pids, quorum_read       # noqa: E402
from ft_master import DEAD, UP, AvailabilityProbe, FaultToleranceMaster   # noqa: E402


class WorkerFaultTests(unittest.TestCase):
    def test_rejects_unknown_fault_and_bad_parameters(self):
        f = WorkerFaults("t")
        with self.assertRaises(ValueError):
            f.inject("meteor")
        with self.assertRaises(ValueError):
            f.inject("omission", probability=1.5)
        with self.assertRaises(ValueError):
            f.inject("byzantine", mode="mystery")

    def test_transient_fault_expires_by_itself(self):
        f = WorkerFaults("t")
        f.inject("service", duration=0.2)
        self.assertIsNotNone(f.get("service"))
        time.sleep(0.3)
        self.assertIsNone(f.get("service"))

    def test_service_fault_breaks_health_and_api_but_not_control_plane(self):
        f = WorkerFaults("t")
        f.inject("service")
        self.assertEqual(f.request_gate("/health", False)[1], 503)
        self.assertEqual(f.request_gate("/api/submit", False)[1], 500)
        self.assertIsNone(f.request_gate("/chaos/clear", False))        # an experiment can always clear faults
        self.assertIsNone(f.request_gate("/admin/restart_service", False))

    def test_partition_drops_only_peer_traffic(self):
        f = WorkerFaults("t")
        f.inject("partition")
        self.assertTrue(f.link_blocked())
        self.assertIsNone(f.request_gate("/api/status", from_peer=False))

    def test_service_restart_clears_service_level_faults_only(self):
        f = WorkerFaults("t")
        for kind in ("service", "omission", "delay", "partition"):
            f.inject(kind)
        cleared = f.clear_service_level()
        self.assertCountEqual(cleared, ["service", "omission", "delay"])
        self.assertIsNotNone(f.get("partition"))

    def test_byzantine_modes(self):
        f = WorkerFaults("t")
        f.inject("byzantine", mode="collude")
        self.assertEqual(f.corrupt("x"), f.corrupt("y"))                # colluding nodes tell the same lie
        f.inject("byzantine", mode="corrupt")
        self.assertNotEqual(f.corrupt("x"), f.corrupt("x"))             # independent lies differ


class AvailabilityMathTests(unittest.TestCase):
    def make(self, pattern, period=1.0):
        p = AvailabilityProbe(lambda: True, period)
        p.samples = [(i * period, ok) for i, ok in enumerate(pattern)]
        return p

    def test_availability_is_uptime_over_total(self):
        p = self.make([True] * 8 + [False] * 2 + [True])               # outage of 2 s inside a 10 s window
        self.assertAlmostEqual(p.availability(0, 10), 0.8)

    def test_outages_report_mttr_input(self):
        p = self.make([True, False, False, True, True, False, True])
        durations = [round(d) for *_, d in p.outages(0, 6)]
        self.assertEqual(durations, [2, 1])
        self.assertAlmostEqual(sum(durations) / len(durations), 1.5)    # MTTR = total repair time / failures


class ClusterTests(unittest.TestCase):
    """Real processes: the whole detect -> failover -> recover -> failback loop."""

    def setUp(self):
        self.cluster = LocalCluster(base_port=5700, base_metrics=8700, wal="flush")
        self.cluster.start_all()
        self.master = FaultToleranceMaster(self.cluster.urls(), interval=0.5, threshold=3, timeout=0.4,
                                           restarter=self.cluster.restarter).start()
        self.addCleanup(self.cluster.cleanup)
        self.addCleanup(self.master.stop)

    def wait_for(self, cond, timeout):
        end = time.time() + timeout
        while time.time() < end:
            if cond():
                return True
            time.sleep(0.1)
        return False

    def test_node_failure_is_detected_failed_over_recovered_without_losing_slices(self):
        load = LoadGen(self.master, rate=10).start()
        time.sleep(2)
        t_inj = FaultInjector(self.cluster.urls(), cluster=self.cluster).inject(SCENARIOS["node_failure"])
        # detection: threshold (3) x interval (0.5 s) plus one probe of slack
        self.assertTrue(self.wait_for(lambda: self.master.nodes["node1"].state == DEAD, 4.0))
        self.assertLess(self.master.incidents[0]["t_detected"] - t_inj, 3.0)
        self.assertTrue(self.wait_for(lambda: self.master.incidents[0].get("t_failover_done"), 3.0))
        self.assertTrue(self.master.read_status(retry=False)["ok"])     # service continues on the backup
        self.assertTrue(self.wait_for(lambda: self.master.nodes["node1"].state == UP, 20.0))
        load.stop()
        self.assertEqual(set(load.accepted) - held_pids(self.cluster), set())   # nothing lost
        self.assertIsNotNone(self.master.incidents[0]["t_recovered"])

    def test_service_failure_is_repaired_by_service_restart_not_node_restart(self):
        FaultInjector(self.cluster.urls(), cluster=self.cluster).inject(SCENARIOS["service"])
        self.assertTrue(self.wait_for(lambda: self.master.incidents and self.master.incidents[0].get("t_recovered"), 15.0))
        actions = [a["action"] for a in self.master.incidents[0]["recovery_actions"]]
        self.assertEqual(actions[0], "restart_service")
        self.assertTrue(self.cluster.alive("node1"))

    def test_duplicate_executions_dropped_after_failback(self):
        """A node that was only partitioned from the master keeps its slices; failover copies them to the backup;
        resync must remove the duplicates so no slice runs twice."""
        for i in range(10):
            requests.post(f"{self.cluster.url('node1')}/api/submit", timeout=2,
                          json={"pid": f"d{i}", "name": "s", "cpu_needed": 1, "memory_needed": 1, "remaining_burst": 100000})
        time.sleep(0.6)                                                  # let async replication reach node2
        FaultInjector(self.cluster.urls(), cluster=self.cluster).inject(SCENARIOS["service"])
        self.assertTrue(self.wait_for(lambda: self.master.incidents and self.master.incidents[0].get("t_recovered"), 15.0))
        copies = 0
        for name in self.cluster.names:
            active = requests.get(f"{self.cluster.url(name)}/api/pids", timeout=2).json()["active"]
            copies += sum(1 for pid in active if pid.startswith("d"))
        self.assertEqual(copies, 10)


class DurabilityTests(unittest.TestCase):
    def test_crash_loses_nothing_with_wal(self):
        cluster = LocalCluster(base_port=5750, base_metrics=8750, wal="flush", replication="none")
        self.addCleanup(cluster.cleanup)
        cluster.start_all()
        url = cluster.url("node1")
        for i in range(50):
            requests.post(f"{url}/api/submit", timeout=2,
                          json={"pid": f"c{i}", "name": "s", "cpu_needed": 1, "memory_needed": 1, "remaining_burst": 100000})
        cluster.kill("node1")                                            # immediately: no checkpoint, no replica
        cluster.start("node1")
        time.sleep(1.0)
        active = requests.get(f"{url}/api/pids", timeout=2).json()["active"]
        self.assertEqual(len([p for p in active if p.startswith("c")]), 50)


class ByzantineTests(unittest.TestCase):
    def test_quorum_masks_one_lying_node_of_four_but_not_of_two(self):
        cluster = LocalCluster(names=("n1", "n2", "n3", "n4"), base_port=5780, base_metrics=8780)
        self.addCleanup(cluster.cleanup)
        cluster.start_all()
        for n in cluster.names:
            requests.put(f"{cluster.url(n)}/api/register/k", json={"value": "truth"}, timeout=2)
        ChaosClient().inject(cluster.url("n1"), "byzantine", mode="corrupt")
        value, deviants = quorum_read(cluster.urls(), "k", 4)
        self.assertEqual(value, '"truth"')
        self.assertEqual(deviants, ["n1"])
        two = {n: cluster.url(n) for n in ("n1", "n2")}                  # N=2 cannot out-vote one liar
        self.assertIsNone(quorum_read(two, "k", 2)[0])


if __name__ == "__main__":
    unittest.main()
