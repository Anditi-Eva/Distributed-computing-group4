"""Worker-side fault injection (Milestone 7).

A FaultInjector holds the set of faults currently active on one worker node.
worker_node.py consults it from a Flask ``before_request`` hook (request-level
faults) and from its outbound peer calls (link faults).

Supported fault types
---------------------
crash      Process exits immediately (fail-stop). State in memory is lost.
service    Process stays up but the service is broken: /health returns 503 and
           the /api endpoints return 500 (software/service failure).
omission   A fraction ``probability`` of incoming requests is silently dropped
           (no response is produced until the caller times out).
delay      Every incoming request is delayed by ``delay_ms`` (+ optional jitter)
           - a slow or congested communication path.
partition  The link to the peer node is cut: outbound peer calls fail and inbound
           peer traffic is dropped. Master <-> node traffic still works.
byzantine  The node stays "healthy" but lies. ``mode`` is one of
             corrupt    - random wrong answers on reads, different every time
             collude    - one fixed wrong answer (colluding Byzantine nodes agree)
             lying_ack  - acknowledges replication messages but discards them

Every fault can be given a ``duration`` (seconds); it then expires by itself
(a transient fault). Without a duration it stays until cleared or until a
recovery action (restart) removes it.
"""
import os
import random
import threading
import time

from prometheus_client import Counter, Gauge

FAULT_TYPES = ("crash", "service", "omission", "delay", "partition", "byzantine")
BYZANTINE_MODES = ("corrupt", "collude", "lying_ack")
COLLUDING_LIE = "BYZANTINE-LIE"

FAULTS_INJECTED = Counter("node_faults_injected_total", "Faults injected on this node", ["type"])
FAULT_ACTIVE = Gauge("node_fault_active", "1 while a fault of this type is active", ["type"])
REQUESTS_DROPPED = Counter("node_requests_dropped_total", "Requests dropped by omission/partition faults")


class FaultInjector:
    def __init__(self, node_id):
        self.node_id = node_id
        self._faults = {}              # type -> {"params": {...}, "start": t, "expires": t|None}
        self._lock = threading.Lock()
        self.history = []              # every injection/clear, for the experiment log
        for t in FAULT_TYPES:
            FAULT_ACTIVE.labels(type=t).set(0)

    # ------------------------------------------------------------------ control
    def inject(self, fault_type, **params):
        fault_type = str(fault_type).lower()
        if fault_type not in FAULT_TYPES:
            raise ValueError(f"unknown fault type '{fault_type}', expected one of {FAULT_TYPES}")
        params = {k: v for k, v in params.items() if v is not None}
        if fault_type == "omission":
            p = float(params.setdefault("probability", 0.5))
            if not 0.0 <= p <= 1.0:
                raise ValueError("probability must be between 0 and 1")
            params.setdefault("hold_s", 5.0)
        elif fault_type == "delay":
            params["delay_ms"] = float(params.get("delay_ms", 1500))
            params["jitter_ms"] = float(params.get("jitter_ms", 0))
        elif fault_type == "byzantine":
            params["mode"] = str(params.get("mode", "corrupt"))
            if params["mode"] not in BYZANTINE_MODES:
                raise ValueError(f"byzantine mode must be one of {BYZANTINE_MODES}")
        duration = params.pop("duration", None)
        now = time.time()
        expires = now + float(duration) if duration else None
        with self._lock:
            self._faults[fault_type] = {"params": params, "start": now, "expires": expires}
            self.history.append({"t": now, "event": "inject", "type": fault_type, "params": params,
                                 "duration": duration})
        FAULTS_INJECTED.labels(type=fault_type).inc()
        FAULT_ACTIVE.labels(type=fault_type).set(1)
        return {"type": fault_type, "params": params, "duration": duration, "injected_at": now}

    def clear(self, fault_type=None):
        """Remove one fault type, or all of them when fault_type is None."""
        with self._lock:
            kinds = [fault_type] if fault_type else list(self._faults)
            cleared = []
            for k in kinds:
                if self._faults.pop(k, None) is not None:
                    cleared.append(k)
                    FAULT_ACTIVE.labels(type=k).set(0)
                    self.history.append({"t": time.time(), "event": "clear", "type": k})
        return cleared

    def clear_service_level(self):
        """What a service restart repairs: stuck/overloaded/broken service, not the network or lying hardware."""
        return [k for k in ("service", "omission", "delay") if self.clear(k)]

    # ------------------------------------------------------------------ queries
    def get(self, fault_type):
        """Return the active params of a fault, or None. Expired faults are removed lazily."""
        with self._lock:
            f = self._faults.get(fault_type)
            if f is None:
                return None
            if f["expires"] is not None and time.time() >= f["expires"]:
                del self._faults[fault_type]
                FAULT_ACTIVE.labels(type=fault_type).set(0)
                self.history.append({"t": time.time(), "event": "expire", "type": fault_type})
                return None
            return dict(f["params"])

    def active(self):
        out = {}
        for t in FAULT_TYPES:
            params = self.get(t)
            if params is not None:
                f = self._faults.get(t)
                if f:
                    out[t] = {"params": params, "since": f["start"], "expires": f["expires"]}
        return out

    # ------------------------------------------------------------------ effects
    def request_gate(self, path, from_peer):
        """Apply request-level faults. Returns None to continue normally, or a (body, status) reply.

        /chaos/* and /admin/* are the control plane and are never faulted, so an experiment can
        always clear what it injected.
        """
        if path.startswith("/chaos") or path.startswith("/admin"):
            return None

        delay = self.get("delay")
        if delay is not None:
            ms = delay["delay_ms"] + random.uniform(0, delay.get("jitter_ms", 0))
            time.sleep(ms / 1000.0)

        if from_peer and self.get("partition") is not None:
            REQUESTS_DROPPED.inc()
            time.sleep(2.0)                       # packets vanish; the sender times out
            return ("", 504)

        om = self.get("omission")
        if om is not None and random.random() < om["probability"]:
            REQUESTS_DROPPED.inc()
            time.sleep(om["hold_s"])              # no reply is produced; the caller times out first
            return ("", 504)

        if self.get("service") is not None:
            if path == "/health":
                return ({"status": "SERVICE_FAILED", "node_id": self.node_id}, 503)
            if path.startswith("/api"):
                return ({"status": "error", "message": "service failure (injected)"}, 500)
        return None

    def link_blocked(self):
        """True while the node-to-node link is partitioned (outbound side)."""
        return self.get("partition") is not None

    def byzantine_mode(self):
        b = self.get("byzantine")
        return b["mode"] if b else None

    def corrupt(self, value):
        """Return what a Byzantine node reports in place of ``value``."""
        mode = self.byzantine_mode()
        if mode == "collude":
            return COLLUDING_LIE
        if mode == "corrupt":
            return f"garbage-{random.randint(0, 10**6)}"
        return value

    @staticmethod
    def crash_now(delay_s=0.05):
        """Fail-stop: leave without cleanup, as a power cut or kill -9 would."""
        threading.Timer(delay_s, lambda: os._exit(137)).start()
