#!/usr/bin/env python3
"""
Milestone 8 - Distributed System Models: simulation and decision analysis.

Compares five architectures for the 5G network-slicing platform built in
Milestones 1-7, using the real workload in slices.csv (URLLC / eMBB / mMTC):

    1. Host-based       one central host does all processing
    2. Processor pool   a pool of identical processors behind a dispatcher
    3. Workstation      each site processes only its own users (shared-nothing)
    4. Server-based     dedicated edge / core servers by slice class (Milestone 3)
    5. Integrated       edge + core cooperating: load-aware placement, failover,
                        migration (Milestones 1, 3, 4 combined)

Every architecture gets the same 12 cores in total, the same request stream
(same seed) and the same SLA, so differences come from the architecture only.

Experiments
    E1  baseline at 70 % offered load      -> latency, throughput, utilisation
    E2  load sweep 30 % ... 110 %          -> saturation behaviour
    E3  scale-out k = 1, 2, 4, 8           -> scalability efficiency
    E4  failure injection                  -> reliability / blast radius
    E5  analytic availability (all states) -> reliability, 99.9 % components
    E6  cost model                         -> monthly cost, cost per SLA-met request
    E7  weighted decision matrix + Monte-Carlo weight sensitivity

All outputs are written to ../results/milestone8/ (CSV, JSON, PNG, Markdown).
All numbers are SIMULATED. Every assumption is a named constant below so the
group can change it and re-run:  python model_simulation.py
"""
import argparse
import csv
import itertools
import json
import math
import os
import random
import statistics
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))

# ----------------------------------------------------------------------------
# MODEL ASSUMPTIONS (edit here, then re-run)
# ----------------------------------------------------------------------------
ARCHS = ["Host-based", "Processor pool", "Workstation", "Server-based", "Integrated"]
CLASSES = ["URLLC", "eMBB", "mMTC"]

# Mean single-core processing time per request (ms), per slice class.
BASE_SVC_MS = {"URLLC": 0.40, "eMBB": 4.00, "mMTC": 0.80}
# Used when slices.csv has a blank max_latency_ms (row 844 does).
DEFAULT_SLA_MS = {"URLLC": 1.0, "eMBB": 50.0, "mMTC": 100.0}
TIMEOUT_MS = 100.0          # admission control: reject if predicted queue wait > this

CORES_EDGE, CORES_CORE = 4, 8          # the Milestone 3 edge / core VMs
TOTAL_CORES = CORES_EDGE + CORES_CORE  # 12 in every architecture
WS_SITES, WS_SHARES = 3, [0.40, 0.35, 0.25]   # workstation sites, uneven user population
POOL_PROCESSORS = 6                    # pool = 6 processors x 2 cores behind one dispatcher
LANE_CORES = 1   # every site reserves this many cores for URLLC (slice isolation, same rule everywhere)

# One-way network latency from the user to the processing site (ms).
NET_EDGE_MS, NET_CENTRAL_MS = 0.10, 2.50
# Extra per-request control overhead (ms): dispatcher / gateway / orchestrator state lookup.
OVERHEAD_MS = {"Host-based": 0.0, "Processor pool": 0.10, "Workstation": 0.0,
               "Server-based": 0.10, "Integrated": 0.05}

# Universal Scalability Law contention coefficient sigma per architecture.
# Service-time multiplier on a station with c cores = 1 + sigma * (c - 1).
SIGMA = {"Host-based": 0.015,      # one shared-memory host: lock / cache contention
         "Processor pool": 0.004,  # independent workers, central queue only
         "Workstation": 0.010, "Server-based": 0.010, "Integrated": 0.010}

# Reliability
COMPONENT_AVAILABILITY = 0.999      # MTBF 1000 h, MTTR 1 h for every component
INTEGRATED_CONTROL_REDUNDANT = True # peers coordinate; no single control-plane node
FAIL_START, FAIL_END = 0.40, 0.60   # failure window as a fraction of the run

# Cost (USD / month) - illustrative, replace with real quotes
COST = {"core_dc": 25, "core_edge": 45, "site_edge": 250, "site_dc": 150,
        "dispatcher": 60, "gateway": 60, "orchestrator": 120}

# Decision weights (sum to 1) for several stakeholder profiles
WEIGHT_PROFILES = {
    "Balanced telecom":  dict(latency=.25, throughput=.15, utilization=.10, scalability=.15, reliability=.25, cost=.10),
    "URLLC-critical":    dict(latency=.40, throughput=.05, utilization=.05, scalability=.10, reliability=.30, cost=.10),
    "Cost-driven":       dict(latency=.15, throughput=.15, utilization=.15, scalability=.10, reliability=.10, cost=.35),
}
CRITERIA = ["latency", "throughput", "utilization", "scalability", "reliability", "cost"]

COLORS = {"Host-based": "#8c564b", "Processor pool": "#1f77b4", "Workstation": "#ff7f0e",
          "Server-based": "#2ca02c", "Integrated": "#d62728"}


# ----------------------------------------------------------------------------
# Workload
# ----------------------------------------------------------------------------
def load_slices(path):
    rows = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            cls = r["slice_type"].strip()
            if cls not in CLASSES:
                continue
            bw = float(r["required_bandwidth_mbps"] or 0)
            sla = float(r["max_latency_ms"]) if r["max_latency_ms"].strip() else DEFAULT_SLA_MS[cls]
            rows.append({"cls": cls, "bw": bw, "sla": sla})
    mean_bw = {c: statistics.mean(r["bw"] for r in rows if r["cls"] == c) for c in CLASSES}
    for r in rows:
        # demand grows with the slice's bandwidth relative to its class mean
        r["factor"] = 0.5 + 0.5 * r["bw"] / mean_bw[r["cls"]]
    return rows


def mean_service_ms(rows):
    return statistics.mean(BASE_SVC_MS[r["cls"]] * r["factor"] for r in rows)


def make_requests(rows, n, rho, capacity_cores, seed):
    """Poisson arrivals; slices bootstrapped from the dataset; service ~ Gamma(k=2)."""
    rng = random.Random(seed)
    lam = rho * capacity_cores / mean_service_ms(rows)      # requests per ms
    t, reqs = 0.0, []
    for _ in range(n):
        t += rng.expovariate(lam)
        row = rows[rng.randrange(len(rows))]
        base = BASE_SVC_MS[row["cls"]] * row["factor"]
        svc = rng.gammavariate(2.0, base / 2.0)
        reqs.append((t, row["cls"], row["sla"], svc, rng.random()))
    return reqs


# ----------------------------------------------------------------------------
# Station = a multi-core FCFS processing site
# ----------------------------------------------------------------------------
class Station:
    def __init__(self, name, cores, sigma, net_ms, ovh_ms):
        self.name, self.cores = name, cores
        self.lane = LANE_CORES if cores >= 2 else 0     # servers [0:lane] are URLLC-only
        self.free = [0.0] * cores
        self.mult = 1.0 + sigma * (cores - 1)
        self.net, self.ovh = net_ms, ovh_ms
        self.busy = 0.0
        self.work = 0.0
        self.out = None            # (start, end, servers_down)
        self._flushed = False

    def set_outage(self, f0, f1, k):
        self.out = (f0, f1, min(k, self.cores))

    def _whole_down(self, t):
        return self.out is not None and self.out[2] >= self.cores and self.out[0] <= t < self.out[1]

    def _flush_after_outage(self, t):
        # a full-site outage loses every queued job; the site restarts empty
        if self.out and self.out[2] >= self.cores and t >= self.out[1] and not self._flushed:
            self.free = [self.out[1]] * self.cores
            self._flushed = True

    def _idx(self, cls):
        if self.lane == 0:
            return range(self.cores)
        return range(self.lane) if cls == "URLLC" else range(self.lane, self.cores)

    def _start(self, i, t, svc):
        s = max(t, self.free[i])
        if self.out:
            f0, f1, k = self.out
            if i >= self.cores - k and s + svc > f0 and s < f1:   # last k servers fail
                if k >= self.cores:
                    return None          # job would be lost in a full outage
                s = f1                   # partial outage: job is re-queued after repair
        return s

    def est_wait(self, t, svc, cls):
        self._flush_after_outage(t)
        if self._whole_down(t):
            return math.inf
        best = math.inf
        for i in self._idx(cls):
            s = self._start(i, t, svc * self.mult)
            if s is not None and s - t < best:
                best = s - t
        return best

    def completion_estimate(self, t, svc, cls):
        w = self.est_wait(t, svc, cls)
        return w + svc * self.mult + 2 * self.net + self.ovh

    def assign(self, t, svc, cls):
        """Returns (status, latency_ms). status in ok | down | timeout."""
        self._flush_after_outage(t)
        if self._whole_down(t):
            return "down", 0.0
        run = svc * self.mult
        best_i, best_s = -1, math.inf
        for i in self._idx(cls):
            s = self._start(i, t, run)
            if s is not None and s < best_s:
                best_i, best_s = i, s
        if best_i < 0:
            return "down", 0.0
        if best_s - t > TIMEOUT_MS:
            return "timeout", 0.0
        self.free[best_i] = best_s + run
        self.busy += run
        self.work += svc
        return "ok", (best_s + run - t) + 2 * self.net + self.ovh


# ----------------------------------------------------------------------------
# Architectures
# ----------------------------------------------------------------------------
def build(arch, k=1):
    """Return (stations, choose(cls, t, svc, u) -> Station, failure_target)."""
    ovh, sg = OVERHEAD_MS[arch], SIGMA[arch]
    if arch == "Host-based":
        st = [Station("host", TOTAL_CORES * k, sg, NET_CENTRAL_MS, ovh)]
        return st, (lambda c, t, s, u: st[0]), (st[0], st[0].cores)
    if arch == "Processor pool":
        st = [Station("pool", TOTAL_CORES * k, sg, NET_CENTRAL_MS, ovh)]
        return st, (lambda c, t, s, u: st[0]), (st[0], TOTAL_CORES * k // POOL_PROCESSORS)  # lose one processor
    if arch == "Workstation":
        st = [Station(f"ws{i + 1}", CORES_EDGE, sg, NET_EDGE_MS, ovh) for i in range(WS_SITES * k)]
        w = [x / k for x in WS_SHARES * k]
        cum = list(itertools.accumulate(w))

        def choose(c, t, s, u):
            for i, cv in enumerate(cum):
                if u <= cv / cum[-1]:
                    return st[i]
            return st[-1]
        return st, choose, (st[0], st[0].cores)
    edges = [Station(f"edge{i + 1}", CORES_EDGE, sg, NET_EDGE_MS, ovh) for i in range(k)]
    cores = [Station(f"core{i + 1}", CORES_CORE, sg, NET_CENTRAL_MS, ovh) for i in range(k)]
    st = edges + cores
    if arch == "Server-based":
        def choose(c, t, s, u):
            grp = edges if c == "mMTC" else cores        # Milestone 3 routing rule
            return min(grp, key=lambda x: x.est_wait(t, s, c))
        return st, choose, (cores[0], cores[0].cores)
    if arch == "Integrated":
        def choose(c, t, s, u):                          # load-aware placement + failover
            return min(st, key=lambda x: x.completion_estimate(t, s, c))
        return st, choose, (cores[0], cores[0].cores)
    raise ValueError(arch)


def simulate(arch, reqs, k=1, inject_failure=False):
    stations, choose, (ftarget, fk) = build(arch, k)
    horizon = reqs[-1][0]
    f0, f1 = FAIL_START * horizon, FAIL_END * horizon
    if inject_failure:
        ftarget.set_outage(f0, f1, fk)
    lat = defaultdict(list)
    off, met, done = defaultdict(int), defaultdict(int), defaultdict(int)
    drop = {"down": 0, "timeout": 0}
    win_off = win_done = 0
    for (t, cls, sla, svc, u) in reqs:
        off[cls] += 1
        in_win = f0 <= t < f1
        win_off += in_win
        stn = choose(cls, t, svc, u)
        status, l = stn.assign(t, svc, cls)
        if status != "ok":
            drop[status] += 1
            continue
        done[cls] += 1
        win_done += in_win
        lat[cls].append(l)
        if l <= sla:
            met[cls] += 1
    return {"lat": lat, "off": off, "met": met, "done": done, "drop": drop, "T": horizon,
            "win_off": win_off, "win_done": win_done,
            "stations": [(s.name, s.cores, s.busy, s.work) for s in stations]}


def pct(xs, p):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(math.ceil(p / 100.0 * len(xs))) - 1)]


def summarise(runs):
    """Merge several seeds of the same scenario into one metrics dict."""
    lat, off, met, done = defaultdict(list), defaultdict(int), defaultdict(int), defaultdict(int)
    drop_dn = drop_to = 0
    T = 0.0
    util = defaultdict(lambda: [0.0, 0.0, 0.0])      # name -> [busy, work, core_ms]
    win_off = win_done = 0
    for r in runs:
        for c in CLASSES:
            lat[c] += r["lat"][c]
            off[c] += r["off"][c]
            met[c] += r["met"][c]
            done[c] += r["done"][c]
        drop_dn += r["drop"]["down"]
        drop_to += r["drop"]["timeout"]
        T += r["T"]
        win_off += r["win_off"]
        win_done += r["win_done"]
        for name, cores, busy, work in r["stations"]:
            u = util[name]
            u[0] += busy
            u[1] += work
            u[2] += cores * r["T"]
    all_lat = [x for c in CLASSES for x in lat[c]]
    n_off, n_done, n_met = sum(off.values()), sum(done.values()), sum(met.values())
    m = {
        "offered": n_off, "completed": n_done,
        "throughput_rps": n_done / (T / 1000.0),
        "sla_goodput_rps": n_met / (T / 1000.0),
        "served_pct": 100.0 * n_done / n_off,
        "sla_pct": 100.0 * n_met / n_off,
        "dropped_down_pct": 100.0 * drop_dn / n_off,
        "dropped_timeout_pct": 100.0 * drop_to / n_off,
        "p50_ms": pct(all_lat, 50), "p95_ms": pct(all_lat, 95), "p99_ms": pct(all_lat, 99),
        "mean_ms": statistics.mean(all_lat) if all_lat else float("nan"),
        "win_served_pct": 100.0 * win_done / win_off if win_off else float("nan"),
    }
    for c in CLASSES:
        m[f"{c}_p95_ms"] = pct(lat[c], 95)
        m[f"{c}_sla_pct"] = 100.0 * met[c] / off[c]
    m["util_by_station"] = {n: min(1.0, v[0] / v[2]) for n, v in util.items()}
    useful = {n: min(1.0, v[1] / v[2]) for n, v in util.items()}
    tot_core_ms = sum(v[2] for v in util.values())
    m["mean_util"] = sum(v[0] for v in util.values()) / tot_core_ms
    m["useful_util"] = sum(v[1] for v in util.values()) / tot_core_ms
    uv = list(m["util_by_station"].values())
    m["imbalance"] = (max(uv) - min(uv)) / max(uv) if len(uv) > 1 and max(uv) > 0 else 0.0
    m["useful_by_station"] = useful
    return m


def run_scenario(arch, rows, n, rho, k, seeds, inject_failure=False):
    runs = []
    for s in seeds:
        reqs = make_requests(rows, n, rho, TOTAL_CORES * k, s)
        runs.append(simulate(arch, reqs, k, inject_failure))
    return summarise(runs)


# ----------------------------------------------------------------------------
# Analytic availability (exact enumeration of component up/down states)
# ----------------------------------------------------------------------------
def analytic_availability(a=COMPONENT_AVAILABILITY, rho=0.7):
    demand = rho * TOTAL_CORES

    def host(u):
        return 1.0 if u["host"] else 0.0

    def pool(u):
        up = sum(u[f"p{i}"] for i in range(POOL_PROCESSORS))
        cores_up = up * TOTAL_CORES / POOL_PROCESSORS
        return (1.0 if u["dispatcher"] else 0.0) * min(1.0, cores_up / demand)

    def ws(u):
        return sum(sh * u[f"ws{i}"] for i, sh in enumerate(WS_SHARES))

    def server(u):
        return (1.0 if u["gateway"] else 0.0) * (u["edge"] / 3.0 + 2.0 * u["core"] / 3.0)

    def integrated(u):
        ctl = 1.0 if (INTEGRATED_CONTROL_REDUNDANT or u["orchestrator"]) else 0.0
        return ctl * min(1.0, (CORES_EDGE * u["edge"] + CORES_CORE * u["core"]) / demand)

    spec = {
        "Host-based": (["host"], host),
        "Processor pool": (["dispatcher"] + [f"p{i}" for i in range(POOL_PROCESSORS)], pool),
        "Workstation": ([f"ws{i}" for i in range(WS_SITES)], ws),
        "Server-based": (["gateway", "edge", "core"], server),
        "Integrated": (["edge", "core", "orchestrator"], integrated),
    }
    out = {}
    for arch, (comps, fn) in spec.items():
        exp_served = p_total = p_degraded = 0.0
        for state in itertools.product([0, 1], repeat=len(comps)):
            pr = 1.0
            for s in state:
                pr *= a if s else (1 - a)
            served = fn(dict(zip(comps, state)))
            exp_served += pr * served
            p_total += pr * (served <= 1e-12)
            p_degraded += pr * (served < 1 - 1e-9)
        out[arch] = {"expected_served": exp_served, "expected_unserved": 1 - exp_served,
                     "p_total_outage": p_total, "p_degraded": p_degraded,
                     "unserved_min_per_year": (1 - exp_served) * 525600}
    return out


# ----------------------------------------------------------------------------
# Cost
# ----------------------------------------------------------------------------
def monthly_cost(arch):
    c = COST
    if arch == "Host-based":
        return TOTAL_CORES * c["core_dc"] + c["site_dc"]
    if arch == "Processor pool":
        return TOTAL_CORES * c["core_dc"] + c["site_dc"] + c["dispatcher"]
    if arch == "Workstation":
        return TOTAL_CORES * c["core_edge"] + WS_SITES * c["site_edge"]
    base = CORES_EDGE * c["core_edge"] + CORES_CORE * c["core_dc"] + c["site_edge"] + c["site_dc"]
    return base + (c["gateway"] if arch == "Server-based" else c["orchestrator"])


# ----------------------------------------------------------------------------
# Decision matrix
# ----------------------------------------------------------------------------
def minmax(vals, higher_better=True):
    lo, hi = min(vals.values()), max(vals.values())
    if hi - lo < 1e-12:
        return {k: 1.0 for k in vals}
    return {k: ((v - lo) / (hi - lo) if higher_better else (hi - v) / (hi - lo)) for k, v in vals.items()}


def build_scores(base, sweep, scal, rel_sim, rel_an, cost_monthly):
    # latency: mean over classes of p95 / SLA (lower is better)
    lat = {}
    for a in ARCHS:
        lat[a] = statistics.mean(base[a][f"{c}_p95_ms"] / DEFAULT_SLA_MS[c] for c in CLASSES)
    # throughput: peak SLA-met goodput anywhere on the load sweep
    thr = {a: max(v["sla_goodput_rps"] for v in sweep[a].values()) for a in ARCHS}
    # utilisation: useful utilisation, penalised by imbalance between stations
    uti = {a: base[a]["useful_util"] * (1 - base[a]["imbalance"]) for a in ARCHS}
    # scalability: mean scaling efficiency for k = 2, 4, 8
    # efficiency is capped at 100 %: gains above 1.0 are statistical-multiplexing effects, not extra scalability
    sca = {a: statistics.mean(min(1.0, scal[a][k]["efficiency"]) for k in scal[a] if k > 1) for a in ARCHS}
    # reliability: failure-window served % and analytic expected unserved fraction
    r1 = minmax({a: rel_sim[a]["win_served_pct"] for a in ARCHS})
    r2 = minmax({a: -math.log10(max(rel_an[a]["expected_unserved"], 1e-12)) for a in ARCHS})
    rel = {a: 0.5 * r1[a] + 0.5 * r2[a] for a in ARCHS}
    s = {
        "latency": minmax(lat, False), "throughput": minmax(thr), "utilization": minmax(uti),
        "scalability": minmax(sca), "reliability": rel, "cost": minmax(cost_monthly, False),
    }
    raw = {"latency": lat, "throughput": thr, "utilization": uti, "scalability": sca,
           "reliability": {a: rel_sim[a]["win_served_pct"] for a in ARCHS}, "cost": cost_monthly}
    return s, raw


def weighted(scores, w):
    return {a: sum(w[c] * scores[c][a] for c in CRITERIA) for a in ARCHS}


def monte_carlo_wins(scores, draws=5000, seed=1):
    rng = random.Random(seed)
    wins = {a: 0 for a in ARCHS}
    for _ in range(draws):
        g = [rng.gammavariate(1.0, 1.0) for _ in CRITERIA]
        tot = sum(g)
        w = {c: x / tot for c, x in zip(CRITERIA, g)}
        tot_scores = weighted(scores, w)
        wins[max(tot_scores, key=tot_scores.get)] += 1
    return {a: 100.0 * v / draws for a, v in wins.items()}


# ----------------------------------------------------------------------------
# Output helpers
# ----------------------------------------------------------------------------
def write_csv(path, header, rows):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def fmt(x, d=2):
    return "n/a" if x is None or (isinstance(x, float) and math.isnan(x)) else f"{x:.{d}f}"


def md_table(header, rows):
    out = ["| " + " | ".join(header) + " |", "|" + "|".join(["---"] * len(header)) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def plot_all(out, base, sweep, scal, rel_sim, rel_an, cost, cost_per_m, scores, totals, mc):
    plt.rcParams.update({"font.size": 9, "axes.grid": True, "grid.alpha": 0.3})

    # 1 latency per class
    fig, ax = plt.subplots(figsize=(8, 4))
    w = 0.16
    for i, a in enumerate(ARCHS):
        ax.bar([j + (i - 2) * w for j in range(3)], [base[a][f"{c}_p95_ms"] for c in CLASSES],
               w, label=a, color=COLORS[a])
    for j, c in enumerate(CLASSES):
        ax.hlines(DEFAULT_SLA_MS[c], j - 0.45, j + 0.45, colors="k", linestyles="--", linewidth=1)
    ax.set_yscale("log"); ax.set_xticks(range(3)); ax.set_xticklabels(CLASSES)
    ax.set_ylabel("p95 latency (ms, log scale)")
    ax.set_title("Latency by slice class at 70% load (dashed = SLA)")
    ax.legend(fontsize=7, ncol=2)
    fig.tight_layout(); fig.savefig(os.path.join(out, "fig1_latency_by_class.png"), dpi=140); plt.close(fig)

    # 2 load sweep
    fig, axs = plt.subplots(1, 3, figsize=(13, 3.8))
    for a in ARCHS:
        rhos = sorted(sweep[a])
        axs[0].plot(rhos, [sweep[a][r]["p95_ms"] for r in rhos], marker="o", label=a, color=COLORS[a])
        axs[1].plot(rhos, [sweep[a][r]["throughput_rps"] for r in rhos], marker="o", label=a, color=COLORS[a])
        axs[2].plot(rhos, [sweep[a][r]["sla_pct"] for r in rhos], marker="o", label=a, color=COLORS[a])
    axs[0].set_yscale("log"); axs[0].set_title("p95 latency vs offered load"); axs[0].set_ylabel("ms")
    axs[1].set_title("Throughput (completed req/s)"); axs[2].set_title("Requests meeting SLA (%)")
    for ax in axs:
        ax.set_xlabel("offered load (fraction of 12-core capacity)")
    axs[0].legend(fontsize=7)
    fig.tight_layout(); fig.savefig(os.path.join(out, "fig2_load_sweep.png"), dpi=140); plt.close(fig)

    # 3 utilisation per station
    fig, ax = plt.subplots(figsize=(8, 4))
    x = 0
    ticks, labels = [], []
    for a in ARCHS:
        for name, u in base[a]["util_by_station"].items():
            ax.bar(x, 100 * u, color=COLORS[a]); ticks.append(x); labels.append(name); x += 1
        x += 1
    ax.set_xticks(ticks); ax.set_xticklabels(labels, rotation=60, fontsize=7)
    ax.set_ylabel("busy time (%)"); ax.set_title("Per-station utilisation at 70% load (colour = architecture)")
    ax.legend(handles=[plt.Rectangle((0, 0), 1, 1, color=COLORS[a]) for a in ARCHS], labels=ARCHS, fontsize=7)
    fig.tight_layout(); fig.savefig(os.path.join(out, "fig3_utilization.png"), dpi=140); plt.close(fig)

    # 4 scalability
    fig, axs = plt.subplots(1, 2, figsize=(10, 3.8))
    for a in ARCHS:
        ks = sorted(scal[a])
        axs[0].plot(ks, [scal[a][k]["sla_goodput_rps"] for k in ks], marker="o", label=a, color=COLORS[a])
        axs[1].plot(ks, [100 * scal[a][k]["efficiency"] for k in ks], marker="o", label=a, color=COLORS[a])
    axs[0].plot(sorted(scal[ARCHS[0]]), [scal["Integrated"][1]["sla_goodput_rps"] * k for k in sorted(scal[ARCHS[0]])],
                "k--", label="ideal linear")
    axs[0].set_title("SLA-met goodput (req/s) when capacity is scaled k x"); axs[0].set_xlabel("k"); axs[0].legend(fontsize=7)
    axs[1].set_title("Scaling efficiency (%)"); axs[1].set_xlabel("k")
    for ax in axs:
        ax.set_xscale("log", base=2); ax.set_xticks([1, 2, 4, 8]); ax.set_xticklabels([1, 2, 4, 8])
    fig.tight_layout(); fig.savefig(os.path.join(out, "fig4_scalability.png"), dpi=140); plt.close(fig)

    # 5 reliability
    fig, axs = plt.subplots(1, 2, figsize=(10, 3.8))
    axs[0].bar(ARCHS, [rel_sim[a]["win_served_pct"] for a in ARCHS], color=[COLORS[a] for a in ARCHS])
    axs[0].set_title("Requests served DURING the injected failure (%)"); axs[0].tick_params(axis="x", rotation=30)
    axs[1].bar(ARCHS, [rel_an[a]["unserved_min_per_year"] for a in ARCHS], color=[COLORS[a] for a in ARCHS])
    axs[1].set_yscale("log")
    axs[1].set_title("Expected unserved time (min/year, 99.9% parts)"); axs[1].tick_params(axis="x", rotation=30)
    fig.tight_layout(); fig.savefig(os.path.join(out, "fig5_reliability.png"), dpi=140); plt.close(fig)

    # 6 cost
    fig, axs = plt.subplots(1, 2, figsize=(10, 3.8))
    axs[0].bar(ARCHS, [cost[a] for a in ARCHS], color=[COLORS[a] for a in ARCHS])
    axs[0].set_title("Monthly cost (USD, illustrative)"); axs[0].tick_params(axis="x", rotation=30)
    axs[1].bar(ARCHS, [cost_per_m[a] for a in ARCHS], color=[COLORS[a] for a in ARCHS])
    axs[1].set_title("Cost per million SLA-met requests (USD)"); axs[1].tick_params(axis="x", rotation=30)
    fig.tight_layout(); fig.savefig(os.path.join(out, "fig6_cost.png"), dpi=140); plt.close(fig)

    # 7 decision
    fig, axs = plt.subplots(1, 2, figsize=(11, 4), gridspec_kw={"width_ratios": [2, 1]})
    w = WEIGHT_PROFILES["Balanced telecom"]
    bottoms = [0.0] * len(ARCHS)
    pal = ["#4c72b0", "#dd8452", "#55a868", "#c44e52", "#8172b3", "#937860"]
    for ci, c in enumerate(CRITERIA):
        vals = [w[c] * scores[c][a] for a in ARCHS]
        axs[0].bar(ARCHS, vals, bottom=bottoms, label=f"{c} ({int(w[c] * 100)}%)", color=pal[ci])
        bottoms = [b + v for b, v in zip(bottoms, vals)]
    axs[0].set_ylabel("weighted score (max 1.0)"); axs[0].set_title("Decision matrix - Balanced telecom weights")
    axs[0].tick_params(axis="x", rotation=20); axs[0].legend(fontsize=7)
    axs[1].bar(ARCHS, [mc[a] for a in ARCHS], color=[COLORS[a] for a in ARCHS])
    axs[1].set_title("Win rate under 5000 random weightings (%)"); axs[1].tick_params(axis="x", rotation=30)
    fig.tight_layout(); fig.savefig(os.path.join(out, "fig7_decision.png"), dpi=140); plt.close(fig)


# ----------------------------------------------------------------------------
# E8 sensitivity: do the conclusions depend on one fragile assumption?
# ----------------------------------------------------------------------------
def sensitivity(rows, n, seeds, rho):
    g = globals()
    scenarios = [
        ("Reference assumptions", {}, rho),
        ("Equal workstation user shares (33/33/33)", {"WS_SHARES": [1 / 3] * 3}, rho),
        ("No URLLC lane (no slice isolation)", {"LANE_CORES": 0}, rho),
        ("2 reserved URLLC cores per site", {"LANE_CORES": 2}, rho),
        ("Central site only 0.5 ms away (metro DC)", {"NET_CENTRAL_MS": 0.5}, rho),
        ("Lighter load (50%)", {}, 0.5),
    ]
    out = []
    for name, patch, r in scenarios:
        saved = {k: g[k] for k in patch}
        g.update(patch)
        try:
            res = {a: run_scenario(a, rows, n, r, 1, seeds) for a in ARCHS}
        finally:
            g.update(saved)
        out.append((name, res))
    return out


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Milestone 8 distributed-system-model simulation")
    ap.add_argument("--slices", default=os.path.join(HERE, "slices.csv"))
    ap.add_argument("--out", default=os.path.join(HERE, "..", "results", "milestone8"))
    ap.add_argument("--requests", type=int, default=30000, help="requests per run")
    ap.add_argument("--seeds", type=int, default=5, help="independent runs per point")
    ap.add_argument("--rho", type=float, default=0.70, help="baseline offered load")
    ap.add_argument("--quick", action="store_true", help="fewer requests/seeds for a smoke test")
    args = ap.parse_args()
    if args.quick:
        args.requests, args.seeds = 6000, 2
    out = os.path.abspath(args.out)
    os.makedirs(out, exist_ok=True)
    rows = load_slices(args.slices)
    seeds = [42 + i for i in range(args.seeds)]
    n = args.requests
    print(f"Workload: {len(rows)} slices from slices.csv | mean demand {mean_service_ms(rows):.2f} ms/req "
          f"| {n} requests x {len(seeds)} seeds per point")

    # E1 baseline
    print("E1 baseline ...")
    base = {a: run_scenario(a, rows, n, args.rho, 1, seeds) for a in ARCHS}
    # E2 load sweep
    print("E2 load sweep ...")
    rhos = [0.3, 0.5, 0.7, 0.85, 0.95, 1.1]
    sweep = {a: {r: run_scenario(a, rows, n, r, 1, seeds[:3]) for r in rhos} for a in ARCHS}
    # E3 scalability (load stays at 70 % of the scaled nominal capacity)
    print("E3 scalability ...")
    scal = {}
    for a in ARCHS:
        scal[a] = {}
        for k in [1, 2, 4, 8]:
            m = run_scenario(a, rows, n, args.rho, k, seeds[:3])
            scal[a][k] = {"throughput_rps": m["throughput_rps"], "sla_goodput_rps": m["sla_goodput_rps"],
                          "p95_ms": m["p95_ms"], "sla_pct": m["sla_pct"]}
        g1 = scal[a][1]["sla_goodput_rps"]
        for k in scal[a]:
            # efficiency = SLA-met goodput relative to perfect linear scaling of the k=1 system
            scal[a][k]["efficiency"] = scal[a][k]["sla_goodput_rps"] / (k * g1)
    # E4 failure injection
    print("E4 failure injection ...")
    rel_sim = {a: run_scenario(a, rows, n, args.rho, 1, seeds, inject_failure=True) for a in ARCHS}
    # E5 analytic availability
    rel_an = analytic_availability(rho=args.rho)
    # E6 cost
    cost = {a: monthly_cost(a) for a in ARCHS}
    secs_month = 30 * 24 * 3600
    cost_per_m = {a: cost[a] / (base[a]["sla_goodput_rps"] * secs_month / 1e6) for a in ARCHS}
    # E8 sensitivity
    print("E8 sensitivity ...")
    sens = sensitivity(rows, n, seeds[:3], args.rho)
    # E7 decision
    scores, raw = build_scores(base, sweep, scal, rel_sim, rel_an, cost)
    totals = {p: weighted(scores, w) for p, w in WEIGHT_PROFILES.items()}
    mc = monte_carlo_wins(scores)

    # ---- CSV / JSON ----
    write_csv(os.path.join(out, "baseline_metrics.csv"),
              ["architecture", "throughput_rps", "sla_goodput_rps", "served_pct", "sla_pct", "mean_ms", "p50_ms",
               "p95_ms", "p99_ms"] + [f"{c}_p95_ms" for c in CLASSES] + [f"{c}_sla_pct" for c in CLASSES]
              + ["dropped_timeout_pct", "mean_util", "useful_util", "imbalance"],
              [[a] + [round(base[a][k], 4) for k in ["throughput_rps", "sla_goodput_rps", "served_pct", "sla_pct",
                                                    "mean_ms", "p50_ms", "p95_ms", "p99_ms"]]
               + [round(base[a][f"{c}_p95_ms"], 4) for c in CLASSES] + [round(base[a][f"{c}_sla_pct"], 2) for c in CLASSES]
               + [round(base[a][k], 4) for k in ["dropped_timeout_pct", "mean_util", "useful_util", "imbalance"]]
               for a in ARCHS])
    write_csv(os.path.join(out, "load_sweep.csv"),
              ["architecture", "offered_load", "throughput_rps", "p95_ms", "sla_pct", "dropped_timeout_pct", "mean_util"],
              [[a, r, round(sweep[a][r]["throughput_rps"], 1), round(sweep[a][r]["p95_ms"], 4),
                round(sweep[a][r]["sla_pct"], 2), round(sweep[a][r]["dropped_timeout_pct"], 2),
                round(sweep[a][r]["mean_util"], 3)] for a in ARCHS for r in rhos])
    write_csv(os.path.join(out, "scalability.csv"),
              ["architecture", "k", "throughput_rps", "sla_goodput_rps", "p95_ms", "sla_pct", "efficiency"],
              [[a, k, round(v["throughput_rps"], 1), round(v["sla_goodput_rps"], 1), round(v["p95_ms"], 4), round(v["sla_pct"], 2), round(v["efficiency"], 4)]
               for a in ARCHS for k, v in scal[a].items()])
    write_csv(os.path.join(out, "reliability.csv"),
              ["architecture", "failure_window_served_pct", "whole_run_served_pct", "whole_run_sla_pct", "p95_ms",
               "expected_unserved", "p_total_outage", "p_degraded", "unserved_min_per_year"],
              [[a, round(rel_sim[a]["win_served_pct"], 2), round(rel_sim[a]["served_pct"], 2),
                round(rel_sim[a]["sla_pct"], 2), round(rel_sim[a]["p95_ms"], 3),
                f"{rel_an[a]['expected_unserved']:.3e}", f"{rel_an[a]['p_total_outage']:.3e}",
                f"{rel_an[a]['p_degraded']:.3e}", round(rel_an[a]["unserved_min_per_year"], 1)] for a in ARCHS])
    write_csv(os.path.join(out, "cost.csv"), ["architecture", "monthly_usd", "usd_per_million_sla_met"],
              [[a, cost[a], round(cost_per_m[a], 4)] for a in ARCHS])
    write_csv(os.path.join(out, "decision_matrix.csv"),
              ["architecture"] + [f"{c}_score" for c in CRITERIA] + [f"total_{p}" for p in WEIGHT_PROFILES] + ["mc_win_pct"],
              [[a] + [round(scores[c][a], 3) for c in CRITERIA] + [round(totals[p][a], 3) for p in WEIGHT_PROFILES]
               + [round(mc[a], 1)] for a in ARCHS])
    write_csv(os.path.join(out, "sensitivity.csv"),
              ["scenario", "architecture", "sla_pct", "URLLC_sla_pct", "p95_ms", "served_pct"],
              [[nm, a, round(res[a]["sla_pct"], 2), round(res[a]["URLLC_sla_pct"], 2), round(res[a]["p95_ms"], 3),
                round(res[a]["served_pct"], 2)] for nm, res in sens for a in ARCHS])
    winner = {p: max(t, key=t.get) for p, t in totals.items()}
    summary = {
        "assumptions": {"base_svc_ms": BASE_SVC_MS, "timeout_ms": TIMEOUT_MS, "cores_edge": CORES_EDGE,
                        "cores_core": CORES_CORE, "net_edge_ms": NET_EDGE_MS, "net_central_ms": NET_CENTRAL_MS,
                        "overhead_ms": OVERHEAD_MS, "sigma": SIGMA, "component_availability": COMPONENT_AVAILABILITY,
                        "ws_shares": WS_SHARES, "cost_usd_month": COST, "weights": WEIGHT_PROFILES,
                        "requests_per_run": n, "seeds": seeds, "baseline_rho": args.rho},
        "raw_criteria": raw, "scores": scores, "totals": totals, "winner_by_profile": winner,
        "monte_carlo_win_pct": mc,
    }
    with open(os.path.join(out, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2, default=str)

    # ---- Markdown report ----
    md = ["# Milestone 8 - simulation results", "",
          f"Generated by `host_pc/model_simulation.py` ({n} requests x {len(seeds)} seeds per point, "
          f"workload = slices.csv). **All values are simulated**; assumptions are listed at the top of the script.", "",
          "## E1 Baseline (70% offered load, 12 cores in every architecture)", "",
          md_table(["Architecture", "Throughput (req/s)", "SLA met %", "p50 ms", "p95 ms", "p99 ms",
                    "URLLC p95", "eMBB p95", "mMTC p95", "Useful util %", "Imbalance"],
                   [[a, fmt(base[a]["throughput_rps"], 0), fmt(base[a]["sla_pct"], 1), fmt(base[a]["p50_ms"]),
                     fmt(base[a]["p95_ms"]), fmt(base[a]["p99_ms"]), fmt(base[a]["URLLC_p95_ms"]),
                     fmt(base[a]["eMBB_p95_ms"]), fmt(base[a]["mMTC_p95_ms"]), fmt(100 * base[a]["useful_util"], 1),
                     fmt(base[a]["imbalance"])] for a in ARCHS]), "",
          "## E2 Load sweep - SLA met % / p95 ms", "",
          md_table(["Architecture"] + [f"{int(r * 100)}%" for r in rhos],
                   [[a] + [f"{sweep[a][r]['sla_pct']:.1f} / {sweep[a][r]['p95_ms']:.1f}" for r in rhos] for a in ARCHS]), "",
          "## E3 Scalability (SLA-met goodput in req/s; efficiency vs. linear scaling)", "",
          md_table(["Architecture (SLA-met goodput req/s, efficiency)", "k=1", "k=2", "k=4", "k=8"],
                   [[a] + [f"{scal[a][k]['sla_goodput_rps']:.0f} ({100 * scal[a][k]['efficiency']:.0f}%)" for k in [1, 2, 4, 8]]
                    for a in ARCHS]), "",
          "## E4/E5 Reliability", "",
          md_table(["Architecture", "Served during failure %", "SLA met whole run %", "Expected unserved",
                    "P(total outage)", "Unserved min/year"],
                   [[a, fmt(rel_sim[a]["win_served_pct"], 1), fmt(rel_sim[a]["sla_pct"], 1),
                     f"{rel_an[a]['expected_unserved']:.2e}", f"{rel_an[a]['p_total_outage']:.2e}",
                     fmt(rel_an[a]["unserved_min_per_year"], 1)] for a in ARCHS]), "",
          "## E6 Cost", "",
          md_table(["Architecture", "USD / month", "USD per million SLA-met requests"],
                   [[a, cost[a], fmt(cost_per_m[a], 3)] for a in ARCHS]), "",
          "## E7 Decision matrix (normalised 0-1 scores, higher is better)", "",
          md_table(["Architecture"] + CRITERIA + list(WEIGHT_PROFILES) + ["Random-weight win %"],
                   [[a] + [fmt(scores[c][a]) for c in CRITERIA] + [fmt(totals[p][a]) for p in WEIGHT_PROFILES]
                    + [fmt(mc[a], 1)] for a in ARCHS]), "",
          "## E8 Sensitivity - requests meeting SLA % (URLLC SLA % in brackets)", "",
          md_table(["Scenario"] + ARCHS,
                   [[nm] + [f"{res[a]['sla_pct']:.1f} ({res[a]['URLLC_sla_pct']:.0f})" for a in ARCHS] for nm, res in sens]), "",
          "Winner by profile: " + "; ".join(f"**{p}** -> {w}" for p, w in winner.items()), ""]
    with open(os.path.join(out, "results_summary.md"), "w") as f:
        f.write("\n".join(md))

    plot_all(out, base, sweep, scal, rel_sim, rel_an, cost, cost_per_m, scores, totals, mc)

    print("\n" + "\n".join(md))
    print(f"\nFiles written to {out}")


if __name__ == "__main__":
    main()
