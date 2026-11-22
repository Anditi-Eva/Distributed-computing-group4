# Defence guide: Milestones 9 & 10 (Group 4)

Read this once end to end, then rehearse with the dashboard. Every number here comes from
`RESULTS.md`, `MULTI_SITE.md` or the live runs.

## 1. The system in 60 seconds

A tenant asks for a network slice (eMBB, URLLC or mMTC). The request goes to one **logical
gateway**, which (a) registers a **name** for the slice process, (b) **places** it on the least
loaded worker, (c) writes a **standby copy** on a second worker, and (d) gives the client only a
**logical URI**. The workers run the process (round-robin scheduler with simulated CPU and memory).
If a worker fails, the gateway's **heartbeat detector** notices, **promotes** the standby on a
survivor and **re-points the name**, so the client keeps using the same URI. When the failed worker
comes back, its stale copy is **demoted** to become the new standby (fencing).

Baseline = the gateway we built in weeks 9–10 plus the Milestone 7 `ft_master` detector.
Proposed = the same API with the detector inside the gateway, direct-owner routing, parallel
admission, fencing, a name cache and runtime worker registration.

## 2. One request, step by step (create)

1. `POST /api/processes` arrives at the proposed gateway (`host_pc/proposed_gateway.py`).
2. Names `process/<id>` and `slice/urllc/<id>` are written to the SQLite name server and its cache
   (`cached_naming.py`).
3. Placement reads the **cached** membership view from the detector (no probing on the request
   path): sort by queue length, then free CPU.
4. Two messages go out **in parallel**: `POST /api/submit` to the primary and `POST /api/replica`
   (with `primary=<node>`) to the standby.
5. The gateway records `owner` and `standby` and returns the logical URI. Total: **2 worker
   messages**, whatever the number of workers (baseline: 3N).

Lookup: `GET /api/slices/<id>` → one `GET /api/process/<id>` to the recorded owner (**1 message**).
If that fails, fall back to a broadcast that skips the node that just failed.

## 3. One failure, step by step

1. Each worker has its own detector thread probing `GET /api/status` every Δ (0.25 s locally,
   0.5 s in the cloud) on a persistent connection.
2. A probe that gets no answer within the node's timeout τ is a **miss**. **K = 2** consecutive
   misses means **suspected**.
3. The gateway calls `POST /api/replicas/promote {"failed_node": X}` on every survivor: only X's
   standby copies start running (a fix we made; the old call woke every standby).
4. The promoted process IDs are re-bound to their new owner in the name table.
5. The next client request goes straight to the new owner.
6. When X answers again: for each process moved off X, `POST /api/process/<id>/demote` stops X's
   stale copy and keeps it as the standby (fencing + re-protection in one message).

Detection bound: **T ≤ Δ + K·max(Δ, τ)** = 0.25 + 2 × 0.3 = **0.85 s** locally.
Timeout per node: **τ = max(SRTT + 4·RTTVAR, 2·SRTT)**, clamped to [0.3 s, 5 s]: the TCP
retransmission-timeout estimator (RFC 6298) with a 2× margin. Locally τ stays at 0.3 s; Tokyo ~1.2–1.6 s.

## 4. Numbers to remember

| What | Baseline | Proposed |
|---|---:|---:|
| Admission, 2 / 4 workers (mean) | 86 / 148 ms | 28 / 33 ms |
| Worker messages: create / locate / terminate | 3N / N / N | 2 / 1 / 2 |
| Locate, 4 workers (mean) | 43.8 ms | 17.2 ms |
| Resolve at 10,000 names (p50) | 10.1 ms | 7.5 ms |
| Resume after crash, no client (median) | 3.13 s (6/8; M7 detector) | 0.84 s (8/8) |
| Same, M7 detector tuned to our timers | 0.92 s | (this is why we don't claim speed) |
| Client requests in 5 s after a crash | 6 | 269 |
| Client request latency during outage (p50) | 1,525 ms | 11 ms |
| Hang trials ending with 2 copies running | 16/16 | 0/16 |
| Heartbeat traffic, 2 workers (cost) | 2.8 msg/s | 8.0 msg/s |
| Experiments | 118 failover trials, 60–300 samples per steady-state cell, seed 2403 | |
| Cloud round trips | Dublin↔Tokyo 208 ms network; ~600–800 ms per request through the SSH tunnel | |

Re-check with the final code: 0.87 s / 0.79 s resume, 0/16 duplicates, ~19 ms client latency.

## 5. Where each brief item lives

| Brief | Where |
|---|---|
| M9 transparent service mechanism | `host_pc/proposed_gateway.py` (baseline: `transparency_gateway.py`) |
| M9 replication/migration experiment, reliability evaluation | E4 in `RESULTS.md`, `failover_trials.csv` |
| M9 overhead analysis | E1 in `RESULTS.md`, `admission_requests.csv` |
| M10 naming subsystem | `host_pc/cached_naming.py` over `naming_service.py` |
| M10 process management subsystem | `/api/processes` create, locate, terminate in `proposed_gateway.py` |
| M10 resolution latency, scalability | E2 and E3 in `RESULTS.md` |
| Failure-driven log (section 7) | `FAILURE_DRIVEN_LOG.md` (7 entries) |
| Reproducibility (section 9) | `RESULTS.md` section 6; `m9_m10_benchmark.py` regenerates everything |
| Cloud deployment | `host_pc/cloud_stack.sh`, `site_worker.sh`, `live_dashboard.py`, `MULTI_SITE.md` |
| Tests | `tests/test_m9_m10_integration.py` (6 tests on real workers) |

## 6. Likely questions and answers

**Is this really a distributed system or just processes on one machine?**
Both, deliberately. The benchmarks ran on one laptop so every system saw identical conditions. The
same code runs live on AWS: gateway and core worker in Dublin, cloud worker in Tokyo, 208 ms apart.
The nodes share no memory and communicate only by messages, and each one fails on its own.

**Why a single gateway? Isn't that a single point of failure?**
Yes, and we say so on the limits slide. Milestone 9 targets worker failures. The next step is to
keep the name table in a replicated log (Raft) so several gateways agree on ownership.

**How do you know a node is dead and not just slow?**
You can't, in an asynchronous network (the FLP result). We **suspect** it after K missed
heartbeats, and handle being wrong: if a suspected node was only slow, it comes back still running
the process, and fencing by demotion removes the duplicate. That is exactly the hang and partition
case we demonstrate.

**Why K = 2 and Δ = 0.25 s?**
A trade-off between detection speed (bound 0.85 s) and false suspicions plus heartbeat cost
(N/Δ = 8 msg/s for 2 workers). K = 1 would react to a single lost packet.

**Your detection is only as fast as the tuned baseline. So what did you contribute?**
Right: we show that ourselves (0.84 vs 0.92 s) and don't claim it. The contribution is what the
gateway does with the detection: re-point names so requests stop waiting on the dead worker
(11 ms vs 1.5 s), fence stale copies (0 vs 16 of 16 duplicates), and O(1) instead of O(N)
messages.

**Why does the baseline take 1.5 s per request even after failover?**
It broadcasts every lookup to every configured worker and waits for all of them, including the
dead one, until the 1.5 s timeout. The M7 detector promotes the standby but the gateway never
learns about it.

**What is fencing and why demote instead of delete?**
Fencing stops a node that might still act on stale ownership. Delete leaves a "terminated"
tombstone, which then blocked the replacement standby on that node (we hit this bug).
Demote stops the stale copy and keeps it as the standby: one message, no tombstone.

**What consistency do you guarantee?**
At-least-once execution: a promoted process restarts from the snapshot taken at admission. Not
exactly-once. Fencing guarantees one running copy after a node rejoins, not during a partition.

**How does the adaptive timeout work?**
It's TCP's retransmission-timeout estimator: smoothed RTT plus four times its mean deviation,
updated from every heartbeat. We add a floor of 2× the smoothed RTT, because on a very steady link
the deviation shrinks to zero and the timeout would sit right on the average (we saw false
suspicions of a healthy Tokyo until we added it).

**Why SSH tunnels instead of opening ports?**
No worker port is exposed to the internet, and the tunnel key may only forward to one port. The
cost is about two extra round trips per new connection, which is why each detector keeps one
persistent connection.

**Why not Spark, Kafka or Kubernetes for this milestone?**
We considered Spark for load-aware placement and dropped it: it can't decide faster than the 0.25 s
heartbeat it would summarise, and we couldn't show a measurable benefit. Kafka, Prometheus and
Grafana are in the earlier milestones' Docker Compose stack; Docker wasn't needed to show M9/M10.

**Did the name cache help?**
A little: 10.1 → 7.5 ms at 10,000 names. SQLite with an index is already flat to 10,000 names;
most of the time is the HTTP request. We report it as a small effect.

**Why are your local numbers different from the Milestone 9 write-up (35 ms)?**
Different machine: that ran in Docker on Linux, this on Windows, where new connections are slower.
Compare systems within one run; the message counts are machine-independent.

**How many trials? Is that enough?**
8 per failover cell, 118 in total. Enough to separate 0.8 s from 3 s and 11 ms from 1.5 s; not
enough to separate us from the tuned baseline on detection time, which we say.

**What happens if both workers fail?**
No live copy is left; the process is unavailable until a worker returns, and it restarts from its
snapshot only if a standby survives. More replicas or durable storage (Milestone 12) are needed.

**What would you do next?**
Replicated name table (Raft) to remove the single gateway; latency-aware placement to keep URLLC
work near its client (after failover to Tokyo, a Nairobi client pays the distance); RPC stubs over
these names in M11; replicated storage in M12.

## 7. Talk plan (about 12 minutes + demo)

| Slides | Time | Say |
|---|---:|---|
| 1–3 cover, architecture, gaps | 2 min | The question, the platform, four measured gaps |
| 4–6 design, M9, M10 | 2 min | Four changes; brief requirement → our implementation |
| 7–9 formulas, AWS, tools | 2 min | Detection bound, message counts, how it's deployed |
| 10–15 method and results | 4 min | Fair baselines; one sentence per chart |
| 16–17 real sites, demo | 3–4 min | Switch to the dashboard: create, crash, cut link |
| 18–20 log, limits, contribution | 2 min | What failed, what's not shown, one-sentence contribution |
