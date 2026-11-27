# Multi-site run: edge in Nairobi, core in Dublin, cloud in Tokyo

The steady-state and failover experiments in `RESULTS.md` ran on one machine. This run puts each
tier on a different continent so heartbeats, replication and failover cross the real internet:

| Tier | Where | Worker | Capacity |
|---|---|---|---|
| Edge + proposed gateway | PC in Nairobi | `edge` | 4 CPU units, 1 GB |
| Core | AWS eu-west-1 (Dublin) | `core` | 8 CPU units, 2 GB |
| Cloud | AWS ap-northeast-1 (Tokyo) | `cloud1` | 8 CPU units, 2 GB |

Each remote worker runs under `host_pc/site_worker.sh` (lowest CPU priority, restarted 10 s after a
crash, listening on 127.0.0.1 only). The gateway reaches it through an SSH tunnel; no port is opened
to the internet. `host_pc/geo_demo.py` drives the run; its config (`geo_nodes.example.json`) keeps
server addresses out of the repository.

## Measured network (22 Nov 2026, from Nairobi, HTTP request through the tunnel)

| Site | p50 | p95 | max | n |
|---|---:|---:|---:|---:|
| Dublin | 402 ms | 658 ms | 1,657 ms | 30 |
| Tokyo | 1,494 ms | 2,033 ms | 2,150 ms | 30 |

The heartbeat's smoothed RTT during the runs: edge 24–36 ms, Dublin 414–434 ms, Tokyo 876–1,174 ms.

## What had to change: adaptive per-node timeouts

The local design used one probe timeout (τ = 0.3 s) and one request timeout (1.5 s). Across these
links Tokyo would have been declared dead on every probe, and half of all Tokyo requests would have
timed out. The gateway now keeps a per-node timeout learned from its heartbeats, using the TCP
retransmission-timeout estimator (RFC 6298, Jacobson/Karels):

  SRTT ← 7/8·SRTT + 1/8·R, RTTVAR ← 3/4·RTTVAR + 1/4·|SRTT − R|, RTO = clamp(SRTT + 4·RTTVAR, 0.3 s, 5 s)

Probe timeout = RTO; worker-request timeout = max(1.5 s, 2·RTO). On one machine RTO stays at the
0.3 s floor, so `RESULTS.md` still describes the local behaviour (6/6 integration tests pass and the
local demo detects in 0.68 s after the change). Measured RTOs here: edge 0.30 s, Dublin 0.87–1.08 s,
Tokyo 2.41–3.72 s.

## Runs (3 demonstration runs, not a statistical sample)

| Run | Fault on the Dublin owner | Suspected | Promotion in Tokyo | Client GET after failover | Rejoin | Copies running at end |
|---|---|---:|---:|---:|---|---:|
| 1 | crash | t = 2.3 s | 1,444 ms | 2,648 / 1,692 ms | re-protected 1 | 1 |
| 2 | network partition 17 s (worker kept running) | t = 1.9 s | 1,695 ms | 1,629 / 2,307 ms | **fenced 1** by demotion, re-protected 1 | 1 |
| 3 | crash | t = 3.2 s | 1,925 ms | 2,092 / 1,996 ms | re-protected 1 | 1 |

Times are from the moment the fault was issued (the crash itself fires ~0.5 s later, after the
request crosses to Dublin). Before the fault, the same logical URI was served from Dublin in
415–486 ms.

## What this shows and what it does not

- **Shows:** the same logical name keeps answering while the process moves between continents;
  failure detection works from messages alone over real WAN links; a true network partition (the
  Dublin worker kept running) is detected, and on reconnection the stale copy is fenced, leaving one copy.
- **Latency is geography.** After failover the client in Nairobi is served from Tokyo, so each
  request costs ~1.6–2.6 s. Transparency hides *where* the process is, not *how far away* it is;
  latency-aware placement (keep URLLC work near the client) is the natural next improvement.
- **A slow site slows detection everywhere.** One heartbeat round waits for its slowest probe, so
  Tokyo's ~1 s RTT stretches Dublin's detection period from 0.5 s to ~1 s (run 3: 3.2 s). Per-node
  detector threads would remove this coupling; not done in this milestone.
- **Three runs.** Enough to show the mechanism works across continents, not to estimate a
  distribution. The servers are shared with other workloads; the demo workers ran at the lowest
  priority and are stopped after each run.
