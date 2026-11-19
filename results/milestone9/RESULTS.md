# Milestone 9 – Transparency and Reliability Results

## Environment and method

Measurements were taken on the host using the Docker Compose edge/core worker containers, Kafka, and the logical gateway. For the overhead comparison, 30 short processes were submitted directly to the edge worker and 30 through the gateway in sequence. The script waited for each short process to complete before the next sample to avoid saturating the small cluster. Each accepted gateway submission was synchronously mirrored to the other worker. A separate controlled failure trial stopped the active `core` container while a long-running process was active, then queried the same logical resource ID through the gateway.

## Access, location, and replication transparency

| Measure | Observation |
|---|---:|
| Direct worker submissions | 30 / 30 succeeded |
| Logical gateway submissions | 30 / 30 succeeded |
| Gateway submissions with a standby replica acknowledgement | 30 / 30 |
| Gateway response exposed worker location | No |

## Gateway overhead

| Path | Mean latency | p50 | p95 |
|---|---:|---:|---:|
| Direct worker API | 8.81 ms | 6.71 ms | 12.69 ms |
| Logical gateway (placement + synchronous mirror) | 35.85 ms | 32.37 ms | 50.82 ms |

Mean gateway overhead was **27.04 ms per request**, or **306.8%** relative to the low-latency direct localhost request. The percentage is large because the baseline is only 8.81 ms; the absolute added latency is the more useful comparison for this host-local prototype. The gateway checks worker status and waits for a standby write before acknowledging, so this cost is expected. The detailed paired rows are in `transparency_requests.csv`; rerun `python host_pc/transparency_experiments.py --requests 30` to measure on another host or load.

## Failure and migration transparency

In the controlled single-worker trial, the active `core` container was stopped. A GET using only the logical resource ID was answered as `running` by the surviving worker after replica promotion, with **4.35 s** measured end-to-end recovery latency. This demonstrates single-node failure recovery and hides the placement change from the caller. One controlled failure is a functional demonstration, not enough samples to estimate a statistical availability probability or general MTTR.

The pre-existing worker overflow path continues to migrate work to its peer when local simulated CPU or memory capacity is exhausted. A 5-CPU process submitted directly to edge (configured for 4 CPU units) was acknowledged as offloaded to its peer; the same ID was then located through the gateway with status `running`. The one-trial record is in `migration_trial.json`, and the result is summarized in `summary.json`.

## Reliability limits

The gateway is a single point of failure. Standby state is held in worker memory, not in a quorum or a durable replicated log. Replication captures the accepted process snapshot; after a crash, unfinished work can restart from that snapshot, so the execution guarantee is at-least-once. An external side effect may repeat. A simultaneous loss of both workers, or loss of the gateway, is not masked by this milestone. Completion notification narrows the replay window but does not provide exactly-once execution.
