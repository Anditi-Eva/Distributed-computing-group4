# Failure-Driven Engineering Log: Milestones 9 & 10 (Proposed System)

Brief section 7 asks six questions for every change. Each entry below is something that actually
failed while we built the proposed gateway; none is hypothetical.

## Entry 1: the first upgrade draft passed its tests but could not run against real workers

| Question | Response |
|---|---|
| What changed? | A first "enhanced gateway" draft added proactive failover, direct-owner routing and a Spark advisory hook. |
| What failed? | Every process creation would have failed. The draft called `POST /api/process` and `POST /api/replicas/sync`, which do not exist on `worker_node.py` (the real routes are `/api/submit` and `/api/replica`). It also sent `resource_id` and `slice_type` inside the worker payload, which `Process(**payload)` rejects as "Invalid process payload". Its Docker image would not even start: `Dockerfile.gateway` never copied the new file. |
| Why did it fail? | Its unit tests replaced the workers with mocks that accepted whatever the gateway sent, so the tests checked the gateway against itself, not against the worker contract. |
| How was it fixed? | Rewrote the proposed gateway (`host_pc/proposed_gateway.py`) against the real worker routes and payload fields, and replaced the mocked tests with integration tests that start real worker processes (`tests/test_m9_m10_integration.py`). |
| What alternative was considered? | Patching the draft endpoint by endpoint. Rejected: the draft also leaked the physical owner node in client responses (`owner_node`), which breaks the location transparency Milestone 9 is graded on. |
| What was learned? | A test that mocks the other side of a distributed interface cannot catch interface mismatches, which are the most common distributed bug. Integration tests over the real network path are the minimum for a gateway. |

## Entry 2: the "reactive baseline" was a strawman

| Question | Response |
|---|---|
| What changed? | We planned to compare the proposed proactive failover against "baseline failover that only happens when a client asks". |
| What failed? | That comparison was wrong. The deployed baseline (`docker-compose.yml`) also runs the Milestone 7 `ft_master.py`, which heartbeats every worker (1.0 s, 3 misses) and promotes replicas on its own. |
| Why did it fail? | The Milestone 9 write-up measured the gateway in isolation, so the earlier milestone's detector was easy to forget. |
| How was it fixed? | The failover experiment now has four configurations: B0 gateway only, B1 gateway + M7 master as deployed, B2 gateway + M7 master tuned to the proposal's timings, and P. B2 separates "faster timers" from "better architecture". |
| What alternative was considered? | Comparing only B0 vs P, which gives a bigger headline number. Rejected because an examiner who knows Milestone 7 would rightly reject it. |
| What was learned? | The baseline must be the strongest system we already had. Part of the detection speed-up is only timer tuning, and B2 shows how much. |

## Entry 3: promotion activated other nodes' replicas

| Question | Response |
|---|---|
| What changed? | Proactive failover calls `POST /api/replicas/promote` on survivors. |
| What failed? | The worker promoted every standby copy it held, whichever node owned the active copy. With 3+ workers, a failure of node A also re-launched node C's processes on B, so C's work ran twice. |
| Why did it fail? | The replica record did not store which node was the primary; with only two workers the bug cannot show. |
| How was it fixed? | `/api/replica` now records the primary, and `/api/replicas/promote {"failed_node": X}` promotes only X's replicas. With no body it keeps the old behaviour, so Milestone 7/9 callers are unchanged. Covered by `test_targeted_promotion_only_activates_failed_nodes_replicas`. |
| What alternative was considered? | Promoting by explicit process ID list from the gateway. Rejected: the gateway may not know every replica after its own restart; the worker does. |
| What was learned? | Two-node experiments hide whole classes of bugs; the node-count sweep (N = 2, 4) is needed for correctness, not just for scalability graphs. |

## Entry 4: fencing poisoned the node it was meant to protect

| Question | Response |
|---|---|
| What changed? | When a suspected node rejoins, the gateway fences its stale copies (a hung node may still be running work that was promoted elsewhere) and then re-protects processes by giving them a new standby. |
| What failed? | The first version fenced with `DELETE /api/process/<id>`. That leaves a termination tombstone, so the re-protection copy sent to the same node was hidden behind the tombstone, and a later promotion there would have been refused ("Process has been terminated"). Found while writing the rejoin integration test. |
| Why did it fail? | DELETE means "this process is finished everywhere", not "this copy is stale". The two meanings needed different operations. |
| How was it fixed? | New worker endpoint `POST /api/process/<id>/demote`: it stops the stale active copy and keeps it as the standby of the new owner. Fencing and re-protection become one message, with no tombstone. |
| What alternative was considered? | Deleting the tombstone before re-protecting. Rejected: the tombstone protects against replayed requests, which Milestone 9 relies on. |
| What was learned? | Recovery operations need their own verbs; reusing "terminate" for "fence" mixed two different states of a process. |

## Entry 5: a client request could hit the dead owner twice

| Question | Response |
|---|---|
| What changed? | Direct-owner routing sends a lookup only to the recorded owner and falls back to a broadcast on a miss. |
| What failed? | In the window before the heartbeat suspects a node, the direct call timed out (1.5 s), and the fallback broadcast asked the same dead node again (another 1.5 s), then returned the un-promoted standby as `replicated`. |
| Why did it fail? | The fallback trusted the membership view, which had not caught up with what this request had just observed. |
| How was it fixed? | The owner that failed inside the request is passed to the fallback, skipped, and treated as failed, so its standby is promoted in the same request (the baseline's reactive path is kept as a safety net). The move is recorded so the node is fenced when it rejoins. |
| What alternative was considered? | A shorter timeout on the direct call. Rejected for this experiment: both gateways use the same 1.5 s timeout so the comparison stays fair. |
| What was learned? | A failure detector is always behind reality; the request path must use its own evidence too. |

## Entry 6: the Spark/HDFS layer was dropped from this milestone

| Question | Response |
|---|---|
| What changed? | An earlier plan added Kafka → Spark Structured Streaming → load advisories into the gateway, plus HDFS archiving. |
| What failed? | Not built. Spark needs a JVM cluster to make placement decisions for two workers whose load the gateway already sees every 0.25 s; the advisory would arrive seconds later than the heartbeat data it summarises. |
| Why did it fail? | Wrong tool for the decision timescale: stream analytics suits trends over many nodes, not sub-second failover on two. |
| How was it fixed? | Kept placement on the heartbeat view. Distributed storage (HDFS-style replicated files) belongs to Milestone 12, where the repository already has a replicated file store. |
| What alternative was considered? | Running Spark anyway for its presentation value. Rejected: we could not defend a measurable benefit for this milestone. |
| What was learned? | Add a framework only when an experiment shows the problem it solves. |
