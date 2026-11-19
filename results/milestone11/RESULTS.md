# Milestone 11 – RPC and Distributed Shared State Results

Measured on the local Docker Compose deployment with two worker containers (edge and core), one transparency gateway, and Kafka running. These numbers characterize this host and this test only.

## RPC communication

| Measurement | Result |
|---|---:|
| Concurrent `system.ping` RPCs | 100/100 succeeded |
| Concurrency | 10 |
| Throughput | 102.50 calls/s |
| Mean latency | 94.40 ms |
| p50 latency | 78.84 ms |
| p95 latency | 202.71 ms |

## Distributed state and synchronization

| Measurement | Result |
|---|---:|
| Versioned replicated writes | 30 |
| Immediate edge/core replica checks | 30/30 matched |
| Mean write latency | 124.76 ms |
| p95 write latency | 137.24 ms |
| Write throughput | 6.45 writes/s |
| Single worker state-read latency sample | 10.14 ms |
| Estimated synchronization overhead | 114.63 ms |
| Average reported protocol messages per write | 18 |
| Average reported application payload bytes per write | 4,223.4 bytes |
| Concurrent compare-and-set race | 1 accepted, 1 conflict |
| Tombstone visibility on both workers | Passed |

Synchronization overhead is estimated as mean replicated write latency minus one direct replica read latency. It is indicative rather than a perfectly controlled paired measurement. Message counts and bytes include the gateway RPC request/response and worker HTTP/JSON-RPC exchanges; they exclude TCP/IP, TLS (not enabled), and Docker bridge framing.

## Distributed process RPC

An additional RPC lifecycle trial created a named process, resolved its name to a running resource, and terminated it through the gateway. Termination received acknowledgements from both workers. The lifecycle returned `created → named → located → scheduled → coordinated → accepted`, then lookup and coordinated termination succeeded.

## Consistency interpretation and limits

Gateway writes are serialized and commit synchronously to every worker reachable during the write. Gateway reads use the same serialization lock, select the highest replica version, reject equal-version conflicts, and repair older replicas. Version-based compare-and-set rejects stale writers; tombstones preserve deletion versions. These trials support consistency for this single-gateway, two-worker setup. They do not establish consensus or linearizability across multiple gateways, nor availability during gateway loss. A commit timeout can leave an uncertain outcome; the transaction ID is returned in the error for diagnosis. Write throughput is lower than ping throughput because each write performs replica discovery, prepare, and commit exchanges.

Raw machine-readable measurements are in `rpc_measurements.csv`, `consistency_trials.csv`, and `summary.json`.
