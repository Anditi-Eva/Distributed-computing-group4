# Milestone 12 – Distributed Telecom File Storage Results

Measured on the local Docker Compose deployment: one gateway, edge and core worker containers, and Kafka. The benchmark stored 20 exact 4 KiB objects (81,920 payload bytes total) in both worker replicas. These results describe this host and workload only.

| Measurement | Result |
|---|---:|
| Uploads and byte-for-byte checksum validations | 20/20 |
| Mean upload latency | 111.82 ms |
| p95 upload latency | 133.48 ms |
| Mean download latency | 24.35 ms |
| p95 download latency | 35.89 ms |
| End-to-end upload throughput | 6.71 files/s |
| Average reported application messages per upload | 22 |
| Average application payload bytes per upload exchange | 13,954.45 bytes |
| Conditional concurrent-write race | 1 commit, 1 version conflict |
| Hierarchical name lookup | Passed |
| File listing | 21 entries (20 data files plus the CAS test object) |

The byte count covers application request/response bodies measured at the gateway, including the replicated file body sent during prepare and returned while checking replicas. It excludes TCP/IP, TLS, and Docker bridge framing. This is not a wire-level packet count.

During the final run, Kafka inspection found 44 `file.replica_committed` records for 22 committed object versions across two workers. The edge Prometheus endpoint reported 22 replica commits and 81,935 committed bytes (including the CAS test object). Kafka emits one record per worker replica, keyed by transaction ID in the event payload; a downstream consumer can correlate the two replica acknowledgements.

## Worker failure and repair trial

With the edge worker stopped, the gateway reported degraded storage with only `core` healthy. A new file write succeeded with one replica, and its subsequent read returned the exact payload. After edge restarted, the next read returned the same content from two replicas and reported one read repair, confirming that the returning worker was brought up to the newer version. Both worker containers were healthy again after the trial.

## Consistency and transaction behavior

The file store uses a per-file version and SHA-256 checksum, gateway-serialized prepare/commit, and `If-Match` compare-and-set. Reads choose the highest version, reject conflicting checksums at the same version, and repair stale replicas. Delete uses a versioned tombstone. In the test, one of two concurrent writes using the same ETag committed and the other received HTTP 409. The failure trial demonstrates degraded write/read service and later replica repair for one worker loss; it is not a proof of quorum consensus or independent-site durability.

The telecom integration smoke check uploaded a two-replica URLLC policy, created and named a process with that `config_path`, and confirmed the worker's process record retained the policy path and version 1. The process was located and terminated through the logical name, with acknowledgements from both workers.

The repository also retains the earlier Milestone 5 coordinator and participant transaction experiments (2PC/3PC). The new file transaction path applies prepare/commit directly to the worker file replicas; it is not a claim that the standalone Milestone 5 coordinator is now the file store's transaction coordinator.

The gateway and workers run as containers with durable named volumes. The file namespace is a compact replicated file/object abstraction backed by local SQLite BLOBs. It is not POSIX NFS or Hadoop HDFS. It currently has no tenant authentication/encryption and the single gateway remains a coordination and availability limit. Temporary test objects are removed from the visible `milestone12/` namespace after the benchmark; measured CSV/JSON artifacts remain here.

Raw measurements: `storage_measurements.csv` and `summary.json`.
