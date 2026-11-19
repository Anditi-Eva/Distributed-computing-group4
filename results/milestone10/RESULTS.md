# Milestone 10 – Naming and Distributed Process Management Results

## Test environment and method

The measurements were collected on the host against the Docker Compose edge/core workers and the persistent SQLite-backed gateway name server. The naming registry was grown cumulatively to 10, 100, 500, and 1,000 hierarchical entries. At each size, the experiment sampled up to 100 lookups and measured a prefix listing. The process workflow trial created 20 long-running processes through the managed API, resolved and located each by name, and terminated each before its simulated work completed.

## Naming resolution and scale

| Registry entries | Sampled lookups | Mean resolve | p95 resolve | Batch registration | Prefix list |
|---:|---:|---:|---:|---:|---:|
| 10 | 10 | 4.40 ms | 5.39 ms | 19.13 ms | 7.31 ms |
| 100 | 100 | 7.04 ms | 13.59 ms | 26.99 ms | 24.57 ms |
| 500 | 100 | 7.19 ms | 13.17 ms | 58.21 ms | 14.36 ms |
| 1,000 | 100 | 7.95 ms | 11.79 ms | 30.82 ms | 18.16 ms |

At 1,000 registered names, sampled logical lookup averaged 7.95 ms and p95 was 11.79 ms. Lookup latency increased modestly from the 10-entry run; batch registration and prefix listing did not increase monotonically because these timings are short, host-local samples affected by runtime noise. This demonstrates operation at the tested scale, not a capacity ceiling or production throughput claim. The benchmark namespace was removed after measurement.

## Process management and coordination

| Operation | Trials successful | Mean latency |
|---|---:|---:|
| Create, name, locate, schedule, coordinate, accept | 20 / 20 | 86.95 ms |
| Resolve by logical name | 20 / 20 | 51.78 ms |
| Locate by logical name | 20 / 20 | 34.65 ms |
| Terminate by logical name | 20 / 20 | 39.73 ms |

The gateway’s recorded create-phase means were 12.20 ms for name registration, 40.47 ms for worker location/capacity probes, 0.01 ms for scheduling choice, and 15.88 ms for replica coordination and submit. The remaining difference between the 80.54 ms internal workflow mean and the 86.95 ms client-observed create mean is HTTP/measurement overhead.

Every termination received two worker acknowledgements in this run, covering the active process and standby copy. After termination, the registry retained a tombstone and name resolution reported `terminated`. The smoke workflow also confirmed a create response with one standby acknowledgement, `running` resolution and location, two termination acknowledgements, and `terminated` resolution afterward.

## Interpretation and limits

The static map resolves `worker/edge` and `worker/core`; the persistent name server stores dynamic process names; prefix rules classify worker, service, slice, and process namespaces; and broadcast lookup checks worker copies concurrently before returning a stable gateway URI. The measured name resolution path for live processes includes worker broadcast and is therefore slower than the synthetic `service/` registry lookup in the scalability table.

The name server and gateway are single instances. The experiment tested a maximum of 1,000 dynamic names, 100 lookups per sampled scale point, and 20 lifecycle trials on one host. It does not measure multi-gateway consistency, concurrent high-throughput registration, or production-scale capacity. The lifecycle and scale CSVs plus machine-readable summary are in this folder.
