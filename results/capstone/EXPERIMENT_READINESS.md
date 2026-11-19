# Final Capstone Experiment Readiness Audit

Audit date: 2026-11-18. This report checks the requested final-capstone dimensions against the current implementation and saved experiment artifacts. It is a code/artifact audit, not a new performance run.

## Verdict

The distributed network-slicing platform contains testable implementations for processing, placement, naming, transactions, RPC, shared state, replicated storage, Kafka events, and worker failure handling. The requested **complete final experiment matrix is not yet implemented as one reproducible experiment**. Several dimensions are supported by separate scripts; others are fixed assumptions or are not measured. Do not describe every requested dimension as already experimentally verified.

## Input dimensions

| Requested variation | Current support | Evidence and limit |
|---|---|---|
| Number of nodes | Partial; simulated scale-out | `host_pc/model_simulation.py` sweeps scale factor `k=1,2,4,8` and reports scalable architecture results in `results/milestone8/scalability.csv`. This is modeled capacity, not additional live Compose workers. The deployed stack has edge and core workers. |
| Number of users | Partial proxy | Live experiments vary request count, request rate, or RPC concurrency (`host_pc/model_live_experiment.py`, `host_pc/rpc_experiments.py`). They do not declare a user population or per-user behavior model. |
| Network bandwidth | Not swept or measured | `host_pc/topology_model.py` contains fixed topology bandwidth labels and `model_simulation.py` uses fixed network-delay assumptions. There is no link shaper or measured link throughput in the capstone experiment. |
| CPU capacity | Partial; logical capacity is fixed per run | Worker `NODE_CPU` values and simulator `CORES_EDGE`/`CORES_CORE` are fixed defaults. Worker resource metrics describe logical process reservations; Docker CPU quota and host CPU saturation are not varied in the experiment. |
| Memory | Partial; logical capacity is fixed per run | Worker `NODE_MEMORY` and simulator memory assumptions are not swept. `/api/status` reports logical available/total memory, not container resident memory pressure. |
| Network load | Partial | Request-rate sweeps exist in the live/simulated Milestone 8 experiments. Offered load and link congestion/bandwidth contention are not jointly varied. |
| Failure rate | Missing for the current worker deployment | The repository has a Milestone 7 fault framework and historical results, but it is not wired to the current `worker_node.py`: the injector expects `/chaos/inject`, and the master calls `/api/failover/promote` and `/admin/restart_service`; the current worker exposes none of those routes. `model_live_experiment.py --failure` masks a node from that test client's view rather than stopping the Compose worker. Milestone 9/12 controlled stop/restart experiments cover a single real worker loss, not a configurable failure-rate sweep. |
| Request rate | Implemented separately | `model_live_experiment.py --rate` controls open-loop requests/s; `model_simulation.py` sweeps offered load. The live script is constrained to the two configured worker URLs. |
| Data volume | Implemented for file storage | `host_pc/storage_experiments.py --files --file-bytes` varies file count and object size, validating checksums and recording storage latency/throughput. It is a storage-specific test, not part of the same end-to-end load matrix. |

## Requested measurements

| Measurement | Current support | Evidence and limit |
|---|---|---|
| Latency and throughput | Yes, separate experiments | Live slice tests, RPC tests, and storage tests report latency/throughput. |
| Jitter | Missing | No script reports latency variation as jitter (for example, standard deviation or consecutive RTT variation). |
| CPU and memory utilization | Partial | Milestone 8 samples worker logical CPU/memory allocation. Milestone 7 includes host-side process resource sampling for specific fault/design experiments. The final live Compose run does not collect actual per-container CPU and RSS alongside each request sample. |
| Bandwidth utilization | Missing | Current tests count storage/RPC application payload bytes, not link bandwidth utilization or saturation. |
| Availability and MTTR | Partial / historical | Saved Milestone 7 reports contain availability and MTTR, but their fault test framework currently mismatches the worker API as described above; treat those numbers as historical, not verified against the current Compose worker. Milestone 9/12 demonstrate a controlled single-worker failure and storage repair, but do not produce a full system availability/MTTR sweep. |
| Failure rate | Missing as a controlled metric | Fault outcomes are recorded in older artifacts, but a valid current-stack injected-failure rate is not measured or correlated with load and resource limits. |
| Consistency | Yes, separately | Milestone 11 checks replicated state, CAS conflicts and tombstones; Milestone 12 checks replicated files, CAS, checksum and read repair. See `results/milestone11/RESULTS.md` and `results/milestone12/RESULTS.md`. |
| Scalability | Partial | Modeled compute scale-out and naming registry scale tests exist. Live worker count is not varied. |
| Coordination overhead | Partial | Gateway/transparency and replicated state/storage experiments report overhead or message counts, but not as a shared cross-layer metric in a single final run. |

## Existing experiment entry points

These are useful component tests; they do not constitute the complete requested factor matrix:

```sh
python host_pc/model_live_experiment.py --requests 60 --rate 8 --repeat 1
python host_pc/rpc_experiments.py --ping-requests 100 --concurrency 10 --state-writes 30
python host_pc/storage_experiments.py --files 20 --file-bytes 4096
```

The live slice test uses the deployed edge/core workers and calls `/api/reset` between models, so only run it when clearing worker queues is safe. RPC and storage are live component tests. Do not treat the checked-in Milestone 7 matrix/soak as a runnable current-stack test until its API mismatch is fixed. The Milestone 9/12 stop/restart test is the current real worker-loss path, but it tests a single controlled failure. Keep component results separate and label all evidence as live, emulated, simulated, or historical.

## Work required for a complete capstone experiment

1. Add one experiment manifest that records each factor value, seed, node topology, and whether a point is live, emulated, or simulated.
2. Add live scale presets for worker counts, plus startup-time CPU/memory capacity parameters; collect actual container CPU/RSS as well as logical allocation.
3. Add controlled link bandwidth/delay/loss shaping and report achieved bytes/s. An application-level delay estimate must be labeled as emulation, not physical bandwidth shaping.
4. Add explicit user count and per-user request generation, and calculate jitter from the request latency series.
5. Adapt failure injection/master recovery to the current worker API (or implement a Compose-aware injector), then parameterize failure frequency and repair duration; join availability/MTTR/failure rate with load and resource data.
6. Combine the workload and distributed-property checks (state/file consistency and coordination-message overhead) into a repeatable sweep with raw CSV/JSON and a generated summary.

Until those items are implemented and run, the project is a substantial capstone platform with component experiments, but the full final-capstone experiment in the assignment has not been completed.
