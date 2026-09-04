# OpenVM

`zkvm: openvm` selects this zkVM. `examples/openvm-4x4.example.yaml` and `examples/openvm-1x1-local.example.yaml` deploy OpenVM 2.1.0-preview.

## Image

- `dockers/zkvm/Dockerfile.openvm` builds `edge-manager` and `edge-worker` from han0110/axiom-edge `8cf56b35` and copies `convert_fixtures` from that build. The worker carries the features `cuda,jemalloc,parallel,rvr` for CUDA architecture 120.
- The entrypoint runs `edge-manager`, `edge-worker`, or any other container command under `tini`.
- The default `RUSTFLAGS` is `-Ctarget-cpu=native`. The `publish-zkvm-image` workflow passes `RUSTFLAGS=`, which overrides it.

## Ports

| Port              | Service                          | Bound to                      |
| ----------------- | -------------------------------- | ----------------------------- |
| 3000              | coordinator client API           | every interface, host network |
| 8001 + the GPU id | worker of that GPU               | every interface, host network |

## Containers

| Container             | Role                                                                    | Restart policy              |
| --------------------- | ----------------------------------------------------------------------- | --------------------------- |
| `openvm-coordinator`  | `edge-manager`                                                          | on-failure                  |
| `openvm-worker-<gpu>` | `edge-worker` on one GPU with an even `cpuset` share of the host's CPUs | on-failure                  |
| `openvm-keygen`       | one-off per guest per host, `convert_fixtures keygen`                   | none, removed after the run |
| `openvm-keygen-probe` | one-off, lists and digests the baselines in the artifacts volume        | none, removed after the run |

## Volumes

| Volume or bind                      | Mount point                    | Holds                                                                                                                                                         |
| ----------------------------------- | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `openvm-artifacts-<zkvm_version>`   | `/data/artifacts`              | `programs/<name>/0/program.vmexe`, `programs/<name>/0/baseline.bin`, and the shared `app_pk` and `agg_stark_pk`, read-only in the coordinator and the workers |
| `openvm-rvr-cache-<zkvm_version>`   | `/var/cache/openvm-rvr`        | the native compile cache of the workers                                                                                                                       |
| bind `/var/tmp/openvm-final-proofs` | `/var/tmp/openvm-final-proofs` | the final proofs the coordinator persists                                                                                                                     |
| bind `/var/tmp/openvm-metrics`      | `/data/metrics`                | the metrics the coordinator writes                                                                                                                            |

## Configuration

The [README](../../README.md#configuration) lists the keys every zkVM shares.

| Key                        | Default                                         | Meaning                                                                                |
| -------------------------- | ----------------------------------------------- | -------------------------------------------------------------------------------------- |
| `workers[].ssh`            | local daemon                                    | host of one worker container                                                           |
| `workers[].ip`             | loopback, required for a worker on another host | `worker_url` the coordinator dials back                                                |
| `workers[].gpu.device_ids` | required                                        | one GPU id, names the container and its port, the CLI rejects an id repeated on a host |
| `config.app_provers`       | `2`                                             | `max_app_provers` in the coordinator and worker TOML                                   |
| `config.leaf_provers`      | `2`                                             | `max_leaf_provers` in the coordinator and worker TOML                                  |
| `config.internal_provers`  | `1`                                             | `max_internal_provers` in the coordinator and worker TOML                              |
| `config.segment_memory`    | `16106127360` (15 GiB)                          | `default_segment_memory` in the worker TOML                                            |
| `config.timeout_secs`      | `600`                                           | `timeout_secs` in the coordinator TOML                                                 |
| `config.shm_size_gb`       | `2`                                             | `/dev/shm` size of the worker and keygen containers                                    |

## Budgets

| Budget            | Value | Covers                                                        |
| ----------------- | ----- | ------------------------------------------------------------- |
| coordinator ready | 120 s | port 3000 open after the start                                |
| worker ready      | 900 s | the `Edge Worker listening on` line of every worker on a host |
| HTTP request      | 300 s | one input upload or proof download                            |
| connect           | 10 s  | one dial to the coordinator                                   |
| stream silence    | 60 s  | a status stream without data, which reconnects                |
| reconnect         | 1 s   | the pause between status stream reconnects                    |
| ready poll        | 3 s   | the pause between `/readyz` polls                             |
| submit retry      | 5 s   | the pause between refused submissions                         |

## Behavior

- Keygen runs per guest per host, the coordinator host included. Keygen is deterministic, so no bytes cross hosts, and a failed run removes the partial program directory.
- Every `up` compares the sha256 of each `baseline.bin` with the sha256 of the configured vk, programs from an earlier `up` included.
- The program name is `program-<first 8 bytes of sha256(elf) in hex>`. The `--elf` of `serve` must therefore be byte identical to the `guests[].elf` of the deployment.
- `serve` checks the vk the cluster holds for the program against `--vk` and polls `/readyz` until every worker registers. It then prints `stateless validator <name> provisioned, program <name>`.
- `serve` reads `GET /proof_pipeline/{uuid}` and `GET /workers` after every proof. The two answers make the `pipeline` of the metric line.
- The worker ready line, not its registration, gates `up`. A worker binds its socket only after it compiles every loadout program.
- `up` fixes the loadout in `EDGE_PROGRAMS`, and the coordinator refuses proofs for any other program.
- The coordinator accepts one proof at a time.
- At `verbose: 0` the workers log `openvm_cuda_common::memory_manager` at warn.

## Security

- The containers run under the host network, so port 3000 and port 8001 plus each GPU id are open on every interface of their hosts.
