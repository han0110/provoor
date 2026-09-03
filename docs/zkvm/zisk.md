# ZisK

`zkvm: zisk` selects this zkVM. `examples/zisk-4x4.example.yaml` and `examples/zisk-1x1-local.example.yaml` deploy ZisK 1.2.0-alpha.

## Image

- `dockers/zkvm/Dockerfile.zisk` installs the `cargo_zisk_linux_amd64` release archive of ZisK under `/root/.zisk`.
- It overlays `zisk-worker-gpu` from han0110/zisk `12b45f0e`, which caches the Main instruction table and serves the health endpoint.
- It builds `zisk-supervisor` from `cmd/zisk-supervisor`.

## Ports

| Port  | Service                       | Bound to                                             |
| ----- | ----------------------------- | ---------------------------------------------------- |
| 7000  | coordinator client API (gRPC) | published on every interface of the coordinator host |
| 50051 | coordinator cluster API       | published on every interface of the coordinator host |
| 7002  | supervisor restart requests   | published on every interface of the coordinator host |
| 7001  | worker health endpoint        | loopback, the worker runs under the host network     |

## Containers

| Container                | Role                                                                           | Restart policy              |
| ------------------------ | ------------------------------------------------------------------------------ | --------------------------- |
| `zisk-coordinator`       | `zisk-coordinator` under `zisk-supervisor`                                     | on-failure                  |
| `zisk-worker`            | `zisk-worker-gpu` under `mpirun` over the selected GPUs, with a health check   | on-failure                  |
| `zisk-proving-key-setup` | one-off, downloads the proving key and builds its const-trees                  | none, removed after the run |
| `zisk-program-setup`     | one-off per guest per host, `cargo-zisk-dev-gpu program-setup`                 | none, removed after the run |
| `zisk-worker-probe`      | one-off, counts NUMA nodes when `mpi_np` is over 1 and `mpi_numa_ppr` is unset | none, removed after the run |

## Volumes

| Volume or bind                    | Mount point              | Holds                                                                                                                                                                         |
| --------------------------------- | ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zisk-proving-key-<zkvm_version>` | `/root/.zisk/provingKey` | the proving key from `https://storage.googleapis.com/zisk-setup/zisk-provingkey-<zkvm_version>.tar.gz` and its const-trees, mounted writable in the worker and the setup jobs |
| `zisk-cache-<zkvm_version>`       | `/root/.zisk/cache`      | registered ELFs, ROM setup, and assembly emulators, addressed by ELF hash, mounted in the coordinator, the worker, and the program setup                                      |

## Configuration

The [README](../../README.md#configuration) lists the keys every zkVM shares.

| Key                                                 | Default                                            | Meaning                                                                                      |
| --------------------------------------------------- | -------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `workers[].ssh`                                     | local daemon                                       | one worker entry per host, the CLI rejects a duplicate host                                  |
| `workers[].gpu.count` or `workers[].gpu.device_ids` | required, one form                                 | the GPU device request and `--compute-capacity` of 10 per GPU, the CLI rejects a repeated id |
| `config.shm_size_gb`                                | `64`                                               | `/dev/shm` size of the worker and program setup containers                                   |
| `config.mpi_np`                                     | `1`                                                | `mpirun -np`                                                                                 |
| `config.mpi_numa_ppr`                               | `mpi_np` divided by the NUMA node count, minimum 1 | `-map-by ppr:<n>:numa`                                                                       |
| `config.mpi_threads`                                | unset                                              | `RAYON_NUM_THREADS` per rank                                                                 |
| `config.max_streams`                                | unset                                              | `--max-streams`                                                                              |
| `config.number_threads_witness`                     | unset                                              | `--number-threads-witness`                                                                   |
| `config.max_witness_stored`                         | unset                                              | `--max-witness-stored`                                                                       |
| `config.minimal_memory`                             | `false`                                            | `--minimal-memory`                                                                           |
| `config.cpu_mops`                                   | `false`                                            | `--cpu-mops`                                                                                 |

## Budgets

| Budget                      | Value                                         | Covers                                                                                |
| --------------------------- | --------------------------------------------- | ------------------------------------------------------------------------------------- |
| coordinator ready           | 120 s                                         | ports 7000, 50051, and 7002 open after the start                                      |
| worker registration         | 300 s                                         | the `Registration accepted` line in the worker log                                    |
| setup job                   | 600 s                                         | the guest setup RPC the forwarder runs                                                |
| cluster ready after restart | 300 s                                         | the register retries and, separately, the setup retries after the coordinator restart |
| register RPC                | 60 s                                          | one register attempt                                                                  |
| restart request             | 30 s                                          | the request to the supervisor                                                         |
| aggregation                 | 600 s                                         | `phase3_timeout_seconds`, counted from the first worker to finish                     |
| submit retry                | 5 s                                           | the pause between refused submissions                                                 |
| worker health check         | every 30 s, 10 s timeout, 10 min start period | kills the ranks when the worker reports itself unrecoverable                          |

## Behavior

- `up` compiles every guest on every worker host before the cluster starts, because a program setup allocates the whole GPU. The setup derives the vk and compares it with the configured one.
- One worker container per host spans the GPUs its `gpu` selects. It advertises 10 compute units per GPU, and the coordinator holds a job until 10 units per deployed GPU are ready.
- The coordinator replays each guest setup to every worker that registers, and two setups in a row corrupt the earlier guest. Each forwarder therefore asks the supervisor on port 7002 to end the coordinator before it registers, and the restart policy starts the replacement.
- The restart count of `zisk-coordinator` climbs by one per forwarder start as well as per failure.
- The supervisor sends SIGTERM at once and SIGKILL 5 s later. A requested end with a clean exit reports 1, so the restart policy starts the replacement. A coordinator that fails on its own carries its own exit code, and a signaled one reports 128 plus the signal.
- `serve` restarts the coordinator, registers the ELF, and runs the guest setup before it prints `stateless validator <name> registered, hash <id>`. The setup reads the cache, so the ROM and assembly generation stays outside a benchmark run.
- The readiness wait returns at once, and the submit retry loop waits out a cluster that refuses the job.

## Security

- The coordinator container publishes ports 7000, 50051, and 7002 on every interface of the coordinator host.
- The supervisor ends the coordinator for anyone who sends `restart` to port 7002.
