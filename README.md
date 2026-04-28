## HpcConnect

Minimal Elixir API for FAU/NHR HPC workflows:

- connect via SSH
- upload/run helper scripts
- download HF models to vault
- query SLURM resources
- allocate GPU nodes

---

## Command quick map

### Local mode (IEx)

| Goal                                            | Command                                                                                                                                          |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| Build session + startup info                    | `boot = HpcConnect.bootstrap(mode: :local, username: "your_username", key_path: Path.expand("~/.ssh/id_hpc"), cluster: :alex, env_file: ".env")` |
| Get session                                     | `session = boot.session`                                                                                                                         |
| Download model                                  | `HpcConnect.download_model(session, "meta-llama/Llama-3.2-1B-Instruct")`                                                                         |
| List downloaded models                          | `HpcConnect.list_downloaded_models(session)`                                                                                                     |
| GPU availability summary                        | `HpcConnect.available_gpu_summary(session)`                                                                                                      |
| Quota summary                                   | `HpcConnect.quota_summary(session)`                                                                                                              |
| Job summary                                     | `HpcConnect.list_jobs_summary(session)`                                                                                                          |
| Allocate GPU (only required params: GPU + time) | `alloc = HpcConnect.allocate_gpu(session, partition: "a100", walltime: "02:00:00")`                                                              |
| Cancel SLURM job                                | `HpcConnect.cancel_job(session, "<job_id>")`                                                                                                     |
| Re-upload helper scripts (manual)               | `HpcConnect.install_remote_scripts!(session)`                                                                                                    |

### Livebook mode

| Goal                     | Command                                                                                       |
| ------------------------ | --------------------------------------------------------------------------------------------- |
| Build session + probe    | `result = HpcConnect.connection_setup(mode: :livebook, remote_command: "hostname && whoami")` |
| Get session              | `session = result.session`                                                                    |
| Preview SSH command      | `result.command_preview`                                                                      |
| Download model           | `HpcConnect.download_model(session, "meta-llama/Llama-3.2-1B-Instruct")`                      |
| Cleanup temp credentials | `HpcConnect.connection_cleanup(result, delete_uploaded: true)`                                |
| Cleanup orphan temp dirs | `HpcConnect.cleanup_livebook_orphans(delete_uploaded: true)`                                  |

### Raw API commands

| Function                            | Example                                                                                       |
| ----------------------------------- | --------------------------------------------------------------------------------------------- |
| `HpcConnect.connect!/3`             | `HpcConnect.connect!(session, "hostname && whoami")`                                          |
| `HpcConnect.connect_command/2`      | `cmd = HpcConnect.connect_command(session, "squeue -u $USER")`                                |
| `HpcConnect.run_command!/2`         | `HpcConnect.run_command!(cmd)`                                                                |
| `HpcConnect.command_preview/1`      | `HpcConnect.command_preview(cmd)`                                                             |
| `HpcConnect.module_load_preamble/1` | `HpcConnect.module_load_preamble(modules: HpcConnect.default_modules(), conda_env: "elixir")` |
| `HpcConnect.new_model/2`            | `model = HpcConnect.new_model("Qwen/Qwen3-0.6B")`                                             |
| `HpcConnect.new_job/2`              | `job = HpcConnect.new_job(session, port_range: {8000, 8999}, conda_env: "elixir")`            |
| `HpcConnect.plan/3`                 | `plan = HpcConnect.plan(session, model, job)`                                                 |

---

## Allocation and inference flow

### Option A — persistent allocation (`hold_gpu`)

Holds the SLURM allocation alive as a background Port. Release or let it die with the process.

| Step                                | Command                                                                         |
| ----------------------------------- | ------------------------------------------------------------------------------- |
| 1. Hold GPU node (returns on ready) | `alloc = HpcConnect.hold_gpu(session, partition: "a100", walltime: "01:00:00")` |
| 2. Inspect                          | `alloc.job_id`, `alloc.node`                                                    |
| 3. Open SSH tunnel to node          | `proxy = HpcConnect.start_proxy(session, alloc.node, remote_port: 8000)`        |
| 4. Open tunnel port                 | `tunnel = HpcConnect.open_proxy!(proxy)`                                        |
| 5. Inference base URL               | `proxy.base_url  # => "http://localhost:<port>"`                                |
| 6. Release                          | `HpcConnect.release_gpu(alloc) ; Port.close(tunnel)`                            |

### Option B — sbatch job (`submit_vllm`)

Submits a self-contained vLLM sbatch script; better for long runs that outlive the Elixir session.

| Step                             | Command                                                                                                                                   |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| 1. Submit vLLM sbatch job        | `job = HpcConnect.submit_vllm(session, "meta-llama/Llama-3.2-1B-Instruct", partition: "a100", walltime: "02:00:00", conda_env: "elixir")` |
| 2. Wait for compute node         | `node = HpcConnect.wait_for_job_node(session, job.job_id)`                                                                                |
| 3. Build SSH tunnel info         | `proxy = HpcConnect.start_proxy(session, node, remote_port: job.port)`                                                                    |
| 4. Open tunnel (background port) | `tunnel = HpcConnect.open_proxy!(proxy)`                                                                                                  |
| 5. Inference base URL            | `proxy.base_url  # => "http://localhost:<port>"`                                                                                          |
| 6. Close tunnel + cancel job     | `Port.close(tunnel) ; HpcConnect.cancel_job(session, job.job_id)`                                                                         |

`submit_vllm` options: `:partition`, `:gpus`, `:walltime` (HH:MM:SS), `:port` (default 8000), `:cpus_per_task`, `:modules`, `:conda_env`.

---

## Speed up repeated commands: ControlMaster

Each SSH command normally pays ~1–2 s for TCP + key exchange. Open a persistent master once:

```elixir
{session, master} = HpcConnect.open_master!(session)
# all subsequent commands using session are near-instant
HpcConnect.connect!(session, "squeue -u $USER")
HpcConnect.list_downloaded_models(session)
Port.close(master)  # close when done
```

Requires OpenSSH ≥ 6.7. Windows: build 1803+ for AF_UNIX socket support.

---

## Default directory behavior

| Purpose            | Path                                          |
| ------------------ | --------------------------------------------- |
| Work scripts/cache | `/home/hpc/<group>/<user>/.cache/hpc_connect` |
| Models vault root  | `/home/vault/<group>/<user>/models`           |

Examples:

- `hpcusr12 -> /home/hpc/hpcusr/hpcusr12/.cache/hpc_connect`
- `barz123h -> /home/hpc/barz/barz123h/.cache/hpc_connect`

---

## Troubleshooting

| Symptom                                 | Action                                                               |
| --------------------------------------- | -------------------------------------------------------------------- |
| `connect to host csnhr... refused`      | Connect VPN, retry SSH                                               |
| Model saved under literal `'$HOME'` dir | `recompile()` and rebuild session (`bootstrap` / `connection_setup`) |
| `huggingface_hub is not installed`      | retry `download_model` (auto-installs)                               |
| Python too old on cluster               | ensure `module load python/3.12-conda` works                         |
| Remote scripts missing or corrupted     | run `HpcConnect.install_remote_scripts!(session)`                    |
