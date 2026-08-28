# HpcConnect

HpcConnect is an Elixir library for FAU/NHR HPC workflows across **Livebook** and **local shell** usage on **Linux** and **Windows**.

It standardizes:

- SSH-based session setup
- Livebook uploaded-key handling
- model download to HPC storage
- SLURM inspection and job control
- vLLM job start / reconnect
- Apptainer/SIF build helpers
- cleanup and crash recovery

## Documentation

- [Command cheat sheet](docs/commands_cheat_sheet.md)
- [Manual and troubleshooting](docs/hpc_connect_manual.md)
- [Livebook example notebook](examples/hpc_connect_tutorial.livemd)
- [Local shell example](examples/hpc_connect_local_example.exs)

---

## Two setup workflows

### 1. Livebook Session Bootstrap

Use this on a shared or local Livebook runtime when the SSH key is uploaded through the notebook UI.

```elixir
# Cell 1, configure everything in the browser overlay
setup =
  HpcConnect.prepare_livebook_session(
    env_file: Path.expand("../.env", __DIR__),
    fallback_env_file: Path.expand("../.env.example", __DIR__),
    submit_label: "Setup"
  )

env_map = setup.env_map
env_file = setup.env_file

# Cell 2, bootstrap with the configured values
boot =
  HpcConnect.bootstrap(
    mode: :local,
    env_file: env_file,
    cluster: env_map["HPC_CONNECT_CLUSTER"] || "fritz",
    username: env_map["HPC_CONNECT_USERNAME"],
    key_path: env_map["HPC_CONNECT_IDENTITY_FILE"]
  )

session = boot.session
```

Notes:

- the overlay configures **every** env var `HpcConnect` reads (cluster, username, SSH key, proxy, work/vault dirs, steady connection, retry, ...)
- values are pre-filled from `.env` / `.env.example` and persisted between notebook opens (secrets are never persisted; files only fill blank fields)
- the SSH identity field auto-detects an existing `~/.ssh` key, or you can upload one (stored temporarily; removed by `HpcConnect.cleanup_livebook_setup/1`)
- `boot.startup` summarizes GPUs, models, quota, and jobs

Most important Livebook cleanup commands:

```elixir
HpcConnect.cleanup_livebook_session(boot)
HpcConnect.cleanup_livebook_orphans(delete_uploaded: true)
HpcConnect.exit(boot)
```

Use `cleanup_livebook_orphans/1` only for crash/restart recovery when you no longer have the original `boot` or `session` value.

### 2. Local Bootstrap Workflow

Use this in IEx, scripts, or local automation when the key already exists on disk.

```elixir
boot =
  HpcConnect.bootstrap(
    mode: :local,
    cluster: :alex,
    username: "your_hpc_username",
    key_path: Path.expand("~/.ssh/id_fau"),
    remote_command: "hostname && whoami",
    env_file: ".env"
  )

session = boot.session
```

You can also pass the Hugging Face token explicitly during bootstrap:

```elixir
boot =
  HpcConnect.bootstrap(
    mode: :local,
    cluster: :alex,
    username: "your_hpc_username",
    key_path: Path.expand("~/.ssh/id_fau"),
    hf_token: System.get_env("HF_TOKEN")
  )
```

---

## Most important commands

These are the main commands used in the tutorial notebook.

### Status and inspection

```elixir
HpcConnect.available_gpu_summary(session)
HpcConnect.list_downloaded_models(session)
HpcConnect.list_jobs_summary(session)
HpcConnect.quota_summary(session)
```

### Model and image preparation

```elixir
HpcConnect.download_model(session, "meta-llama/Llama-3.2-1B-Instruct")
HpcConnect.build_sif(session, "vllm")
```

### Start or reconnect a vLLM app

```elixir
vllm =
  HpcConnect.start_app(session,
    app: "vllm",
    args: [partition: "a40", gpus: 1, walltime: "02:00:00", port: 50200]
  )

HpcConnect.vllm_chat(vllm, "Hello from Livebook")
```

```elixir
reconnected =
  HpcConnect.reconnect(session, "JOBID",
    app: "vllm",
    args: [port: 50200]
  )
```

### Job control and cleanup

```elixir
HpcConnect.cancel_job(session, "JOBID")
HpcConnect.cancel_all_jobs(session)
HpcConnect.cleanup_livebook_session(session)
HpcConnect.exit(boot)
```

---

## Steady SSH connection (faster + more reliable)

By default every OS `ssh` command spawns a fresh process with its own TCP
handshake and key exchange (~1-2 s each through a jump host). Enable the steady
connection to multiplex all commands over **one persistent
`ssh <target> "bash -s"` shell**:

- no per-command handshake → multi-command worksteps (bootstrap, image builds,
  benchmark runs) get dramatically faster
- **auto-reconnect**: if the shell drops (network, jump-host hiccup) the next
  command transparently reopens it
- **exponential backoff** retries on transient failures
- works on **all platforms**, including Windows (Win32-OpenSSH does not support
  OpenSSH `ControlMaster` multiplexing; this approach needs no ControlMaster)

Activate per session (`.env`):

```env
HPC_CONNECT_STEADY_CONNECTION=true
# optional: HPC_CONNECT_STEADY_TIMEOUT_SECONDS=30
```

Or per call: `HpcConnect.Session.local(env_file: ".env", steady_connection: true)`.

Runtime control:

```elixir
session = HpcConnect.open_steady_connection!(session)   # pre-warm + verify
HpcConnect.steady_connection?(session)                  # enabled?
HpcConnect.close_steady_connection(session)             # tear the shell down
```

`bootstrap/1` pre-warms the shell automatically when the flag is set, and the
startup summary + remote script install are also batched into fewer SSH calls.

---

## Platform support

HpcConnect is designed so that:

- **Livebook mode** works on Linux and Windows runtimes that can execute `ssh`/`scp`
- **local mode** works on Linux and Windows
- HPC access uses the system OpenSSH tools for maximum portability

Practical requirements:

- `ssh` and `scp` must be available in `PATH`
- outbound **TCP port 22** to the first reachable jump/login host must be allowed
- in shared Linux environments, **IPv4 connectivity** to the jump host is often required

The default Livebook and local bootstrap flows use the OS SSH path. A separate
PEM key is only needed when native Erlang SSH is enabled explicitly.

For setup details and troubleshooting, see the [manual](docs/hpc_connect_manual.md).
