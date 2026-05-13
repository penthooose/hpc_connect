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

---

## Two setup workflows

### 1. Livebook Session Bootstrap

Use this on a shared or local Livebook runtime when the SSH key is uploaded through the notebook UI.

```elixir
boot =
  HpcConnect.prepare_livebook_session(
    cluster: :alex,
    remote_command: "hostname && whoami",
    persist_form: true,
    submit_label: "Connect to HPC"
  )
  |> HpcConnect.bootstrap()

session = boot.session
```

Notes:

- the form collects **cluster**, **username**, **SSH private key**, and optional **HF token**
- the uploaded SSH key is copied into a temporary credential directory
- the optional HF token is used for gated Hugging Face models and is **not persisted**
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
    username: "v135ca12",
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
    username: "v135ca12",
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
