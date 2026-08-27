# HpcConnect Command Cheat Sheet

This sheet lists the most important commands from the current workflow.

See also:

- [README](../README.md)
- [Manual](./hpc_connect_manual.md)
- [Tutorial notebook](../examples/hpc_connect_tutorial.livemd)
- [Local shell example](../examples/hpc_connect_local_example.exs)

---

## Livebook bootstrap

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

Optional: provide a Hugging Face token during bootstrap for gated models.

```elixir
boot =
	HpcConnect.prepare_livebook_session(
		cluster: :alex,
		hf_token: System.get_env("HF_TOKEN")
	)
	|> HpcConnect.bootstrap()
```

## Local bootstrap

```elixir
boot =
	HpcConnect.bootstrap(
		mode: :local,
		cluster: :alex,
		username: "your_hpc_username",
		key_path: Path.expand("~/.ssh/id_fau"),
		env_file: ".env"
	)

session = boot.session
```

Native SSH is optional. The default local bootstrap flow uses the normal OS SSH path.

## Connection setup (unified)

`connection_setup/1` is the one-call setup `bootstrap/1` builds on — use it
directly when you only need a session (no startup status gathering).

```elixir
result =
	HpcConnect.connection_setup(
		mode: :local,
		cluster: :alex,
		username: "you",
		key_path: Path.expand("~/.ssh/id_fau"),
		env_file: ".env"
	)

session = result.session
```

## Steady SSH connection (recommended)

Multiplexes all commands over one persistent `ssh <target> "bash -s"` shell
with auto-reconnect and exponential backoff — no per-command handshake.
Enable it in `.env`:

```dotenv
HPC_CONNECT_STEADY_CONNECTION=true
HPC_CONNECT_STEADY_TIMEOUT_SECONDS=30   # per-command ssh connect timeout
```

or per call: `bootstrap(..., steady_connection: true)` /
`connection_setup(..., steady_connection: true)`. Query and control it:

```elixir
HpcConnect.steady_connection?(session)          # boolean
HpcConnect.open_steady_connection!(session)     # open / repair the steady shell
HpcConnect.close_steady_connection(session)     # close it
```

Works on all platforms (including Windows, which lacks OpenSSH ControlMaster).

---

## Status queries

```elixir
HpcConnect.available_gpu_summary(session)
HpcConnect.list_downloaded_models(session)
HpcConnect.list_jobs_summary(session)
HpcConnect.quota_summary(session)
HpcConnect.check_free_nodes(session)          # sinfo-style free-node check
HpcConnect.startup_summary(session)           # what bootstrap gathered at connect time
HpcConnect.connect!(session, "hostname")      # run a one-off remote command
```

## Models and images

```elixir
HpcConnect.download_model(session, "meta-llama/Llama-3.2-1B-Instruct")
HpcConnect.build_sif(session, "vllm")
HpcConnect.build_sif(session, "vllm", force_rebuild: true)  # rebuild even if the .sif exists
HpcConnect.put_hf_token(session, System.get_env("HF_TOKEN")) # gated models after bootstrap
```

## Start vLLM

```elixir
vllm =
	HpcConnect.start_app(session,
		app: "vllm",
		args: [
			partition: "a40",
			gpus: 1,
			walltime: "02:00:00",
			model: "meta-llama/Llama-3.2-1B-Instruct",
			port: 50200
		]
	)
```

## Chat with vLLM

```elixir
HpcConnect.vllm_chat(vllm, "Hello from Livebook")
HpcConnect.vllm_answer(vllm, "Summarize SLURM in one sentence")
```

## Reconnect to an existing vLLM job

```elixir
[%{job_id: job_id} | _] = HpcConnect.list_jobs_summary(session)

reconnected =
	HpcConnect.reconnect(session, job_id,
		app: "vllm",
		args: [port: 50200]
	)
```

## Manual port-forward / tunnel

`start_app` normally handles the tunnel; use these only when you need a manual
forward (e.g. after `wait_for_job_node/2`):

```elixir
node = HpcConnect.wait_for_job_node(session, job_id)
proxy = HpcConnect.start_proxy(session, node, remote_port: 8000, local_port: 50200)
port = HpcConnect.open_proxy!(proxy)
# ... use proxy.base_url ...
HpcConnect.close_proxy(port)
```

## Submit vLLM directly via Apptainer

```elixir
HpcConnect.submit_vllm_apptainer(session, "meta-llama/Llama-3.2-1B-Instruct",
	partition: "a40", gpus: 1, walltime: "02:00:00", port: 50200)
```

## Allocate a GPU without starting an app

```elixir
alloc = HpcConnect.allocate_gpu(session, partition: "a100", walltime: "01:00:00")
HpcConnect.release_gpu(session, alloc)

# or:
HpcConnect.release_gpu(session, "JOBID")
```

## Job control

```elixir
HpcConnect.cancel_job(session, "JOBID")
HpcConnect.cancel_all_jobs(session)
HpcConnect.cancel_pending_waits(session)
```

## Re-upload helper files if needed

```elixir
HpcConnect.install_remote_scripts!(session)
HpcConnect.upload_def_file(session)
HpcConnect.remote_def_path(session)
HpcConnect.remote_sif_path(session)
```

## Cleanup

```elixir
HpcConnect.cleanup_livebook_session(boot)
HpcConnect.cleanup_livebook_session(session)
HpcConnect.cleanup_livebook_orphans(delete_uploaded: true)
HpcConnect.clear_app_cache(boot)
HpcConnect.exit(boot)
```

## Remove remote HpcConnect files

```elixir
HpcConnect.uninstall(boot)
HpcConnect.uninstall(boot, remove_models: true)
```
