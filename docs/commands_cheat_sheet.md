# HpcConnect Command Cheat Sheet

This sheet lists the most important commands from the current workflow.

See also:

- [README](../README.md)
- [Manual](./hpc_connect_manual.md)
- [Tutorial notebook](../examples/hpc_connect_tutorial.livemd)

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
		username: "v135ca12",
		key_path: Path.expand("~/.ssh/id_fau_hpc_connect_pem"),
		env_file: ".env"
	)

session = boot.session
```

---

## Status queries

```elixir
HpcConnect.available_gpu_summary(session)
HpcConnect.list_downloaded_models(session)
HpcConnect.list_jobs_summary(session)
HpcConnect.quota_summary(session)
```

## Models and images

```elixir
HpcConnect.download_model(session, "meta-llama/Llama-3.2-1B-Instruct")
HpcConnect.build_sif(session, "vllm")
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
