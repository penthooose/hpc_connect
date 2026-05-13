# HpcConnect Manual

This manual summarizes the current HpcConnect workflow for **Livebook** and **local** usage on **Linux** and **Windows**.

For a shorter command list, see the [command cheat sheet](./commands_cheat_sheet.md).

---

## 1. What HpcConnect does

HpcConnect provides a consistent Elixir API for:

- SSH session setup
- Livebook uploaded-key handling
- Hugging Face model download
- SLURM status queries and job control
- vLLM start / reconnect
- Apptainer/SIF build helpers
- cleanup and crash recovery

The library uses the system OpenSSH tools (`ssh`, `scp`) for portability.

---

## 2. Installation

### Livebook

```elixir
Mix.install([
	{:kino, "~> 0.19"},
	{:table, "0.1.2"},
	{:hpc_connect, github: "penthooose/hpc_connect", force: true}
])
```

### Mix project / local IEx

Add the dependency in `mix.exs` and run `mix deps.get`.

Requirements:

- Elixir / OTP compatible with the project
- `ssh` available in `PATH`
- `scp` available in `PATH`

On Windows, use Win32-OpenSSH. On Linux, OpenSSH is usually already available.

---

## 3. Setup workflows

### A. Livebook Session Bootstrap

Use this when the SSH key is uploaded through the notebook.

```elixir
boot =
	HpcConnect.prepare_livebook_session(
		cluster: :alex,
		remote_command: "hostname && whoami",
		persist_form: true
	)
	|> HpcConnect.bootstrap()

session = boot.session
```

Important details:

- a temporary credential directory is created under the system temp directory
- a generated SSH config is used for Livebook sessions
- the uploaded key is copied into that temp directory
- an optional Hugging Face token can be supplied during bootstrap
- the token is **not persisted** by the Livebook form defaults
- you may also pass `env_file: ".env"` if the runtime can read that file

### B. Local Bootstrap Workflow

Use this for local IEx, scripts, or CI-style automation.

```elixir
boot =
	HpcConnect.bootstrap(
		mode: :local,
		cluster: :alex,
		username: "v135ca12",
		key_path: Path.expand("~/.ssh/id_fau"),
		env_file: ".env"
	)
```

In local mode the key is used directly from disk; it is not copied into a Livebook temp directory.
Native SSH is **not** enabled by default in the standard local bootstrap flow.

### Optional `.env` support

Both workflows can use an optional `.env` file through `env_file: ".env"`.

This is useful for values such as:

- `HF_TOKEN`
- `username`
- `cluster` defaults

Important behavior:

- the `.env` file is optional
- explicit function arguments still take precedence
- missing values from runtime session such as the HF token are taken from `.env`
- non-`HPC_CONNECT_*` entries are merged into the runtime session environment during bootstrap

---

## 4. SSH key format and native SSH

In the common workflow, HpcConnect can still work entirely through the system
OpenSSH tools (`ssh` / `scp`). That means a separate PEM key is **not always required**.

### When a PEM key is not required

- the default `bootstrap/1` flow uses `native_ssh: false`
- the default `connection_setup(mode: :local)` flow now also uses `native_ssh: false`
- standard Livebook bootstrap therefore usually works with your normal SSH key
- if you keep using the OS SSH path, HpcConnect can continue to use the same
  key that already works with `ssh` / `scp`

### When a PEM key is required

A PEM-compatible private key is needed when HpcConnect uses **native Erlang SSH**.

This matters in particular when:

- you enable `native_ssh: true` explicitly
- you want native vLLM access/tunneling instead of the OS SSH proxy path
- you explicitly turn on native SSH in local setup or later app access

### Which key format is needed

The current native SSH callback does **not** accept private keys in OpenSSH's newer format:

```text
-----BEGIN OPENSSH PRIVATE KEY-----
```

For native SSH, use a PEM-encoded private key instead.

The safest documented choice is an RSA key in PEM format, for example:

```text
-----BEGIN RSA PRIVATE KEY-----
```

### Recommended manual creation command

Linux / macOS:

```bash
ssh-keygen -t rsa -b 4096 -m PEM -f ~/.ssh/id_fau_hpc_connect_pem -N ''
```

Windows PowerShell:

```powershell
ssh-keygen -t rsa -b 4096 -m PEM -f "$HOME\.ssh\id_fau_hpc_connect_pem" -N ""
```

This creates:

- the private key: `id_fau_hpc_connect_pem`
- the public key: `id_fau_hpc_connect_pem.pub`

### What to upload to the HPC portal

Upload the **public** key only, never the private key.

That means:

- upload the contents of the `.pub` file
- or upload the `.pub` file itself if the portal supports file upload
- do **not** upload the private key file

For FAU/NHR, the upload target is:

```text
https://portal.hpc.fau.de/ui/user
```

To view the public key contents:

Linux / macOS:

```bash
cat ~/.ssh/id_fau_hpc_connect_pem.pub
```

Windows PowerShell:

```powershell
Get-Content "$HOME\.ssh\id_fau_hpc_connect_pem.pub"
```

The public key is a single line starting with something like `ssh-rsa ...`.

### Automatic PEM fallback creation

If native SSH is requested and the current key is in OpenSSH private-key format,
HpcConnect automatically tries to create a compatible PEM fallback key next to it.

This compatibility check and auto-creation step are **not part of the default**
Livebook or local bootstrap workflow. They only run when native SSH is explicitly
requested during bootstrap/setup.

The generated key name is:

```text
<original_key_path>_hpc_connect_pem
```

and the matching public key is:

```text
<original_key_path>_hpc_connect_pem.pub
```

When that happens, HpcConnect prints:

- the generated PEM key path
- the public key contents or `.pub` path
- the portal URL
- the instruction to upload the public key and wait for propagation

If bootstrap had to create a new fallback key first, upload the new `.pub` key,
wait for activation on the cluster, and then rerun bootstrap.

If you enable native SSH later for commands such as `start_app/2` or `reconnect/3`,
prepare a compatible PEM key in advance, or run bootstrap/setup once with
`native_ssh: true` so HpcConnect can perform the compatibility preparation there.

### If you want to avoid native SSH entirely

Use the OS SSH path and disable native SSH explicitly where needed:

```elixir
HpcConnect.bootstrap(mode: :local, native_ssh: false, ...)
```

This is a good fallback when your regular OpenSSH key works, but native Erlang
SSH has trouble with key format or proxy handling.

---

## 5. Hugging Face token handling

For gated models such as `meta-llama/*`, provide a Hugging Face token.

Supported ways:

1. `hf_token: "..."` during `bootstrap/1`
2. `HF_TOKEN` or `HUGGINGFACE_HUB_TOKEN` in an `.env` file
3. `HpcConnect.put_hf_token(session, token)` after bootstrap

Examples:

```elixir
boot =
	HpcConnect.prepare_livebook_session(
		cluster: :alex,
		hf_token: System.get_env("HF_TOKEN")
	)
	|> HpcConnect.bootstrap()
```

```elixir
session = HpcConnect.put_hf_token(session, System.get_env("HF_TOKEN"))
```

If `hf_token:` is not passed explicitly, `bootstrap/1` also checks `.env` values such as
`HUGGINGFACE_HUB_TOKEN` and `HF_TOKEN`.

---

## 6. Custom definition files and startup scripts

HpcConnect supports the built-in `vllm` flow, but the app launcher is intentionally more general.

### Definition files

Bundled definition files live in:

```text
priv/def_files/
```

Convention:

- use the stem `<app>`
- save the definition file as:

```text
priv/def_files/<app>.def
```

Examples:

- `priv/def_files/vllm.def`
- `priv/def_files/myservice.def`

Behavior:

- `bootstrap/1` uploads bundled `.def` files by default
- when already connected, you can re-upload them with:

```elixir
HpcConnect.upload_def_file(session)
HpcConnect.upload_def_file(session, "myservice")
```

- then build the image with:

```elixir
HpcConnect.build_sif(session, "myservice")
```

### Startup scripts

Bundled startup scripts live in:

```text
priv/scripts/
```

Convention:

- use the same app stem `<app>`
- save the startup script as:

```text
priv/scripts/start_<app>.sh
```

Examples:

- `priv/scripts/start_vllm.sh`
- `priv/scripts/start_myservice.sh`

Behavior:

- `bootstrap/1` uploads bundled scripts by default
- when already connected, re-upload them with:

```elixir
HpcConnect.install_remote_scripts!(session)
```

### Generic app launch rule

If you use:

```elixir
HpcConnect.start_app(session, app: "myservice", args: [...])
```

then HpcConnect looks for the uploaded remote startup script:

```text
work_dir/scripts/start_myservice.sh
```

and the matching SIF is typically built from:

```text
myservice.def
```

To reconnect to an existing job, use the same app name:

```elixir
HpcConnect.reconnect(session, job_id, app: "myservice", args: [port: 9000])
```

The app name is therefore the shared stem used across:

- `priv/def_files/<app>.def`
- `priv/scripts/start_<app>.sh`
- `build_sif(session, "<app>")`
- `start_app(..., app: "<app>")`
- `reconnect(..., app: "<app>")`

---

## 7. Recommended app workflow

### Start a vLLM job

```elixir
vllm =
	HpcConnect.start_app(session,
		app: "vllm",
		args: [partition: "a40", gpus: 1, walltime: "02:00:00", port: 50200]
	)
```

### Reconnect after notebook or kernel restart

```elixir
[%{job_id: job_id} | _] = HpcConnect.list_jobs_summary(session)

vllm =
	HpcConnect.reconnect(session, job_id,
		app: "vllm",
		args: [port: 50200]
	)
```

### Query the endpoint

```elixir
HpcConnect.vllm_chat(vllm, "Hello from Livebook")
```

---

## 8. Cleanup model

### Normal end of a Livebook session

```elixir
HpcConnect.exit(boot)
```

This is the most complete shutdown helper:

- cancels jobs
- clears app cache
- cleans Livebook credentials

### Only delete current Livebook temp credentials

```elixir
HpcConnect.cleanup_livebook_session(boot)
HpcConnect.cleanup_livebook_session(session)
```

### Recover from interrupted sessions

```elixir
HpcConnect.cleanup_livebook_orphans(delete_uploaded: true)
```

Use this only when the original `boot` or `session` variable is gone.

### Cancel jobs only

```elixir
HpcConnect.cancel_job(session, "3604228")
HpcConnect.cancel_all_jobs(session)
```

`HpcConnect.cancel_job/2` returns `:ok` on success.

---

## 9. Uninstall

If you want to remove HpcConnect-managed files from the remote side, use:

```elixir
HpcConnect.uninstall(boot)
```

This removes the remote `work_dir` content created by HpcConnect and clears remote cache content while keeping downloaded models by default.

To also remove downloaded models:

```elixir
HpcConnect.uninstall(boot, remove_models: true)
```

Use the `remove_models: true` variant carefully, because it removes the model cache too.

---

## 10. SSH and network troubleshooting

### Outbound SSH must be possible

The runtime must be able to open outbound **TCP port 22** to the first reachable jump or login host.

For FAU/NHR setups this typically means the Livebook host must reach the jump host (for example `csnhr.nhr.fau.de`) over SSH.

If outbound SSH is blocked, Livebook mode cannot work reliably.

### IPv4 connectivity matters

In some shared Linux environments, DNS may resolve both IPv4 and IPv6 addresses, but only IPv4 is actually routable. If you see inconsistent SSH behavior, verify that at least the IPv4 path to the jump host works.

### Direct compute-node access is usually not available

This is expected. Compute nodes are often reachable only through the login/jump chain. HpcConnect handles this with generated SSH config and managed tunnel fallback.

### If `start_app` fails but the job is running

Use:

```elixir
HpcConnect.reconnect(session, job_id, app: "vllm", args: [port: 50200])
```

The current `start_app` pipeline already retries through reconnect-style attach logic for transient tunnel failures, but reconnect remains the safest manual recovery tool.

### If helper files are missing remotely

```elixir
HpcConnect.install_remote_scripts!(session)
HpcConnect.upload_def_file(session)
```

### If a Livebook runtime crashed

```elixir
HpcConnect.cleanup_livebook_orphans(delete_uploaded: true)
```

### If a key works locally but not in native Erlang SSH mode

Some OTP builds cannot use OpenSSH private-key format directly for native SSH. In that case, keep OS fallback enabled or use a PEM key as described in section 4.

---

## 11. Windows and Linux portability notes

HpcConnect is intended to work in these combinations:

- Livebook on Linux
- Livebook on Windows
- local mode on Linux
- local mode on Windows

Practical conditions:

- `ssh` and `scp` must exist in `PATH`
- temp-file creation must be allowed
- the runtime must be allowed to keep background SSH tunnel processes alive

If a shared Livebook server blocks outbound SSH, the same notebook may still work locally on your own machine.

---

## 12. Where to look next

- [README](../README.md) for the short overview
- [Command cheat sheet](./commands_cheat_sheet.md) for copy/paste commands
- [Tutorial notebook](../examples/hpc_connect_tutorial.livemd) for the end-to-end Livebook flow
