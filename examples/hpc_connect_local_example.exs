# Load in IEx from the project root:
#
#   iex -S mix
#   iex> c("examples/hpc_connect_local_example.exs")
#   iex> boot = HpcConnectLocalExample.bootstrap()
#   iex> session = boot.session
#
# Keep private values such as your real HPC username in the local `.env`
# file or pass them explicitly at runtime. `.env` is git-ignored in this repo.
#
# Override defaults with explicit values when needed:
#
#   iex> boot = HpcConnectLocalExample.bootstrap(
#   ...>   cluster: :alex,
#   ...>   username: "your_hpc_username",
#   ...>   key_path: Path.expand("~/.ssh/id_fau")
#   ...> )

defmodule HpcConnectLocalExample do
  @moduledoc """
  Local-shell companion example for the Livebook tutorial.

  This file is meant to be loaded into `iex -S mix` so users can run the same
  core HpcConnect commands from a local shell with `mode: :local`.
  """

  @doc """
  Bootstraps a local session.

  Defaults are chosen so a project-local `.env` can provide the username,
  identity file, and optional Hugging Face token.
  """
  @spec bootstrap(keyword()) :: map()
  def bootstrap(overrides \\ []) do
    HpcConnect.bootstrap(default_bootstrap_opts(overrides))
  end

  @doc """
  Returns the default bootstrap options used by `bootstrap/1`.
  """
  @spec default_bootstrap_opts(keyword()) :: keyword()
  def default_bootstrap_opts(overrides \\ []) do
    overrides
    |> Keyword.merge(
      mode: :local,
      cluster: :alex,
      key_path: default_key_path(),
      remote_command: "hostname && whoami",
      env_file: ".env"
    )
    |> maybe_put_username_from_env()
  end

  @doc """
  Expands the default key path for local-shell usage.
  """
  @spec default_key_path() :: binary()
  def default_key_path do
    case System.get_env("HPC_CONNECT_IDENTITY_FILE") do
      nil -> Path.expand("~/.ssh/id_fau")
      "" -> Path.expand("~/.ssh/id_fau")
      path -> Path.expand(path)
    end
  end

  defp maybe_put_username_from_env(opts) do
    case {Keyword.has_key?(opts, :username), System.get_env("HPC_CONNECT_USERNAME")} do
      {true, _value} -> opts
      {false, nil} -> opts
      {false, ""} -> opts
      {false, username} -> Keyword.put(opts, :username, username)
    end
  end

  @doc """
  Extracts the short bootstrap summary shown near the top of the Livebook example.
  """
  @spec bootstrap_summary(map()) :: map()
  def bootstrap_summary(boot) do
    %{
      details: boot.details,
      probe: boot.probe,
      command_preview: boot.command_preview
    }
  end

  @doc """
  Extracts the startup status summary returned by bootstrap.
  """
  @spec startup_summary(map()) :: map()
  def startup_summary(boot) do
    %{
      gpus: boot.gpus,
      models: boot.models,
      quota: boot.quota,
      jobs: boot.jobs
    }
  end

  @doc """
  Default vLLM args matching the Livebook tutorial.
  """
  @spec vllm_args() :: keyword()
  def vllm_args do
    [
      partition: "a40",
      gpus: 1,
      walltime: "02:00:00",
      model: "meta-llama/Llama-3.2-1B-Instruct",
      port: 50_200
    ]
  end
end

# Most important local-shell commands, mirroring the Livebook tutorial:
#
# Bootstrap and summaries
#
#   iex> boot = HpcConnectLocalExample.bootstrap()
#   iex> session = boot.session
#   iex> boot
#   iex> HpcConnectLocalExample.bootstrap_summary(boot)
#   iex> HpcConnectLocalExample.startup_summary(boot)
#
# Status queries
#
#   iex> HpcConnect.available_gpu_summary(session)
#   iex> HpcConnect.list_downloaded_models(session)
#   iex> HpcConnect.list_jobs_summary(session)
#   iex> HpcConnect.quota_summary(session)
#
# Start and use vLLM
#
#   iex> vllm = HpcConnect.start_app(session, app: "vllm", args: HpcConnectLocalExample.vllm_args())
#   iex> HpcConnect.vllm_chat(vllm, "Hello from local IEx")
#   iex> answer = HpcConnect.vllm_chat(vllm, "Can you solve: forall x in {a,b}: (P(x) -> Q(x)) and (Q(x) -> R(x))").answer
#   iex> answer |> String.split("\n") |> Enum.each(&IO.puts/1)
#
# Reconnect to an existing vLLM job after shell restart
#
#   iex> [%{job_id: job_id} | _] = HpcConnect.list_jobs_summary(session)
#   iex> reconnected = HpcConnect.reconnect(session, job_id, app: "vllm", args: [port: 50_200])
#   iex> HpcConnect.vllm_chat(reconnected, "Hello from local IEx")
#
# Model and image preparation
#
#   iex> HpcConnect.download_model(session, "meta-llama/Llama-3.2-1B-Instruct")
#   iex> HpcConnect.build_sif(session, "vllm")
#
# Allocate and release a GPU without starting an app
#
#   iex> alloc = HpcConnect.allocate_gpu(session, partition: "a100", walltime: "01:00:00")
#   iex> HpcConnect.release_gpu(session, alloc)
#   iex> HpcConnect.release_gpu(session, "JOBID")
#
# Remote helper files and paths
#
#   iex> HpcConnect.remote_def_path(session)
#   iex> HpcConnect.remote_sif_path(session)
#   iex> HpcConnect.install_remote_scripts!(session)
#   iex> HpcConnect.upload_def_file(session)
#
# Job control and cleanup
#
#   iex> HpcConnect.cancel_job(session, "JOBID")
#   iex> HpcConnect.cancel_all_jobs(session)
#   iex> HpcConnect.exit(boot)
#   iex> HpcConnect.clear_app_cache(boot)
#
# Uninstall HpcConnect-managed remote files
#
#   iex> HpcConnect.uninstall(boot)
#   iex> HpcConnect.uninstall(boot, remove_models: true)
