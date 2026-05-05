defmodule HpcConnect do
  @moduledoc """
  Public API for building and executing standardized HPC connection workflows.

  The library uses the system OpenSSH tools (`ssh` and `scp`) for portability
  across Windows, Linux, and macOS.
  """

  alias HpcConnect.{
    Cluster,
    Command,
    EnvFile,
    Job,
    Livebook,
    Model,
    Scripts,
    Session,
    Shell,
    Slurm,
    SSH
  }

  @doc """
  Returns the built-in cluster definitions.
  """
  @spec clusters() :: [Cluster.t()]
  def clusters, do: Cluster.defaults()

  @doc """
  Resolves a built-in cluster definition.
  """
  @spec cluster!(atom() | binary()) :: Cluster.t()
  def cluster!(name), do: Cluster.fetch!(name)

  @doc """
  Creates a session for a given cluster.
  """
  @spec new_session(atom() | binary() | Cluster.t(), keyword()) :: Session.t()
  def new_session(cluster, opts \\ []), do: Session.new(cluster, opts)

  @doc """
  Creates a Livebook-friendly session from an uploaded SSH private key.

  The uploaded key is copied into a temporary credential directory together with
  a generated SSH config that includes all built-in cluster hosts.
  """
  @spec new_livebook_session(atom() | binary() | Cluster.t(), binary(), binary(), keyword()) ::
          Session.t()
  def new_livebook_session(cluster, username, uploaded_key_path, opts \\ []) do
    Livebook.new_session(cluster, username, uploaded_key_path, opts)
  end

  @doc """
  One-call Livebook workflow: build session, connect, and return display-ready output.
  """
  @spec livebook_connect(atom() | binary() | Cluster.t(), binary(), binary(), keyword()) :: map()
  def livebook_connect(cluster, username, uploaded_key_path, opts \\ []) do
    Livebook.connect(cluster, username, uploaded_key_path, opts)
  end

  @doc """
  Unified one-call connection setup.

  Modes:
  - `mode: :livebook` builds Kino inputs automatically when required
  - `mode: :local` uses explicit `username` + `key_path` without Kino
  """
  @spec connection_setup(keyword()) :: map()
  def connection_setup(opts \\ []), do: Livebook.connection_setup(opts)

  @doc """
  High-level bootstrap helper for both local and Livebook modes.

  It performs `connection_setup/1`, optionally loads a local `.env` file into the
  runtime session environment, and gathers the most important remote status data
  (quota, downloaded models, free GPUs, jobs).

  Options:
  - `:reset_permission` (default: `false`) – when `true`, explicitly runs
    remote `chmod 755` on hpc_connect working directories during bootstrap.
  """
  @spec bootstrap(keyword()) :: map()
  def bootstrap(opts \\ []) do
    result = connection_setup(opts)
    reset_permission? = Keyword.get(opts, :reset_permission, false)

    session =
      case Keyword.get(opts, :env_file) do
        value when is_binary(value) and value != "" ->
          Session.merge_env_file(result.session, value)

        _ ->
          result.session
      end

    # Auto-open a persistent Erlang :ssh connection when STEADY_SSH_CONNECTION=true
    # (set in .env, as an OS env var, or via :steady_ssh_connection opt).
    session =
      if steady_ssh_connection?(session, opts) do
        {sess, _tunnel} = open_connection!(session)
        sess
      else
        session
      end

    setup_session =
      if Keyword.get(opts, :install_scripts, true) do
        install_remote_scripts!(session, reset_permission: reset_permission?)
        session
      else
        session
      end

    setup_session =
      if Keyword.get(opts, :install_def_files, true) do
        def_dir = Path.join(setup_session.work_dir, "singularity_def_files")
        img_dir = Path.join(setup_session.work_dir, "singularity_images")

        SSH.ssh_command(
          setup_session,
          "mkdir -p #{HpcConnect.Shell.escape(def_dir)} #{HpcConnect.Shell.escape(img_dir)}" <>
            if(reset_permission?,
              do:
                " && chmod 755 #{HpcConnect.Shell.escape(def_dir)} #{HpcConnect.Shell.escape(img_dir)}",
              else: ""
            ),
          "Ensure singularity directories"
        )
        |> run_command_with_retry!()

        upload_def_file(setup_session)
        setup_session
      else
        setup_session
      end

    startup = collect_startup_summary(setup_session, opts)

    Map.merge(result, %{
      session: setup_session,
      startup: startup,
      gpus: startup_value(startup.available_gpus),
      models: startup_value(startup.downloaded_models),
      quota: startup_value(startup.quota),
      jobs: startup_value(startup.jobs)
    })
  end

  defp startup_value(%{ok?: true, value: value}), do: value
  defp startup_value(%{ok?: false}), do: []
  defp startup_value(_), do: []

  defp collect_startup_summary(%Session{ssh_conn: conn} = session, opts) when not is_nil(conn) do
    startup_summary(session, opts)
  end

  defp collect_startup_summary(%Session{} = session, opts) do
    if Keyword.get(opts, :startup_via_connection, false) do
      try do
        {startup_session, _tunnel} = open_connection!(session)

        try do
          startup_summary(startup_session, opts)
        after
          _ = close_connection(startup_session)
        end
      rescue
        _ ->
          startup_summary(session, opts)
      end
    else
      startup_summary(session, opts)
    end
  end

  @doc """
  Convenience cleanup for connection setup results or direct sessions.
  """
  @spec connection_cleanup(map() | Session.t(), keyword()) :: :ok
  def connection_cleanup(result_or_session, opts \\ []),
    do: Livebook.connection_cleanup(result_or_session, opts)

  @doc """
  Creates a model descriptor.
  """
  @spec new_model(binary(), keyword()) :: Model.t()
  def new_model(repo_id, opts \\ []), do: Model.new(repo_id, opts)

  @doc """
  Loads key-value pairs from a local `.env` file.
  """
  @spec load_env_file(binary()) :: map()
  def load_env_file(path \\ ".env"), do: EnvFile.load(path)

  @doc """
  Returns a session with one extra runtime environment variable.
  """
  @spec put_env(Session.t(), binary(), binary()) :: Session.t()
  def put_env(%Session{} = session, key, value), do: Session.put_env(session, key, value)

  @doc """
  Returns a session with values from a local `.env` file merged in.
  """
  @spec merge_env_file(Session.t(), binary()) :: Session.t()
  def merge_env_file(%Session{} = session, path \\ ".env"),
    do: Session.merge_env_file(session, path)

  @doc """
  Convenience helper for setting the Hugging Face token on a session.
  """
  @spec put_hf_token(Session.t(), binary()) :: Session.t()
  def put_hf_token(%Session{} = session, token),
    do: Session.put_env(session, "HUGGINGFACE_HUB_TOKEN", token)

  @doc """
  Creates a job descriptor.
  """
  @spec new_job(Session.t(), keyword()) :: Job.t()
  def new_job(session, opts \\ []), do: Job.new(session, opts)

  @doc """
  Returns a plan of command objects for preparing and starting a remote vLLM job.

  The plan is intentionally explicit, so callers can inspect, modify, or execute
  the individual steps.
  """
  @spec plan(Session.t(), Model.t(), Job.t()) :: map()
  def plan(%Session{} = session, %Model{} = model, %Job{} = job) do
    install_scripts = Scripts.install_commands(session)

    %{
      install_scripts: install_scripts,
      download_model: Scripts.download_model_command(session, model),
      reserve_port: Scripts.find_free_port_command(session, job.port_range),
      export_env: Scripts.export_env_command(session, model),
      start_vllm: Scripts.start_vllm_command(session, model, job)
    }
  end

  @doc """
  Renders a user-friendly preview of a command.
  """
  @spec command_preview(Command.t()) :: binary()
  def command_preview(%Command{binary: binary, args: args}) do
    Enum.join([binary | Enum.map(args, &SSH.preview_arg/1)], " ")
  end

  @doc """
  Builds a direct SSH command for testing connectivity or running a one-off remote command.
  """
  @spec connect_command(Session.t(), binary()) :: Command.t()
  def connect_command(%Session{} = session, remote_command \\ "hostname") do
    SSH.ssh_command(session, remote_command, "Connect to remote cluster")
  end

  @doc """
  Connects to the remote cluster and returns the command output.
  """
  @spec connect(Session.t(), binary(), keyword()) :: {binary(), non_neg_integer()}
  def connect(%Session{} = session, remote_command \\ "hostname", opts \\ []) do
    session
    |> connect_command(remote_command)
    |> run_command(opts)
  end

  @doc """
  Connects to the remote cluster and raises on failure.

  When the session has a persistent `:ssh` connection (set by `open_connection!/1`),
  the command runs over it without spawning any OS process.
  """
  @spec connect!(Session.t(), binary(), keyword()) :: binary()
  def connect!(session, remote_command \\ "hostname", opts \\ [])

  def connect!(%Session{ssh_conn: nil} = session, remote_command, opts) do
    session
    |> connect_command(remote_command)
    |> run_command!(opts)
  end

  def connect!(%Session{} = session, remote_command, opts) do
    SSH.exec!(session, remote_command, opts)
  end

  @doc """
  Removes temporary credential material created for a Livebook session.

  Options:
  - `delete_uploaded: true` additionally removes the original uploaded temp file
    created by Livebook (only when the path matches a safe Livebook temp pattern).
  - `sweep_orphans: true` (default) also cleans orphaned artifacts from interrupted
    sessions via the cleanup registry.
  - `force_uploaded_delete: true` bypasses safe-path checks for uploaded files.
  """
  @spec cleanup_session(Session.t(), keyword()) :: :ok
  def cleanup_session(%Session{} = session, opts \\ []), do: Livebook.cleanup(session, opts)

  @doc """
  Sweeps orphaned Livebook temp artifacts tracked by `hpc_connect`.

  Call this when reconnecting after an interrupted/paused notebook session and
  you no longer have the original `session` variable.
  """
  @spec cleanup_livebook_orphans(keyword()) :: :ok
  def cleanup_livebook_orphans(opts \\ []), do: Livebook.cleanup_orphans(opts)

  @doc """
  Executes a command using `System.cmd/3` and returns its result.
  """
  @spec run_command(Command.t(), keyword()) :: {binary(), non_neg_integer()}
  def run_command(%Command{} = command, opts \\ []) do
    SSH.run(command, opts)
  end

  @doc """
  Executes a command and raises on non-zero exit status.
  """
  @spec run_command!(Command.t(), keyword()) :: binary()
  def run_command!(%Command{} = command, opts \\ []) do
    case run_command(command, opts) do
      {output, 0} ->
        output

      {output, status} ->
        raise RuntimeError,
              "command failed with exit status #{status}: #{command_preview(command)}\n#{output}"
    end
  end

  @transient_errors ["Connection refused", "Connection closed", "connect to host"]

  @doc """
  Like `run_command!/2` but retries up to `retries` times (default 3) with a
  delay between attempts when a transient SSH/SCP connection error is detected.
  Useful for commands that go through a jump host that occasionally drops connections.
  """
  @spec run_command_with_retry!(Command.t(), keyword()) :: binary()
  def run_command_with_retry!(%Command{} = command, opts \\ []) do
    retries = Keyword.get(opts, :retries, 3)
    delay_ms = Keyword.get(opts, :retry_delay_ms, 3_000)
    opts_clean = Keyword.drop(opts, [:retries, :retry_delay_ms])
    do_run_with_retry!(command, opts_clean, retries, delay_ms)
  end

  defp do_run_with_retry!(command, opts, retries_left, delay_ms) do
    run_command!(command, opts)
  rescue
    e in RuntimeError ->
      msg = Exception.message(e)
      transient? = Enum.any?(@transient_errors, &String.contains?(msg, &1))

      if transient? and retries_left > 0 do
        Process.sleep(delay_ms)
        do_run_with_retry!(command, opts, retries_left - 1, delay_ms)
      else
        reraise e, __STACKTRACE__
      end
  end

  # ---------------------------------------------------------------------------
  # SLURM / GPU node helpers
  # ---------------------------------------------------------------------------

  @doc """
  Returns the raw `sinfo` output from the remote cluster.
  """
  @spec sinfo(Session.t(), keyword()) :: binary()
  def sinfo(%Session{} = session, opts \\ []), do: Slurm.query_sinfo(session, opts)

  @doc """
  Parses raw `sinfo` text into a list of node-info maps.
  """
  @spec parse_sinfo(binary()) :: [map()]
  def parse_sinfo(raw), do: Slurm.parse_sinfo(raw)

  @doc """
  One-call: SSH → parse → summarise free/mixed/alloc nodes per partition.

  Returns `{summaries, raw_output}`.
  """
  @spec check_free_nodes(Session.t(), keyword()) :: {[map()], binary()}
  def check_free_nodes(%Session{} = session, opts \\ []),
    do: Slurm.check_free_nodes(session, opts)

  @doc """
  Returns the output of `shownicerquota.pl` for the session user.
  """
  @spec quota(Session.t(), keyword()) :: binary()
  def quota(%Session{} = session, opts \\ []) do
    with_retry(fn -> Slurm.query_quota(session, opts) end, opts)
  end

  @doc """
  Returns parsed quota information from `shownicerquota.pl`.
  """
  @spec quota_summary(Session.t(), keyword()) :: [map()]
  def quota_summary(%Session{} = session, opts \\ []) do
    session |> quota(opts) |> Slurm.parse_quota()
  end

  @doc """
  Builds a shell module-load + conda-activate preamble string.

  Options: `:modules`, `:conda_env`, `:source_bash_profile`.
  """
  @spec module_load_preamble(keyword()) :: binary()
  def module_load_preamble(opts \\ []), do: Slurm.module_load_preamble(opts)

  @doc """
  Returns the default FAU/NHR module names.
  """
  @spec default_modules() :: [binary()]
  def default_modules, do: Slurm.default_modules()

  @doc """
  Returns free/mixed GPU availability summaries by partition.
  """
  @spec available_gpus(Session.t(), keyword()) :: {[map()], binary()}
  def available_gpus(%Session{} = session, opts \\ []) do
    with_retry(fn -> Slurm.available_gpus(session, opts) end, opts)
  end

  @doc """
  Returns only the parsed GPU availability summary by partition.
  """
  @spec available_gpu_summary(Session.t(), keyword()) :: [map()]
  def available_gpu_summary(%Session{} = session, opts \\ []) do
    {summary, _raw} = available_gpus(session, opts)
    summary
  end

  @doc """
  Returns the models root directory under the vault path.
  """
  @spec models_root(Session.t()) :: binary()
  def models_root(%Session{} = session), do: Model.models_root(session)

  @doc """
  Lists downloaded model directories in the remote vault models directory.
  """
  @spec list_downloaded_models(Session.t(), keyword()) :: [map()]
  def list_downloaded_models(%Session{} = session, opts \\ []) do
    with_retry(
      fn ->
        {output, status} =
          session
          |> Model.list_remote_command()
          |> run_command(Keyword.get(opts, :connect_opts, []))

        if status != 0 do
          raise RuntimeError, "listing downloaded models failed: #{output}"
        end

        Model.parse_remote_listing(output)
      end,
      opts
    )
  end

  @doc """
  Uploads helper scripts (if needed) and downloads a model snapshot to the
  remote models directory.

  By default this requires `HUGGINGFACE_HUB_TOKEN` to be available either in
  the session env or the local process env. You can set it via `put_hf_token/2`
  or by merging a local `.env` file.
  """
  @spec download_model(Session.t(), Model.t() | binary(), keyword()) :: binary()
  def download_model(session, model_or_repo, opts \\ [])

  def download_model(%Session{} = session, %Model{} = model, opts) do
    if model.hf_token_env && !Session.fetch_env(session, model.hf_token_env) do
      raise ArgumentError,
            "missing required #{model.hf_token_env} value; set it via put_hf_token/2, put_env/3, or merge_env_file/2"
    end

    command =
      Scripts.download_model_command(session, model,
        modules: Keyword.get(opts, :modules, ["python/3.12-conda"]),
        conda_env: Keyword.get(opts, :conda_env, "hpc_connect"),
        source_bash_profile: Keyword.get(opts, :source_bash_profile, false)
      )

    case run_command(command) do
      {output, 0} ->
        String.trim(output)

      {output, _status} when is_binary(output) ->
        if script_missing?(output, "download_model.sh") do
          install_remote_scripts!(session)
          command |> run_command!() |> String.trim()
        else
          raise RuntimeError,
                "command failed: #{command_preview(command)}\n#{output}"
        end
    end
  end

  def download_model(%Session{} = session, repo_id, opts) when is_binary(repo_id) do
    model = new_model(repo_id, Keyword.get(opts, :model_opts, []))
    download_model(session, model, opts)
  end

  @doc """
  Allocates a GPU node via `sbatch --wrap="sleep <secs>"` and returns once the job
  is RUNNING. No OS process or CMD window is opened — works in Livebook.

  Returns `%{job_id, node, partition, gpus, walltime}`.

  Release with `release_gpu/2` (runs `scancel <job_id>`).
  """
  @spec allocate_gpu(Session.t(), keyword()) :: map()
  def allocate_gpu(%Session{} = session, opts \\ []), do: Slurm.allocate_gpu(session, opts)

  @doc """
  Returns the output of `squeue -u <username>` for the current session user.
  """
  @spec list_jobs(Session.t(), keyword()) :: binary()
  def list_jobs(%Session{} = session, opts \\ []) do
    with_retry(fn -> Slurm.query_jobs(session, opts) end, opts)
  end

  @doc """
  Returns parsed job table entries for the current session user.
  """
  @spec list_jobs_summary(Session.t(), keyword()) :: [map()]
  def list_jobs_summary(%Session{} = session, opts \\ []) do
    session |> list_jobs(opts) |> Slurm.parse_jobs()
  end

  @doc """
  Cancels a SLURM job by ID.
  """
  @spec cancel_job(Session.t(), binary() | pos_integer(), keyword()) :: binary()
  def cancel_job(%Session{} = session, job_id, opts \\ []),
    do: Slurm.cancel_job(session, job_id, opts)

  @doc """
  Gathers the most relevant remote status information for a connected session.
  """
  @spec startup_summary(Session.t(), keyword()) :: map()
  def startup_summary(%Session{} = session, opts \\ []) do
    %{
      models_dir: models_root(session),
      quota: safe_call(fn -> quota_summary(session, opts) end),
      available_gpus: safe_call(fn -> available_gpu_summary(session, opts) end),
      downloaded_models: safe_call(fn -> list_downloaded_models(session, opts) end),
      jobs: safe_call(fn -> list_jobs_summary(session, opts) end)
    }
  end

  @doc """
  Uploads the bundled helper scripts to the remote `work_dir/scripts/` directory.
  Normally you don't need this; script uploads happen automatically when a
  remote script is missing.

  Options:
  - `:reset_permission` (default: `false`) – when `true`, runs `chmod 755`
    on remote hpc_connect directories after upload.
  """
  @spec install_remote_scripts!(Session.t(), keyword()) :: :ok
  def install_remote_scripts!(session, opts \\ [])

  def install_remote_scripts!(%Session{ssh_conn: nil} = session, opts) do
    session
    |> Scripts.install_commands(opts)
    |> Enum.each(&run_command_with_retry!/1)

    :ok
  end

  def install_remote_scripts!(%Session{} = session, opts) do
    scripts_dir = Path.join([:code.priv_dir(:hpc_connect), "scripts"])
    remote_dir = session.work_dir <> "/scripts"
    SSH.upload!(session, scripts_dir, remote_dir, recursive: true)

    if Keyword.get(opts, :reset_permission, false) do
      SSH.exec!(
        session,
        "chmod 755 #{HpcConnect.Shell.escape(session.work_dir)} #{HpcConnect.Shell.escape(remote_dir)}",
        []
      )
    end

    :ok
  end

  @doc """
  Submits a vLLM job via `sbatch` and returns a map with job_id and parameters.

  Options: `:partition`, `:gpus`, `:walltime`, `:port`, `:cpus_per_task`, `:modules`, `:conda_env`.
  """
  @spec submit_vllm(Session.t(), Model.t() | binary(), keyword()) :: map()
  def submit_vllm(session, model_or_repo, opts \\ [])

  def submit_vllm(%Session{} = session, %Model{} = model, opts) do
    Slurm.submit_vllm_job(session, model, opts)
  end

  def submit_vllm(%Session{} = session, repo_id, opts) when is_binary(repo_id) do
    submit_vllm(session, new_model(repo_id, Keyword.get(opts, :model_opts, [])), opts)
  end

  @doc """
  Returns the remote path for a Singularity definition file.

  `name` defaults to `"vllm"`. Files live under `<work_dir>/singularity_def_files/<name>.def`.
  """
  @spec remote_def_path(Session.t(), binary()) :: binary()
  def remote_def_path(%Session{} = session, name \\ "vllm"),
    do: Slurm.remote_def_path(session, name)

  @doc """
  Returns the remote path for a built Singularity/Apptainer SIF image.

  Images live under `<work_dir>/singularity_images/<name>.sif`.
  """
  @spec remote_sif_path(Session.t(), binary()) :: binary()
  def remote_sif_path(%Session{} = session, name \\ "vllm"),
    do: Slurm.remote_sif_path(session, name)

  @doc """
  Uploads a local Singularity definition file to the remote cluster.

  `name` is the stem (e.g. `"vllm"`). Uses bundled `priv/def_files/*.def`
  by default and uploads all available def files; pass `local_def_path` to
  override. Returns the remote path for `name`.
  """
  @spec upload_def_file(Session.t(), binary(), binary() | nil) :: binary()
  def upload_def_file(%Session{} = session, name \\ "vllm", local_def_path \\ nil),
    do: Slurm.upload_def_file(session, name, local_def_path)

  @doc """
  Submits an Apptainer build job via `sbatch` (CPU-only, no GPU needed).

  Uploads the `.def` file, then submits `build_sif.sh`. Returns immediately with
  `%{job_id, sif_path, def_path, name}`. Poll with `squeue` or use
  `build_sif_blocking/2` to wait for completion.

  Options: `:name`, `:local_def_path`, `:partition`, `:walltime`, `:cpus`,
  `:force_rebuild`, `:timeout`, `:interval`,
  `:build_cluster` – route the build through a different cluster that supports
  user namespaces (default: `:woody`; AlmaLinux login nodes support apptainer
  builds, Ubuntu-based systems like TinyX do not). Pass `build_cluster: nil`
  to build on the session's own cluster.
  `:apptainer_tmpdir` – override apptainer's tmp/cache directory. Unset by default;
  apptainer manages its own cache under `~/.apptainer` on the remote node, which
  means `apptainer cache clean` works naturally. Set only if you need to redirect
  the cache to a specific path.
  """
  @spec build_sif_job(Session.t(), keyword() | binary()) :: map()
  def build_sif_job(session, opts \\ [])

  def build_sif_job(%Session{} = session, name) when is_binary(name),
    do: Slurm.build_sif_job(session, name)

  def build_sif_job(%Session{} = session, opts),
    do: Slurm.build_sif_job(session, opts)

  @doc """
  Uploads the `.def` file, submits the build job, then **blocks** until the SIF
  is ready and returns its remote path.

  Supports either a name string or options list.

  Options: same as `build_sif_job/2` plus `:timeout` (ms, default 3_600_000 = 1 h),
  `:interval` (ms between polls, default 15_000).
  """
  @spec build_sif_blocking(Session.t(), binary() | keyword(), keyword()) :: binary()
  def build_sif_blocking(session, name_or_opts \\ [], opts \\ [])

  def build_sif_blocking(%Session{} = session, name, opts) when is_binary(name) and is_list(opts),
    do: Slurm.build_sif_blocking(session, [name: name] ++ opts)

  def build_sif_blocking(%Session{} = session, opts, []) when is_list(opts),
    do: Slurm.build_sif_blocking(session, opts)

  @doc """
  Builds a Singularity/Apptainer image and waits for completion.

  Supports both forms:
  - `build_sif(session, "vllm")`
  - `build_sif(session, "vllm", force_rebuild: true)`
  - `build_sif(session, name: "vllm", force_rebuild: true)`
  """
  @spec build_sif(Session.t(), binary() | keyword(), keyword()) :: binary()
  def build_sif(session, name_or_opts \\ [], opts \\ [])

  def build_sif(%Session{} = session, name, opts) when is_binary(name) and is_list(opts),
    do: build_sif_blocking(session, [name: name] ++ opts)

  def build_sif(%Session{} = session, opts, []) when is_list(opts),
    do: build_sif_blocking(session, opts)

  @doc """
  Submits a vLLM inference job using a pre-built Apptainer SIF image.

  All paths and variables are injected as `--export` into `sbatch`. No CMD window.
  Returns `%{job_id, node: nil, partition, gpus, walltime, port, sif_path, logs_dir}`.

  Use `wait_for_job_node/3` to block until the node is assigned, then
  `start_proxy/3` + `open_proxy!/1` to forward the port to localhost.

  Options: `:partition`, `:gpus`, `:walltime`, `:port`, `:cpus`, `:sif_name`,
  `:sif_path`, `:tensor_parallel`, `:gpu_mem_util`, `:max_model_len`, `:hf_token`.
  """
  @spec submit_vllm_apptainer(Session.t(), Model.t() | binary(), keyword()) :: map()
  def submit_vllm_apptainer(session, model_or_repo, opts \\ [])

  def submit_vllm_apptainer(%Session{} = session, %Model{} = model, opts),
    do: Slurm.submit_vllm_apptainer(session, model, opts)

  def submit_vllm_apptainer(%Session{} = session, repo_id, opts) when is_binary(repo_id),
    do:
      submit_vllm_apptainer(session, new_model(repo_id, Keyword.get(opts, :model_opts, [])), opts)

  @doc """
  Submits a generalized Apptainer application job via `sbatch`.

  The app name is used to auto-discover the startup script at `work_dir/scripts/start_<app>.sh`.
  All dynamic variables are injected as `--export` into sbatch.

  Returns `%{job_id, node, partition, gpus, walltime, port, sif_path, logs_dir}`.
  The `node` field is populated after up to 10 seconds of polling; if still nil,
  the job is waiting for resource availability.

  Options:
  - `:app`           – application name (required; used to find start_<app>.sh)
  - `:partition`     – GPU partition (default: cluster default or `"a100"`)
  - `:gpus`          – number of GPUs (default: `1`)
  - `:walltime`      – time limit (default: `"02:00:00"`)
  - `:port`          – app port (default: `8000`)
  - `:cpus`          – cpus-per-task (default: `8`)
  - `:sif_name`      – stem name of the .sif (default: app name)
  - `:sif_path`      – override full remote sif path
  - `:app_env`       – extra environment variables (map, default: `%{}`)

  Example:
      HpcConnect.submit_apptainer(session, app: "vllm", gpus: 2, port: 8080)
      HpcConnect.submit_apptainer(session, app: "myservice", app_env: %{"VAR" => "value"})
  """
  @spec submit_apptainer(Session.t(), keyword()) :: map()
  def submit_apptainer(%Session{} = session, opts \\ []),
    do: Slurm.submit_apptainer(session, opts)

  @doc """
  High-level app launcher. Submits a `start_<app>.sh` script via sbatch.

  All arguments can be passed as a flat `args:` list. SLURM scheduling keys
  (`partition`, `gpus`, `walltime`, `cpus`, `sif_name`, `sif_path`) are forwarded
  to sbatch; all remaining keys become `<APP_UPPER>_<KEY_UPPER>` environment
  variables inside the job script (e.g. `model:` → `VLLM_MODEL`). The `port`
  value is also passed as `APP_PORT` for the generic fallback in scripts.

  The legacy two-list form (`slurm: [...]`, `config: [...]`) is still accepted
  for backward compatibility.

  Returns `%{job_id, node, partition, gpus, walltime, port, sif_path, logs_dir}`.
  Retries silently on transient SSH connection errors (up to 3 attempts).

  By default this call waits until a compute node is assigned, polling every 10s.
  While pending, it prints `"Waiting for GPU allocation ..."`. Pressing Ctrl+C
  attempts to auto-cancel the submitted job (`release_gpu(session, job_id)`) before
  re-raising the interrupt.

  Optional wait controls:
  - `:wait_for_node` (default: `true`)
  - `:node_poll_interval_ms` (default: `10_000`)
  - `:node_poll_timeout_ms` (default: `:infinity`)
  - `:cancel_on_interrupt` (default: `true`)

  vLLM convenience controls (`app: "vllm"` only):
  - `:native_ssh` (default: `true`) ensures a persistent native `:ssh` session
  - `:auto_proxy` (default: `false`) opens a local OpenSSH tunnel (legacy mode)
  - `:local_port` optional fixed local tunnel port
  - `:wait_for_app` (default: `true`) waits for `/v1/models` to become reachable
  - `:app_ready_timeout_ms` (default: `600_000`)
  - `:app_ready_interval_ms` (default: `5_000`)

  Example:
      HpcConnect.start_app(session,
        app: "vllm",
        args: [partition: "a100", gpus: 1, walltime: "02:00:00",
               model: "meta-llama/Llama-3.2-1B-Instruct", port: 50200]
      )
  """
  @slurm_keys [:partition, :gpus, :walltime, :cpus, :sif_name, :sif_path]

  @spec start_app(Session.t(), keyword()) :: map()
  def start_app(%Session{} = session, opts) do
    app = Keyword.fetch!(opts, :app)
    session = maybe_prepare_session_for_app(session, app, opts)
    app_upper = app |> to_string() |> String.upcase()

    # Support both flat `args:` and legacy `slurm:` + `config:` forms.
    {slurm_kw, config_kw} =
      case Keyword.fetch(opts, :args) do
        {:ok, args} ->
          {Enum.filter(args, fn {k, _} -> k in @slurm_keys end),
           Enum.reject(args, fn {k, _} -> k in @slurm_keys end)}

        :error ->
          slurm = Keyword.get(opts, :slurm, [])
          config = Keyword.get(opts, :config, [])
          {slurm, config}
      end

    port = Keyword.get(config_kw, :port, Keyword.get(slurm_kw, :port, 8000))

    app_env =
      Map.new(config_kw, fn {k, v} ->
        {"#{app_upper}_#{k |> to_string() |> String.upcase()}", v}
      end)

    slurm_opts =
      [app: app, port: port, app_env: app_env] ++
        Enum.flat_map(@slurm_keys, &maybe_opt(slurm_kw, &1))

    submitted = with_retry(fn -> submit_apptainer(session, slurm_opts) end, opts)

    submitted
    |> maybe_wait_for_allocation(session, opts)
    |> maybe_attach_vllm_access(session, app, opts)
  end

  defp maybe_prepare_session_for_app(%Session{} = session, app, opts) do
    app_name = app |> to_string() |> String.downcase()

    if app_name == "vllm" and Keyword.get(opts, :native_ssh, true) and is_nil(session.ssh_conn) do
      try do
        {updated, _tunnel} =
          open_connection!(session,
            timeout: Keyword.get(opts, :ssh_timeout_ms, 20_000),
            attempts: Keyword.get(opts, :ssh_attempts, 3)
          )

        updated
      rescue
        e in RuntimeError ->
          if Keyword.get(opts, :native_ssh_fallback_to_os, true) do
            IO.puts(
              "Native Erlang SSH unavailable (#{Exception.message(e)}). " <>
                "Falling back to managed OpenSSH commands/tunnel ..."
            )

            session
          else
            reraise e, __STACKTRACE__
          end
      end
    else
      session
    end
  end

  defp maybe_wait_for_allocation(%{job_id: job_id} = submitted, _session, _opts)
       when job_id in [nil, ""] do
    submitted
  end

  defp maybe_wait_for_allocation(submitted, %Session{} = session, opts) do
    if Keyword.get(opts, :wait_for_node, true) do
      interval_ms = Keyword.get(opts, :node_poll_interval_ms, 10_000)
      timeout_ms = Keyword.get(opts, :node_poll_timeout_ms, :infinity)
      cancel_on_interrupt? = Keyword.get(opts, :cancel_on_interrupt, true)

      IO.puts("Waiting for GPU allocation ...")

      try do
        node =
          wait_for_node_with_progress(session, submitted.job_id,
            interval_ms: interval_ms,
            timeout_ms: timeout_ms,
            connect_opts: Keyword.get(opts, :connect_opts, [])
          )

        %{submitted | node: node}
      rescue
        e ->
          if cancel_on_interrupt?, do: safe_release_gpu(session, submitted.job_id)
          reraise e, __STACKTRACE__
      catch
        kind, value ->
          if cancel_on_interrupt?, do: safe_release_gpu(session, submitted.job_id)

          case kind do
            :exit -> exit(value)
            :throw -> throw(value)
            :error -> :erlang.error(value)
          end
      end
    else
      submitted
    end
  end

  defp maybe_attach_vllm_access(submitted, _session, app, _opts)
       when not is_binary(app) and not is_atom(app) do
    submitted
  end

  defp maybe_attach_vllm_access(submitted, %Session{} = session, app, opts) do
    app_name = app |> to_string() |> String.downcase()

    if app_name == "vllm" do
      submitted = Map.put_new(submitted, :session, session)

      node =
        submitted.node ||
          wait_for_job_node(session, submitted.job_id,
            timeout: Keyword.get(opts, :proxy_wait_timeout_ms, 600_000),
            interval: Keyword.get(opts, :node_poll_interval_ms, 10_000)
          )

      native_enriched =
        submitted
        |> Map.put(:node, node)
        |> Map.put(:session, session)
        |> Map.put(:port, submitted.port)
        |> Map.put(:base_url, "http://#{node}:#{submitted.port}")
        |> Map.put(:access_mode, :native_ssh)

      use_proxy? = Keyword.get(opts, :auto_proxy, false) or is_nil(session.ssh_conn)

      if use_proxy? do
        proxy =
          start_proxy(session, node, remote_port: submitted.port, local_port: opts[:local_port])

        tunnel_port = open_proxy!(proxy)

        enriched =
          native_enriched
          |> Map.put(:proxy, proxy)
          |> Map.put(:tunnel_port, tunnel_port)
          |> Map.put(:base_url, proxy.base_url)
          |> Map.put(:access_mode, :openssh_proxy)

        maybe_wait_for_vllm_ready(enriched, opts)
      else
        maybe_wait_for_vllm_ready(native_enriched, opts)
      end
    else
      submitted
    end
  end

  defp wait_for_node_with_progress(%Session{} = session, job_id, opts) do
    interval_ms = Keyword.get(opts, :interval_ms, 10_000)
    timeout_ms = Keyword.get(opts, :timeout_ms, :infinity)
    connect_opts = Keyword.get(opts, :connect_opts, [])

    deadline =
      case timeout_ms do
        :infinity ->
          :infinity

        nil ->
          :infinity

        value when is_integer(value) and value > 0 ->
          System.monotonic_time(:millisecond) + value

        _ ->
          System.monotonic_time(:millisecond)
      end

    do_wait_for_node_with_progress(session, job_id, interval_ms, deadline, connect_opts)
  end

  defp do_wait_for_node_with_progress(session, job_id, interval_ms, deadline, connect_opts) do
    case get_job_node(session, job_id, connect_opts: connect_opts) do
      nil ->
        if deadline == :infinity or System.monotonic_time(:millisecond) < deadline do
          IO.puts("Waiting for GPU allocation ...")
          Process.sleep(interval_ms)
          do_wait_for_node_with_progress(session, job_id, interval_ms, deadline, connect_opts)
        else
          raise RuntimeError,
                "Job #{job_id} did not receive a compute node within the configured timeout"
        end

      node ->
        node
    end
  end

  defp safe_release_gpu(%Session{} = session, job_id) do
    IO.puts("Interrupt received — releasing pending GPU job #{job_id} ...")
    _ = release_gpu(session, job_id)
    :ok
  rescue
    _ -> :ok
  end

  defp maybe_opt(kw, key) do
    case Keyword.fetch(kw, key) do
      {:ok, val} -> [{key, val}]
      :error -> []
    end
  end

  defp maybe_wait_for_vllm_ready(%{base_url: base_url} = app, opts) when is_binary(base_url) do
    if Keyword.get(opts, :wait_for_app, true) do
      timeout_ms = Keyword.get(opts, :app_ready_timeout_ms, 600_000)
      interval_ms = Keyword.get(opts, :app_ready_interval_ms, 5_000)

      request_fun =
        case app do
          %{
            access_mode: :native_ssh,
            session: %Session{} = session,
            node: node,
            port: port,
            job_id: job_id
          } ->
            Keyword.get(opts, :request_fun, ssh_http_request_fun(session, node, port, job_id))

          %{access_mode: :native_ssh, session: %Session{} = session, node: node, port: port} ->
            Keyword.get(opts, :request_fun, ssh_http_request_fun(session, node, port, nil))

          _ ->
            Keyword.get(opts, :request_fun, &default_http_request/5)
        end

      IO.puts("Waiting for vLLM API readiness ...")
      wait_for_vllm_ready!(base_url, timeout_ms, interval_ms, request_fun)
      app
    else
      app
    end
  end

  defp maybe_wait_for_vllm_ready(app, _opts), do: app

  defp wait_for_vllm_ready!(base_url, timeout_ms, interval_ms, request_fun) do
    deadline =
      case timeout_ms do
        :infinity -> :infinity
        value when is_integer(value) and value > 0 -> System.monotonic_time(:millisecond) + value
        _ -> System.monotonic_time(:millisecond)
      end

    do_wait_for_vllm_ready(base_url, interval_ms, deadline, request_fun)
  end

  defp do_wait_for_vllm_ready(base_url, interval_ms, deadline, request_fun) do
    url = normalize_base_url(base_url) <> "/v1/models"

    case request_fun.(:get, url, [], nil, max(interval_ms, 2_000)) do
      {:ok, status, body, _headers} when status in 200..299 ->
        case Jason.decode(body) do
          {:ok, %{"data" => [_ | _]}} -> :ok
          _ -> wait_or_timeout(base_url, interval_ms, deadline, request_fun)
        end

      _ ->
        wait_or_timeout(base_url, interval_ms, deadline, request_fun)
    end
  end

  defp wait_or_timeout(base_url, interval_ms, deadline, request_fun) do
    if deadline == :infinity or System.monotonic_time(:millisecond) < deadline do
      IO.puts("Waiting for vLLM API readiness ...")
      Process.sleep(interval_ms)
      do_wait_for_vllm_ready(base_url, interval_ms, deadline, request_fun)
    else
      raise RuntimeError,
            "vLLM API did not become ready in time at #{normalize_base_url(base_url)}/v1/models"
    end
  end

  @doc """
  Returns the compute node for a SLURM job, or `nil` if not yet running.
  """
  @spec get_job_node(Session.t(), binary() | pos_integer(), keyword()) :: binary() | nil
  def get_job_node(%Session{} = session, job_id, opts \\ []),
    do: Slurm.get_job_node(session, job_id, opts)

  @doc """
  Polls `squeue` until the job is RUNNING and returns the compute node.

  Options: `:timeout` (ms, default 300_000), `:interval` (ms, default 10_000).
  """
  @spec wait_for_job_node(Session.t(), binary() | pos_integer(), keyword()) :: binary()
  def wait_for_job_node(%Session{} = session, job_id, opts \\ []),
    do: Slurm.wait_for_job_node(session, job_id, opts)

  @doc """
  Builds SSH port-forwarding info for tunnelling to a compute node.

  Finds a free local port automatically unless `:local_port` is given.
  Returns a map with `:local_port`, `:remote_port`, `:node`, `:base_url`, and
  a `:command` (`%Command{}`) ready to pass to `open_proxy!/1`.

  Options: `:local_port`, `:remote_port` (default `8000`).
  """
  @spec start_proxy(Session.t(), binary(), keyword()) :: map()
  def start_proxy(%Session{} = session, node, opts \\ []) do
    local_port = Keyword.get(opts, :local_port) || SSH.find_free_local_port()
    remote_port = Keyword.get(opts, :remote_port, 8000)
    command = SSH.port_forward_command(session, node, local_port, remote_port)

    %{
      local_port: local_port,
      remote_port: remote_port,
      node: node,
      base_url: "http://localhost:#{local_port}",
      preview: command_preview(command),
      command: command
    }
  end

  @doc """
  Opens the SSH tunnel as a background OS process (supervised by the calling process).

  Returns an Erlang `Port`. The tunnel stays alive as long as the process lives.
  Close it with `Port.close(port)` or kill the underlying OS process.
  """
  @spec open_proxy!(map()) :: port()
  def open_proxy!(%{command: %Command{binary: binary, args: args}, local_port: local_port}) do
    port =
      Port.open({:spawn_executable, binary}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        args: args
      ])

    wait_for_local_proxy_port!(port, local_port, 15_000)
    port
  end

  def open_proxy!(%{command: %Command{binary: binary, args: args}}) do
    Port.open({:spawn_executable, binary}, [:binary, :exit_status, :stderr_to_stdout, args: args])
  end

  defp wait_for_local_proxy_port!(port, local_port, timeout_ms, waited \\ 0)

  defp wait_for_local_proxy_port!(_port, local_port, timeout_ms, waited)
       when waited >= timeout_ms do
    raise RuntimeError,
          "SSH proxy did not open localhost:#{local_port} within #{timeout_ms}ms"
  end

  defp wait_for_local_proxy_port!(port, local_port, timeout_ms, waited) do
    case :gen_tcp.connect(~c"127.0.0.1", local_port, [:binary, {:active, false}], 500) do
      {:ok, sock} ->
        :gen_tcp.close(sock)
        :ok

      {:error, _reason} ->
        if Port.info(port) == nil do
          raise RuntimeError,
                "SSH proxy process exited before localhost:#{local_port} became reachable"
        else
          Process.sleep(200)
          wait_for_local_proxy_port!(port, local_port, timeout_ms, waited + 200)
        end
    end
  end

  @doc """
  Sends a chat completion request to a running vLLM OpenAI-compatible endpoint.

  `app_or_url` can be:
  - the map returned by `start_app/2` (for `app: "vllm"`), or
  - a base URL string (e.g. `"http://localhost:58886"`).

  Options:
  - `:model` (optional; auto-detected from `/v1/models` when omitted)
  - `:max_tokens` (default: `256`)
  - `:temperature` (default: `0.2`)
  - `:system` (optional system instruction)
  - `:timeout` ms for HTTP calls (default: `60_000`)
  - `:stream` (default: `false`)

  Returns a map containing at least `:content`, `:model`, and `:base_url`.
  """
  @spec vllm_chat(map() | binary(), binary(), keyword()) :: map()
  def vllm_chat(app_or_url, message, opts \\ []) when is_binary(message) do
    {base_url, request_fun} = resolve_vllm_endpoint(app_or_url, opts)
    timeout_ms = Keyword.get(opts, :timeout, 60_000)
    stream? = Keyword.get(opts, :stream, false)

    model = Keyword.get(opts, :model) || fetch_first_model!(base_url, timeout_ms, request_fun)
    max_tokens = Keyword.get(opts, :max_tokens, 256)
    temperature = Keyword.get(opts, :temperature, 0.2)
    system = Keyword.get(opts, :system)

    messages =
      case system do
        value when is_binary(value) and value != "" ->
          [
            %{role: "system", content: value},
            %{role: "user", content: message}
          ]

        _ ->
          [%{role: "user", content: message}]
      end

    payload = %{
      model: model,
      messages: messages,
      max_tokens: max_tokens,
      temperature: temperature,
      stream: stream?
    }

    body = Jason.encode!(payload)
    url = normalize_base_url(base_url) <> "/v1/chat/completions"

    case request_fun.(:post, url, [{"content-type", "application/json"}], body, timeout_ms) do
      {:ok, status, response_body, _headers} when status in 200..299 ->
        if stream? do
          content = parse_stream_content(response_body)

          %{
            ok?: true,
            stream: true,
            base_url: normalize_base_url(base_url),
            model: model,
            content: content,
            raw: response_body
          }
        else
          decoded = Jason.decode!(response_body)

          content =
            get_in(decoded, ["choices", Access.at(0), "message", "content"]) ||
              get_in(decoded, ["choices", Access.at(0), "text"]) || ""

          %{
            ok?: true,
            stream: false,
            base_url: normalize_base_url(base_url),
            model: model,
            content: content,
            finish_reason: get_in(decoded, ["choices", Access.at(0), "finish_reason"]),
            raw: decoded
          }
        end

      {:ok, status, response_body, _headers} ->
        raise RuntimeError,
              "vLLM chat request failed (HTTP #{status}): #{truncate_http_body(response_body)}"

      {:error, reason} ->
        raise RuntimeError, "vLLM chat request failed: #{inspect(reason)}"
    end
  end

  defp resolve_vllm_endpoint(
         %{
           access_mode: :native_ssh,
           session: %Session{} = session,
           node: node,
           port: port,
           job_id: job_id
         },
         opts
       ) do
    {
      "http://#{node}:#{port}",
      Keyword.get(opts, :request_fun, ssh_http_request_fun(session, node, port, job_id))
    }
  end

  defp resolve_vllm_endpoint(
         %{access_mode: :native_ssh, session: %Session{} = session, node: node, port: port},
         opts
       ) do
    {
      "http://#{node}:#{port}",
      Keyword.get(opts, :request_fun, ssh_http_request_fun(session, node, port, nil))
    }
  end

  defp resolve_vllm_endpoint(%{session: %Session{} = session, node: node, port: port}, opts)
       when is_binary(node) do
    {
      "http://#{node}:#{port}",
      Keyword.get(opts, :request_fun, ssh_http_request_fun(session, node, port, nil))
    }
  end

  defp resolve_vllm_endpoint(%{base_url: base_url}, opts) when is_binary(base_url) do
    {base_url, Keyword.get(opts, :request_fun, &default_http_request/5)}
  end

  defp resolve_vllm_endpoint(%{proxy: %{base_url: base_url}}, opts) when is_binary(base_url) do
    {base_url, Keyword.get(opts, :request_fun, &default_http_request/5)}
  end

  defp resolve_vllm_endpoint(base_url, opts) when is_binary(base_url) do
    {base_url, Keyword.get(opts, :request_fun, &default_http_request/5)}
  end

  defp resolve_vllm_endpoint(other, _opts) do
    raise ArgumentError,
          "vllm_chat expects a vLLM app map or a base URL string, got: #{inspect(other)}"
  end

  defp ssh_http_request_fun(%Session{} = session, node, remote_port, job_id) do
    fn method, url, headers, body, timeout_ms ->
      ssh_http_request(session, node, remote_port, job_id, method, url, headers, body, timeout_ms)
    end
  end

  defp ssh_http_request(
         %Session{} = session,
         node,
         remote_port,
         job_id,
         method,
         url,
         headers,
         body,
         timeout_ms
       ) do
    uri = URI.parse(url)
    path = if uri.path in [nil, ""], do: "/", else: uri.path
    path = if uri.query, do: path <> "?" <> uri.query, else: path
    target_url = "http://#{node}:#{remote_port}#{path}"

    method_arg = "-X #{method |> to_string() |> String.upcase()}"

    header_args =
      headers
      |> Enum.map(fn {k, v} -> "-H #{Shell.escape("#{k}: #{v}")}" end)
      |> Enum.join(" ")

    data_arg =
      case body do
        value when is_binary(value) and value != "" -> "--data-binary #{Shell.escape(value)}"
        _ -> ""
      end

    timeout_s = max(div(timeout_ms + 999, 1_000), 1)

    cmd =
      [
        "curl -sS",
        "--connect-timeout #{min(timeout_s, 30)}",
        "--max-time #{timeout_s}",
        method_arg,
        header_args,
        data_arg,
        "-w '__HPC_STATUS__:%{http_code}'",
        Shell.escape(target_url)
      ]
      |> Enum.reject(&(&1 == ""))
      |> Enum.join(" ")

    case SSH.exec(session, cmd, timeout: timeout_ms + 5_000) do
      {output, 0} ->
        parse_curl_wrapped_response(output)

      {_output, _status} when job_id in [nil, ""] ->
        {:error, :direct_node_http_failed}

      {_output, _status} ->
        ssh_http_request_via_srun(
          session,
          remote_port,
          job_id,
          method,
          url,
          headers,
          body,
          timeout_ms
        )
    end
  end

  defp ssh_http_request_via_srun(
         %Session{} = session,
         remote_port,
         job_id,
         method,
         url,
         headers,
         body,
         timeout_ms
       ) do
    uri = URI.parse(url)
    path = if uri.path in [nil, ""], do: "/", else: uri.path
    path = if uri.query, do: path <> "?" <> uri.query, else: path
    target_url = "http://127.0.0.1:#{remote_port}#{path}"

    method_arg = "-X #{method |> to_string() |> String.upcase()}"

    header_args =
      headers
      |> Enum.map(fn {k, v} -> "-H #{Shell.escape("#{k}: #{v}")}" end)
      |> Enum.join(" ")

    data_arg =
      case body do
        value when is_binary(value) and value != "" -> "--data-binary #{Shell.escape(value)}"
        _ -> ""
      end

    timeout_s = max(div(timeout_ms + 999, 1_000), 1)

    curl_inner =
      [
        "curl -sS",
        "--connect-timeout #{min(timeout_s, 30)}",
        "--max-time #{timeout_s}",
        method_arg,
        header_args,
        data_arg,
        "-w '__HPC_STATUS__:%{http_code}'",
        Shell.escape(target_url)
      ]
      |> Enum.reject(&(&1 == ""))
      |> Enum.join(" ")

    cmd =
      "srun --jobid=#{job_id} --overlap --ntasks=1 bash -lc #{Shell.escape(curl_inner)}"

    case SSH.exec(session, cmd, timeout: timeout_ms + 10_000) do
      {output, 0} ->
        parse_curl_wrapped_response(output)

      {output, status} ->
        {:error, {:curl_failed, status, String.trim(output)}}
    end
  end

  defp parse_curl_wrapped_response(output) do
    case Regex.run(~r/__HPC_STATUS__:(\d{3})\s*$/s, output, capture: :all_but_first) do
      [status_str] ->
        {status, _} = Integer.parse(status_str)

        body_without_marker =
          output
          |> String.replace(~r/__HPC_STATUS__:\d{3}\s*$/s, "")

        {:ok, status, body_without_marker, []}

      _ ->
        {:error, :invalid_curl_status_marker}
    end
  end

  defp fetch_first_model!(base_url, timeout_ms, request_fun) do
    url = normalize_base_url(base_url) <> "/v1/models"

    case request_fun.(:get, url, [], nil, timeout_ms) do
      {:ok, status, body, _headers} when status in 200..299 ->
        decoded = Jason.decode!(body)

        case get_in(decoded, ["data", Access.at(0), "id"]) do
          id when is_binary(id) and id != "" ->
            id

          _ ->
            raise RuntimeError,
                  "vLLM /v1/models returned no model id: #{truncate_http_body(body)}"
        end

      {:ok, status, body, _headers} ->
        raise RuntimeError,
              "vLLM model discovery failed (HTTP #{status}): #{truncate_http_body(body)}"

      {:error, reason} ->
        raise RuntimeError, "vLLM model discovery failed: #{inspect(reason)}"
    end
  end

  defp default_http_request(method, url, headers, body, timeout_ms) do
    uri = URI.parse(url)

    with {:ok, host} <- tcp_host(uri),
         {:ok, port} <- tcp_port(uri),
         {:ok, socket} <- tcp_connect(host, port, timeout_ms),
         :ok <-
           :gen_tcp.send(socket, build_http_request(method, uri, headers, body || "", host, port)),
         {:ok, raw_response} <- recv_all(socket, [], timeout_ms) do
      :gen_tcp.close(socket)
      parse_http_response(raw_response)
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp tcp_host(%URI{host: nil}), do: {:error, :missing_host}
  defp tcp_host(%URI{host: host}), do: {:ok, host}

  defp tcp_port(%URI{scheme: "http", port: nil}), do: {:ok, 80}
  defp tcp_port(%URI{scheme: "https", port: nil}), do: {:ok, 443}
  defp tcp_port(%URI{port: port}) when is_integer(port), do: {:ok, port}
  defp tcp_port(_), do: {:error, :invalid_port}

  defp tcp_connect(host, port, timeout_ms) do
    opts = [:binary, {:packet, :raw}, {:active, false}]
    :gen_tcp.connect(String.to_charlist(host), port, opts, timeout_ms)
  end

  defp build_http_request(method, %URI{} = uri, headers, body, host, port) do
    method_str = method |> to_string() |> String.upcase()
    path = (uri.path && uri.path != "" && uri.path) || "/"
    full_path = if uri.query, do: path <> "?" <> uri.query, else: path

    host_header =
      if (uri.scheme == "http" and port == 80) or (uri.scheme == "https" and port == 443) do
        host
      else
        "#{host}:#{port}"
      end

    default_headers =
      [
        {"Host", host_header},
        {"Connection", "close"}
      ] ++
        if(method == :post,
          do: [{"Content-Length", Integer.to_string(byte_size(body))}],
          else: []
        )

    rendered_headers =
      (headers ++ default_headers)
      |> Enum.map(fn {k, v} -> "#{k}: #{v}\r\n" end)
      |> IO.iodata_to_binary()

    [method_str, " ", full_path, " HTTP/1.1\r\n", rendered_headers, "\r\n", body]
    |> IO.iodata_to_binary()
  end

  defp recv_all(socket, acc, timeout_ms) do
    case :gen_tcp.recv(socket, 0, timeout_ms) do
      {:ok, chunk} -> recv_all(socket, [acc, chunk], timeout_ms)
      {:error, :closed} -> {:ok, IO.iodata_to_binary(acc)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_http_response(raw) when is_binary(raw) do
    case String.split(raw, "\r\n\r\n", parts: 2) do
      [head, body] ->
        lines = String.split(head, "\r\n", trim: true)

        with [status_line | header_lines] <- lines,
             {status, _} <- parse_status_line(status_line) do
          headers =
            Enum.map(header_lines, fn line ->
              case String.split(line, ":", parts: 2) do
                [k, v] -> {String.trim(k), String.trim(v)}
                _ -> {line, ""}
              end
            end)

          parsed_body =
            if chunked_transfer?(headers) do
              case decode_chunked_body(body) do
                {:ok, decoded} -> decoded
                _ -> body
              end
            else
              body
            end

          {:ok, status, parsed_body, headers}
        else
          _ -> {:error, :invalid_status_line}
        end

      _ ->
        {:error, :invalid_http_response}
    end
  end

  defp parse_status_line(line) do
    case Regex.run(~r/^HTTP\/\d\.\d\s+(\d{3})/, line, capture: :all_but_first) do
      [status] ->
        case Integer.parse(status) do
          {value, _} -> {value, line}
          :error -> {:error, :invalid_status}
        end

      _ ->
        {:error, :invalid_status}
    end
  end

  defp chunked_transfer?(headers) do
    Enum.any?(headers, fn {k, v} ->
      String.downcase(k) == "transfer-encoding" and
        String.contains?(String.downcase(v), "chunked")
    end)
  end

  defp decode_chunked_body(body) when is_binary(body), do: do_decode_chunked(body, [])

  defp do_decode_chunked(body, acc) do
    case :binary.match(body, "\r\n") do
      {idx, 2} ->
        size_line = binary_part(body, 0, idx)
        rest = binary_part(body, idx + 2, byte_size(body) - idx - 2)

        chunk_size_hex =
          size_line
          |> String.split(";", parts: 2)
          |> hd()
          |> String.trim()

        case Integer.parse(chunk_size_hex, 16) do
          {0, _} ->
            {:ok, IO.iodata_to_binary(acc)}

          {chunk_size, _} when chunk_size > 0 ->
            if byte_size(rest) >= chunk_size + 2 do
              chunk = binary_part(rest, 0, chunk_size)
              tail = binary_part(rest, chunk_size, byte_size(rest) - chunk_size)

              if String.starts_with?(tail, "\r\n") do
                remaining = binary_part(tail, 2, byte_size(tail) - 2)
                do_decode_chunked(remaining, [acc, chunk])
              else
                {:error, :invalid_chunk_terminator}
              end
            else
              {:error, :truncated_chunk}
            end

          _ ->
            {:error, :invalid_chunk_size}
        end

      :nomatch ->
        {:error, :invalid_chunk_header}
    end
  end

  defp parse_stream_content(raw) when is_binary(raw) do
    raw
    |> String.split("\n", trim: true)
    |> Enum.filter(&String.starts_with?(&1, "data:"))
    |> Enum.map(&String.trim(String.trim_leading(&1, "data:")))
    |> Enum.reject(&(&1 in ["", "[DONE]"]))
    |> Enum.reduce("", fn chunk, acc ->
      case Jason.decode(chunk) do
        {:ok, decoded} ->
          delta =
            get_in(decoded, ["choices", Access.at(0), "delta", "content"]) ||
              get_in(decoded, ["choices", Access.at(0), "message", "content"]) ||
              get_in(decoded, ["choices", Access.at(0), "text"]) || ""

          acc <> delta

        _ ->
          acc
      end
    end)
  end

  defp normalize_base_url(base_url) do
    String.trim_trailing(base_url, "/")
  end

  defp truncate_http_body(body) when is_binary(body) and byte_size(body) > 500 do
    binary_part(body, 0, 500) <> "..."
  end

  defp truncate_http_body(body) when is_binary(body), do: body
  defp truncate_http_body(other), do: inspect(other)

  @doc """
  Opens a **persistent native SSH connection** and returns `{updated_session, tunnel_port_or_nil}`.

  After this call, `updated_session.ssh_conn` is set and all commands (including
  `connect!/2`, `download_model/3`, `install_remote_scripts!/1`, etc.) run over the
  established connection — **no new OS processes, no CMD windows, works in Livebook**.

  If the cluster requires a ProxyJump host (all FAU/NHR clusters do), a single
  background OS process opens a plain TCP tunnel through the proxy and then Erlang `:ssh`
  connects through it. After that, the OS process is idle (no shell, no output).

  Close with `close_connection/1` or let the session be garbage-collected.

  Options: `:timeout` (ms, default 20_000).
  """
  @spec open_connection!(Session.t(), keyword()) :: {Session.t(), port() | nil}
  def open_connection!(%Session{} = session, opts \\ []), do: SSH.open_connection!(session, opts)

  @doc """
  Closes a persistent SSH connection and its proxy tunnel (if any).

  Returns the session with `ssh_conn` and `tunnel_port` cleared.
  """
  @spec close_connection(Session.t()) :: Session.t()
  def close_connection(%Session{} = session), do: SSH.close_connection(session)

  @doc """
  Allocates a GPU node persistently. Unlike `allocate_gpu/2` (which probes and releases),
  this holds the SLURM allocation alive as long as the returned port lives.

  Returns `%{port, job_id, node, partition, gpus, walltime}`.

  Release with `release_gpu/2` or simply `Port.close(alloc.port)`.

  Options: `:partition`, `:gpus`, `:walltime`, `:ntasks`, `:timeout` (ms, default 300_000).

  Deprecated: use `allocate_gpu/2` instead — it is now the persistent sbatch-based allocator.
  """
  @spec hold_gpu(Session.t(), keyword()) :: map()
  def hold_gpu(%Session{} = session, opts \\ []), do: allocate_gpu(session, opts)

  @doc """
  Releases a GPU allocation obtained from `allocate_gpu/2`.

  Cancels the SLURM batch job via `scancel <job_id>`.

  Works with:
  - `release_gpu(session, alloc)`
  - `release_gpu(session, job_id)`
  - `release_gpu(alloc)` when `alloc` includes `:session`
  - `release_gpu(job_id, alloc)` when `alloc` includes `:session`
  """
  @spec release_gpu(Session.t(), map() | binary() | pos_integer()) :: :ok
  def release_gpu(%Session{} = session, %{job_id: job_id}) do
    maybe_cancel_job(session, job_id)
  end

  def release_gpu(%Session{} = session, job_id)
      when is_binary(job_id) or is_integer(job_id) do
    maybe_cancel_job(session, job_id)
  end

  def release_gpu(job_id, %{job_id: alloc_job_id, session: %Session{} = session})
      when is_binary(job_id) or is_integer(job_id) do
    if to_string(job_id) == to_string(alloc_job_id) do
      maybe_cancel_job(session, job_id)
    else
      maybe_cancel_job(session, alloc_job_id)
    end
  end

  def release_gpu(%{job_id: job_id, session: %Session{} = session}) do
    maybe_cancel_job(session, job_id)
  end

  defp maybe_cancel_job(_session, job_id) when job_id in [nil, ""] do
    :ok
  end

  defp maybe_cancel_job(session, job_id) do
    cancel_job(session, job_id)
    :ok
  end

  @doc """
  Opens an SSH ControlMaster background connection for the session.

  Returns `{updated_session, master_port}`. Pass `updated_session` to subsequent
  API calls — all SSH commands will multiplex over the established connection,
  eliminating the per-command handshake overhead (~1-2 s saved per command).

  Close with `SSH.close_master(master_port)` or `Port.close(master_port)`.

  Requires OpenSSH ≥ 6.7. On Windows, requires Win32-OpenSSH with AF_UNIX
  support (Windows 10 build 1803+).

  Options: `:timeout` (ms to wait for socket, default 15_000).
  """
  @spec open_master!(Session.t(), keyword()) :: {Session.t(), port()}
  def open_master!(%Session{} = session, opts \\ []), do: SSH.open_master!(session, opts)

  defp script_missing?(output, script_name) do
    String.contains?(output, [script_name, "No such file or directory", "cannot execute"])
  end

  defp with_retry(fun, opts, retries \\ nil, delay_ms \\ 3_000)

  defp with_retry(fun, opts, nil, delay_ms),
    do: with_retry(fun, opts, Keyword.get(opts, :retries, 3), delay_ms)

  defp with_retry(fun, opts, retries, delay_ms) do
    fun.()
  rescue
    e in RuntimeError ->
      msg = Exception.message(e)
      transient? = Enum.any?(@transient_errors, &String.contains?(msg, &1))

      if transient? and retries > 0 do
        Process.sleep(delay_ms)
        with_retry(fun, opts, retries - 1, delay_ms)
      else
        reraise e, __STACKTRACE__
      end
  end

  defp safe_call(fun, retries \\ 3, delay_ms \\ 3_000) do
    %{ok?: true, value: fun.()}
  rescue
    e ->
      msg = Exception.message(e)
      transient? = Enum.any?(@transient_errors, &String.contains?(msg, &1))

      if transient? and retries > 0 do
        Process.sleep(delay_ms)
        safe_call(fun, retries - 1, delay_ms)
      else
        %{ok?: false, error: msg}
      end
  end

  # Returns true when STEADY_SSH_CONNECTION is enabled via opt, session env, or OS env.
  defp steady_ssh_connection?(session, opts) do
    truthy = fn v -> v in ["true", "1", "yes", true] end

    cond do
      Keyword.has_key?(opts, :steady_ssh_connection) ->
        truthy.(Keyword.get(opts, :steady_ssh_connection))

      Map.has_key?(session.env, "STEADY_SSH_CONNECTION") ->
        truthy.(session.env["STEADY_SSH_CONNECTION"])

      true ->
        truthy.(System.get_env("STEADY_SSH_CONNECTION", "false"))
    end
  end
end
