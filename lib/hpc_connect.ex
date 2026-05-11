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
    SSH,
    TunnelManager
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

  Option precedence is:
  1) explicit opts passed to `connection_setup/1`
  2) values loaded from `:env_file` (if provided)
  3) built-in defaults
  """
  @spec connection_setup(keyword()) :: map()
  def connection_setup(opts \\ []) do
    opts
    |> resolve_setup_opts()
    |> Livebook.connection_setup()
  end

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
    resolved_opts = resolve_setup_opts(opts)

    if Keyword.get(resolved_opts, :key_just_created) do
      IO.puts("")

      IO.puts(
        "Bootstrap paused: new SSH key was created. Re-run bootstrap after uploading the key."
      )

      return_bootstrap_key_created(resolved_opts)
    else
      bootstrap_continue(resolved_opts)
    end
  end

  defp return_bootstrap_key_created(opts) do
    key = Keyword.get(opts, :key_path) || Keyword.get(opts, :uploaded_key_path)

    %{
      ok?: false,
      key_just_created: true,
      key_path: key,
      session: nil,
      startup: nil,
      gpus: [],
      models: [],
      quota: [],
      jobs: []
    }
  end

  defp bootstrap_continue(resolved_opts) do
    setup_opts = Keyword.put_new(resolved_opts, :native_ssh, false)
    result = Livebook.connection_setup(setup_opts)
    reset_permission? = Keyword.get(resolved_opts, :reset_permission, false)

    session =
      result.session
      |> maybe_prepare_bootstrap_native_key(setup_opts)
      |> merge_bootstrap_runtime_env(resolved_opts)
      |> maybe_apply_hf_token(resolved_opts)
      |> maybe_open_bootstrap_native_connection(setup_opts)

    setup_session =
      if Keyword.get(resolved_opts, :install_scripts, true) do
        install_remote_scripts!(session, reset_permission: reset_permission?)
        session
      else
        session
      end

    setup_session =
      if Keyword.get(resolved_opts, :install_def_files, true) do
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

    startup = collect_startup_summary(setup_session, resolved_opts)

    Map.merge(result, %{
      session: setup_session,
      startup: startup,
      gpus: startup_value(startup.available_gpus),
      models: startup_value(startup.downloaded_models),
      quota: startup_value(startup.quota),
      jobs: startup_value(startup.jobs)
    })
  end

  defp resolve_setup_opts(opts) do
    env_map = bootstrap_env_map(opts)

    opts
    |> put_default_opt(:mode, parse_setup_mode(Map.get(env_map, "HPC_CONNECT_MODE")))
    |> put_default_opt(:username, Map.get(env_map, "HPC_CONNECT_USERNAME"))
    |> put_default_opt(:cluster, env_cluster(env_map))
    |> put_default_opt(:ssh_alias, Map.get(env_map, "HPC_CONNECT_SSH_ALIAS"))
    |> put_default_opt(:key_path, env_key_path(env_map))
    |> put_default_opt(:uploaded_key_path, Map.get(env_map, "HPC_CONNECT_UPLOADED_KEY_PATH"))
    |> put_default_opt(:work_dir, Map.get(env_map, "HPC_CONNECT_WORK_DIR"))
    |> put_default_opt(:vault_dir, Map.get(env_map, "HPC_CONNECT_VAULT_DIR"))
    |> put_default_opt(:proxy_jump, Map.get(env_map, "HPC_CONNECT_PROXY_JUMP"))
    |> put_default_opt(:port_range, parse_port_range(Map.get(env_map, "HPC_CONNECT_PORT_RANGE")))
    |> put_default_opt(:remote_command, Map.get(env_map, "HPC_CONNECT_REMOTE_COMMAND"))
    |> put_default_opt(:hf_token, env_hf_token(env_map))
    |> maybe_prepare_local_native_key()
  end

  defp maybe_prepare_local_native_key(opts) do
    mode = Keyword.get(opts, :mode, :livebook)
    native_ssh? = Keyword.get(opts, :native_ssh, false)

    if mode == :local and native_ssh? do
      key = Keyword.get(opts, :key_path) || Keyword.get(opts, :uploaded_key_path)

      case normalize_key_path(key) do
        nil ->
          opts

        key_path ->
          {resolved_key_path, status} = ensure_native_key_ready!(key_path, :local)

          case status do
            :existing_fallback ->
              IO.puts("Using existing native-SSH PEM fallback key: #{resolved_key_path}")
              opts

            :created_fallback ->
              IO.puts(native_key_created_message(resolved_key_path))
              Keyword.put(opts, :key_just_created, true)

            _ ->
              opts
          end
          |> Keyword.put(:key_path, resolved_key_path)
      end
    else
      opts
    end
  end

  defp maybe_prepare_bootstrap_native_key(%Session{} = session, opts) do
    if Keyword.get(opts, :native_ssh, true) do
      mode = Keyword.get(opts, :mode, :livebook)

      case session.identity_file do
        path when is_binary(path) and path != "" ->
          {resolved_key_path, status} = ensure_native_key_ready!(normalize_key_path(path), mode)

          case status do
            :existing_fallback ->
              IO.puts("Using existing native-SSH PEM fallback key: #{resolved_key_path}")

            :created_fallback ->
              IO.puts(native_key_created_message(resolved_key_path))

            _ ->
              :ok
          end

          %{session | identity_file: resolved_key_path}

        _ ->
          session
      end
    else
      session
    end
  end

  defp ensure_native_key_ready!(nil, mode) do
    raise ArgumentError,
          "No SSH private key path available for #{mode} mode. Provide :key_path (or :uploaded_key_path)."
  end

  defp ensure_native_key_ready!(key_path, mode) do
    unless File.exists?(key_path) do
      raise RuntimeError,
            "SSH private key not found: #{key_path}. Aborting before opening any SSH/proxy connection."
    end

    case native_ssh_key_format_path(key_path) do
      :openssh ->
        candidate = pem_fallback_key_path(key_path)

        cond do
          File.exists?(candidate) ->
            {candidate, :existing_fallback}

          true ->
            case create_local_pem_fallback_key(candidate) do
              :ok ->
                {candidate, :created_fallback}

              {:error, reason} ->
                manual_cmd = ssh_keygen_manual_cmd(candidate)

                raise RuntimeError,
                      "Could not auto-create native PEM key fallback for #{mode} mode: #{reason}\n" <>
                        "Run manually:\n#{manual_cmd}\n" <>
                        "Then upload #{candidate}.pub to https://portal.hpc.fau.de/ui/user and wait 10–30 minutes for propagation."
            end
        end

      _ ->
        {key_path, :original}
    end
  end

  defp ssh_keygen_manual_cmd(candidate) do
    case :os.type() do
      {:win32, _} ->
        "ssh-keygen -t rsa -b 4096 -m PEM -f \"#{candidate}\" -N \"\""

      _ ->
        "ssh-keygen -t rsa -b 4096 -m PEM -f #{Shell.escape(candidate)} -N ''"
    end
  end

  defp normalize_key_path(nil), do: nil
  defp normalize_key_path(path) when is_binary(path), do: Path.expand(path)

  defp pem_fallback_key_path(path) do
    if String.ends_with?(path, "_hpc_connect_pem") do
      path
    else
      path <> "_hpc_connect_pem"
    end
  end

  defp create_local_pem_fallback_key(path) do
    keygen = System.find_executable("ssh-keygen") || "ssh-keygen"

    case :os.type() do
      {:win32, _} ->
        # Windows ssh-keygen.exe drops empty string argv values, so -N "" never
        # reaches the process correctly via System.cmd/Port. Write a temp .bat
        # file that embeds the full command verbatim and execute it via cmd /c.
        bat_path =
          Path.join(System.tmp_dir!(), "hpc_keygen_#{:erlang.unique_integer([:positive])}.bat")

        bat_content =
          "@echo off\r\n\"#{keygen}\" -t rsa -b 4096 -m PEM -f \"#{path}\" -N \"\"\r\n"

        File.write!(bat_path, bat_content)

        result = System.cmd("cmd", ["/c", bat_path], stderr_to_stdout: true)
        File.rm(bat_path)

        case result do
          {_output, 0} ->
            :ok

          {output, status} ->
            {:error, "ssh-keygen failed (exit #{status}): #{String.trim(output)}"}
        end

      _ ->
        args = ["-t", "rsa", "-b", "4096", "-m", "PEM", "-f", path, "-N", ""]

        case System.cmd(keygen, args, stderr_to_stdout: true) do
          {_output, 0} ->
            :ok

          {output, status} ->
            {:error, "ssh-keygen failed (exit #{status}): #{String.trim(output)}"}
        end
    end
  rescue
    e -> {:error, Exception.message(e)}
  end

  defp native_key_created_message(candidate_path) do
    pub_path = candidate_path <> ".pub"

    pub_line =
      case File.read(pub_path) do
        {:ok, pub} ->
          "\nPublic key to upload (single line):\n#{String.trim(pub)}\n"

        {:error, _} ->
          "\nUpload public key file: #{pub_path}\n"
      end

    "New native-SSH PEM key created: #{candidate_path}\n" <>
      pub_line <>
      "Portal: https://portal.hpc.fau.de/ui/user\n" <>
      "Upload the public key there, then wait 10–30 minutes for propagation.\n" <>
      "Re-run bootstrap once the key is active on the cluster."
  end

  defp bootstrap_env_map(opts) do
    case Keyword.get(opts, :env_file) do
      value when is_binary(value) and value != "" -> EnvFile.load(value)
      _ -> %{}
    end
  end

  defp env_key_path(env_map) do
    Map.get(env_map, "HPC_CONNECT_KEY_PATH") || Map.get(env_map, "HPC_CONNECT_IDENTITY_FILE")
  end

  defp env_cluster(env_map) do
    Map.get(env_map, "HPC_CONNECT_CLUSTER") || Map.get(env_map, "HPC_CONNECT_SSH_ALIAS")
  end

  defp env_hf_token(env_map) do
    Map.get(env_map, "HUGGINGFACE_HUB_TOKEN") || Map.get(env_map, "HF_TOKEN")
  end

  defp merge_bootstrap_runtime_env(%Session{} = session, opts) do
    env_map =
      opts
      |> bootstrap_env_map()
      |> Enum.reject(fn {key, _value} ->
        String.starts_with?(key, "HPC_CONNECT_") or key == "STEADY_SSH_CONNECTION"
      end)
      |> Map.new()

    Session.merge_env(session, env_map)
  end

  defp maybe_open_bootstrap_native_connection(%Session{} = session, opts) do
    native_ssh? = Keyword.get(opts, :native_ssh, true)
    bootstrap_open_connection? = Keyword.get(opts, :bootstrap_open_connection, false)
    fallback_to_os? = Keyword.get(opts, :native_ssh_fallback_to_os, false)
    open_opts = Keyword.get(opts, :open_connection_opts, [])
    custom_connect_fun? = match?(fun when is_function(fun, 3), Keyword.get(opts, :connect_fun))

    cond do
      not bootstrap_open_connection? ->
        session

      custom_connect_fun? ->
        session

      not native_ssh? ->
        session

      not is_nil(session.ssh_conn) ->
        session

      true ->
        try do
          {connected, _tunnel} = open_connection!(session, open_opts)
          connected
        rescue
          e ->
            if fallback_to_os? do
              IO.puts(
                "Native SSH bootstrap connection failed; continuing with OS SSH fallback: #{Exception.message(e)}"
              )

              session
            else
              reraise e, __STACKTRACE__
            end
        end
    end
  end

  defp maybe_apply_hf_token(%Session{} = session, opts) do
    token =
      Keyword.get(opts, :hf_token) ||
        Keyword.get(opts, :"hf:token") ||
        Session.fetch_env(session, "HUGGINGFACE_HUB_TOKEN")

    case token do
      value when is_binary(value) and value != "" -> put_hf_token(session, value)
      _ -> session
    end
  end

  defp put_default_opt(opts, _key, nil), do: opts

  defp put_default_opt(opts, key, value) do
    if Keyword.has_key?(opts, key) do
      opts
    else
      Keyword.put(opts, key, value)
    end
  end

  defp parse_setup_mode(nil), do: nil

  defp parse_setup_mode(value) when is_binary(value) do
    case String.downcase(String.trim(value)) do
      "local" -> :local
      "livebook" -> :livebook
      _ -> nil
    end
  end

  defp parse_setup_mode(value) when value in [:local, :livebook], do: value
  defp parse_setup_mode(_), do: nil

  defp parse_port_range(nil), do: nil

  defp parse_port_range(value) when is_binary(value) do
    case String.split(value, ":", parts: 2) do
      [min_port, max_port] -> {String.to_integer(min_port), String.to_integer(max_port)}
      _ -> nil
    end
  rescue
    ArgumentError -> nil
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
  - `:native_ssh` (default: `false`) enables a persistent native `:ssh` session
    (existing `session.ssh_conn` is used only when this is explicitly `true`)
  - `:native_ssh_fallback_to_os` (default: `false`) — when `true`, falls back
    to managed OpenSSH commands/tunnel if native `:ssh` cannot connect
  - `:proxy_jump_via_native` (default: `true`) enables native ProxyJump tunnel
    (`:ssh.tcpip_tunnel_to_server`) when direct connect is blocked
  - `:proxy_jump_via_os` (default: `true` when ProxyJump is required,
    otherwise `false`) — enables managed OpenSSH ProxyJump tunnel fallback if
    native jump tunneling cannot be established; set `false` for strict
    native-only behavior
    - `:auto_proxy` (default depends on `:native_ssh`) where:
      - with `native_ssh: false` (default), managed OpenSSH proxy is enabled
      - with `native_ssh: true`, managed OpenSSH proxy is disabled unless enabled/fallback
      - `false` keeps native-only access (no localhost proxy)
      - `true` opens an OpenSSH localhost tunnel explicitly
    Local port is selected automatically unless `:local_port` is set.
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
  @pending_wait_table :hpc_connect_pending_waits
  @proxy_table :hpc_connect_open_proxies

  @spec start_app(Session.t(), keyword()) :: map()
  def start_app(%Session{} = session, opts) do
    app = Keyword.fetch!(opts, :app)
    app_upper = app |> to_string() |> String.upcase()

    {slurm_kw, config_kw} = split_start_app_args(opts)
    port = start_app_port(slurm_kw, config_kw)

    app_env =
      Map.new(config_kw, fn {k, v} ->
        {"#{app_upper}_#{k |> to_string() |> String.upcase()}", v}
      end)

    slurm_opts =
      [app: app, port: port, app_env: app_env] ++
        Enum.flat_map(@slurm_keys, &maybe_opt(slurm_kw, &1))

    submitted = submit_app_job(session, slurm_opts, opts)

    submitted
    |> maybe_wait_for_allocation(session, opts)
    |> maybe_attach_vllm_access(session, app, opts)
  end

  @doc """
  Reconnects to an already submitted/running app job without submitting a new one.

  This is useful after `recompile()`, shell restart, or Livebook kernel restart when
  the SLURM job is still alive.

  Example:

      HpcConnect.reconnect(session, 1591066,
        app: "vllm",
        args: [port: 50200]
      )

  For vLLM this returns the same enriched structure as `start_app/2` (including
  `:base_url`, and optionally `:proxy`/`:tunnel_port` depending on access mode).
  """
  @spec reconnect(Session.t(), binary() | pos_integer(), keyword()) :: map()
  def reconnect(%Session{} = session, job_id, opts) do
    app = Keyword.fetch!(opts, :app)

    {slurm_kw, config_kw} = split_start_app_args(opts)
    job_id = to_string(job_id)

    submitted =
      %{job_id: job_id, port: start_app_port(slurm_kw, config_kw)}
      |> maybe_put(:partition, Keyword.get(slurm_kw, :partition))
      |> maybe_put(:gpus, Keyword.get(slurm_kw, :gpus))
      |> maybe_put(:walltime, Keyword.get(slurm_kw, :walltime))
      |> enrich_reconnect_submission(session, job_id, opts)

    submitted
    |> maybe_wait_for_allocation(session, Keyword.put_new(opts, :cancel_on_interrupt, false))
    |> maybe_attach_vllm_access(session, app, opts)
  end

  defp maybe_prepare_session_for_app(%Session{} = session, app, opts) do
    app_name = app |> to_string() |> String.downcase()
    proxy_jump_required? = is_binary(session.proxy_jump) and session.proxy_jump != ""

    if app_name == "vllm" and Keyword.get(opts, :native_ssh, false) and
         is_nil(session.ssh_conn) do
      IO.puts(
        "[HpcConnect] Native stage 1/2: establish login-node SSH session to #{session.cluster.host} " <>
          "(alias: #{Session.target(session)})."
      )

      IO.puts(
        "[HpcConnect] Native stage 2/2 (after stage 1 succeeds): open compute-node tunnel to vLLM port."
      )

      IO.puts(
        "[HpcConnect] Native SSH equivalent (diagnostic): #{SSH.native_login_connect_preview(session)}"
      )

      if proxy_jump_required? do
        IO.puts(
          "[HpcConnect] ProxyJump required via #{session.proxy_jump}; " <>
            "using hybrid native strategy (native jump first, OpenSSH bridge fallback if needed)."
        )
      end

      native_ssh_fallback_to_os? = Keyword.get(opts, :native_ssh_fallback_to_os, false)
      open_connection_fun = Keyword.get(opts, :open_connection_fun, &open_connection!/2)

      # Only attempt native SSH if the key format is supported; otherwise fall back immediately.
      native_key_ok? =
        native_ssh_fallback_to_os? or
          native_ssh_key_format(session) != :openssh

      if native_key_ok? do
        try do
          open_opts = [
            timeout: Keyword.get(opts, :ssh_timeout_ms, 20_000),
            attempts: Keyword.get(opts, :ssh_attempts, 3),
            proxy_jump_via_native: Keyword.get(opts, :proxy_jump_via_native, true),
            proxy_jump_via_os:
              case Keyword.fetch(opts, :proxy_jump_via_os) do
                {:ok, value} -> value
                :error -> proxy_jump_required?
              end
          ]

          {updated, _tunnel} = open_connection_fun.(session, open_opts)
          updated
        rescue
          e in RuntimeError ->
            key_hint = native_ssh_key_hint(session)

            if native_ssh_fallback_to_os? do
              IO.puts(
                "[HpcConnect] Native Erlang SSH unavailable (#{Exception.message(e)}). " <>
                  "Falling back to OS SSH commands/tunnel for vLLM access." <> key_hint
              )

              session
            else
              raise RuntimeError,
                    "Native Erlang SSH unavailable for vLLM access (#{Exception.message(e)})." <>
                      key_hint
            end
        end
      else
        key_hint = native_ssh_key_hint(session)

        if native_ssh_fallback_to_os? do
          session
        else
          raise RuntimeError,
                "Native Erlang SSH key format unsupported for vLLM access. " <>
                  "Use a PEM key or set native_ssh_fallback_to_os: true." <> key_hint
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

      IO.puts("Submitted app job #{submitted.job_id}. Waiting for GPU allocation ...")

      IO.puts(
        "IEx stop: Ctrl+C then k. Manual cancel from another shell: HpcConnect.release_gpu(session, #{submitted.job_id})"
      )

      IO.puts(
        "Or cancel all pending waits from another process in the same BEAM VM: HpcConnect.cancel_pending_waits()"
      )

      register_pending_wait(self(), session, submitted.job_id)

      interrupt_guard =
        if cancel_on_interrupt? do
          start_interrupt_guard(session, submitted.job_id)
        else
          nil
        end

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
        :throw, {:hpc_connect_cancel_wait, reason} ->
          if cancel_on_interrupt?, do: safe_release_gpu(session, submitted.job_id)

          raise RuntimeError,
                "GPU allocation wait canceled (#{reason}) for job #{submitted.job_id}"

        kind, value ->
          if cancel_on_interrupt?, do: safe_release_gpu(session, submitted.job_id)

          case kind do
            :exit -> exit(value)
            :throw -> throw(value)
            :error -> :erlang.error(value)
          end
      after
        stop_interrupt_guard(interrupt_guard)
        unregister_pending_wait(self())
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
      wait_for_node? = Keyword.get(opts, :wait_for_node, true)

      if is_nil(submitted[:node]) and not wait_for_node? do
        submitted
        |> Map.put(:pending?, true)
        |> Map.put(:access_mode, :pending_allocation)
      else
        node =
          submitted.node ||
            wait_for_access_node(session, submitted.job_id, opts)

        IO.puts("[HpcConnect] vLLM compute node resolved: #{node}")

        IO.puts(
          "[HpcConnect] vLLM tunnel equivalent (diagnostic): " <>
            SSH.native_compute_tunnel_preview(
              session,
              node,
              submitted.port,
              Keyword.get(opts, :local_port)
            )
        )

        # Important: prepare native SSH only after we know the compute node.
        # This avoids early proxy-jump attempts during start/reconnect pre-phase.
        {session, native_error} = maybe_prepare_session_for_app_after_node(session, app, opts)

        native_enriched =
          submitted
          |> Map.put(:node, node)
          |> Map.put(:session, session)
          |> Map.put(:port, submitted.port)
          |> Map.put(:base_url, "http://#{node}:#{submitted.port}")
          |> Map.put(:access_mode, :native_ssh)

        # When we have a native SSH connection to the login node, open a
        # tcpip_tunnel_to_server channel from it to the compute node's app port.
        # This is fully native — no OS process, no background port — the tunnel
        # lives inside the existing :ssh connection and stays open as long as
        # session.ssh_conn is alive.
        # Keep OpenSSH proxy as the default access path unless the caller
        # explicitly opts into native SSH for this app launch.
        native_conn = if Keyword.get(opts, :native_ssh, false), do: session.ssh_conn, else: nil

        if not is_nil(native_conn) do
          local_port = Keyword.get(opts, :local_port, 0)

          {compute_conn, actual_port} =
            open_vllm_native_tunnel!(session, native_conn, node, submitted.port, local_port, opts)

          # Store compute_conn in the session so it stays alive and can be closed via close_connection/1.
          session_with_compute = %{session | compute_ssh_conn: compute_conn}

          IO.puts(
            "[HpcConnect] Native 2-hop tunnel active: localhost:#{actual_port} → #{node}:#{submitted.port}. " <>
              "Keep the returned vllm.session alive to keep this tunnel open."
          )

          enriched =
            native_enriched
            |> Map.put(:session, session_with_compute)
            |> Map.put(:tunnel_port, actual_port)
            |> Map.put(:base_url, "http://localhost:#{actual_port}")
            |> Map.put(:access_mode, :native_tunnel)

          maybe_wait_for_vllm_ready(enriched, opts)
        else
          use_proxy? = not is_nil(native_error) or auto_proxy_enabled?(session, opts)

          if use_proxy? do
            if native_error do
              IO.puts(
                "[HpcConnect] Native SSH access unavailable after node allocation; " <>
                  "falling back to managed proxy tunnel: #{native_error}"
              )
            end

            {proxy, tunnel_port} = open_vllm_proxy!(session, node, submitted.port, opts)

            enriched =
              native_enriched
              |> Map.put(:proxy, proxy)
              |> Map.put(:tunnel_port, tunnel_port)
              |> Map.put(:base_url, proxy.base_url)
              |> Map.put(:access_mode, :openssh_proxy)
              |> maybe_put(:native_error, native_error)

            maybe_wait_for_vllm_ready(enriched, opts)
          else
            if native_error do
              raise RuntimeError,
                    "Native vLLM access failed after compute-node allocation (#{native_error}). " <>
                      "Set auto_proxy: true to enable managed localhost forwarding."
            else
              maybe_wait_for_vllm_ready(native_enriched, opts)
            end
          end
        end
      end
    else
      submitted
    end
  end

  defp submit_app_job(%Session{} = session, slurm_opts, opts) do
    case Keyword.get(opts, :submit_fun) do
      fun when is_function(fun, 2) ->
        fun.(session, slurm_opts)

      _ ->
        with_retry(fn -> submit_apptainer(session, slurm_opts) end, opts)
    end
  end

  defp wait_for_access_node(%Session{} = session, job_id, opts) do
    wait_opts = [
      timeout: Keyword.get(opts, :proxy_wait_timeout_ms, 600_000),
      interval: Keyword.get(opts, :node_poll_interval_ms, 10_000)
    ]

    case Keyword.get(opts, :wait_for_job_node_fun) do
      fun when is_function(fun, 3) ->
        fun.(session, job_id, wait_opts)

      _ ->
        wait_for_job_node(session, job_id, wait_opts)
    end
  end

  defp maybe_prepare_session_for_app_after_node(%Session{} = session, app, opts) do
    try do
      {maybe_prepare_session_for_app(session, app, opts), nil}
    rescue
      e in RuntimeError ->
        {session, Exception.message(e)}
    end
  end

  defp open_vllm_native_tunnel!(session, native_conn, node, remote_port, local_port, opts) do
    timeout = Keyword.get(opts, :native_tunnel_timeout_ms, 20_000)

    case Keyword.get(opts, :open_native_tunnel_fun) do
      fun when is_function(fun, 6) ->
        fun.(session, native_conn, node, remote_port, local_port, timeout)

      _ ->
        SSH.open_native_compute_tunnel!(
          session,
          native_conn,
          node,
          remote_port,
          local_port,
          timeout
        )
    end
  end

  defp open_vllm_proxy!(%Session{} = session, node, remote_port, opts) do
    proxy_opts = [
      remote_port: remote_port,
      local_port: opts[:local_port],
      attempts: Keyword.get(opts, :proxy_open_attempts, 5)
    ]

    case Keyword.get(opts, :open_proxy_fun) do
      fun when is_function(fun, 3) ->
        fun.(session, node, proxy_opts)

      _ ->
        open_proxy_with_retry!(session, node, proxy_opts)
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
          interruptible_sleep(interval_ms)
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

  defp start_interrupt_guard(%Session{} = _session, job_id) when job_id in [nil, ""], do: nil

  defp start_interrupt_guard(%Session{} = session, job_id) do
    owner = self()

    spawn(fn ->
      ref = Process.monitor(owner)

      receive do
        :done ->
          :ok

        {:DOWN, ^ref, :process, ^owner, reason} ->
          # If the caller process gets killed from the IEx break menu (`k`),
          # ensure we still attempt to clean up the pending allocation.
          if reason not in [:normal, :shutdown] do
            safe_release_gpu(session, job_id)
          end
      end
    end)
  end

  defp stop_interrupt_guard(nil), do: :ok

  defp stop_interrupt_guard(pid) when is_pid(pid) do
    send(pid, :done)
    :ok
  end

  defp split_start_app_args(opts) do
    case Keyword.fetch(opts, :args) do
      {:ok, args} ->
        {Enum.filter(args, fn {k, _} -> k in @slurm_keys end),
         Enum.reject(args, fn {k, _} -> k in @slurm_keys end)}

      :error ->
        {Keyword.get(opts, :slurm, []), Keyword.get(opts, :config, [])}
    end
  end

  defp start_app_port(slurm_kw, config_kw) do
    Keyword.get(config_kw, :port, Keyword.get(slurm_kw, :port, 8000))
  end

  defp enrich_reconnect_submission(submitted, %Session{} = session, job_id, opts) do
    connect_opts = Keyword.get(opts, :connect_opts, [])

    job_entry =
      session
      |> list_jobs_summary(connect_opts: connect_opts)
      |> Enum.find(fn row -> to_string(Map.get(row, :job_id)) == to_string(job_id) end)

    case job_entry do
      nil ->
        submitted

      row ->
        submitted
        |> Map.put_new(:partition, Map.get(row, :partition))
        |> Map.put_new(:walltime, Map.get(row, :time_limit))
        |> maybe_put(:node, normalize_squeue_node(Map.get(row, :node_list)))
    end
  rescue
    _ -> submitted
  end

  defp normalize_squeue_node(nil), do: nil

  defp normalize_squeue_node(value) when is_binary(value) do
    value = String.trim(value)

    cond do
      value == "" -> nil
      String.starts_with?(value, "(") -> nil
      true -> value
    end
  end

  defp normalize_squeue_node(_), do: nil

  defp native_ssh_key_hint(%Session{identity_file: nil}), do: ""

  defp native_ssh_key_hint(%Session{identity_file: path}) when is_binary(path) do
    case File.read(path) do
      {:ok, "-----BEGIN OPENSSH PRIVATE KEY-----" <> _rest} ->
        " Native Erlang :ssh on this OTP build cannot parse OpenSSH private keys directly. " <>
          "Use a PEM key for native_ssh (or keep fallback enabled)."

      _ ->
        ""
    end
  end

  defp native_ssh_key_hint(_), do: ""

  defp native_ssh_key_format(%Session{identity_file: path}) when is_binary(path) do
    native_ssh_key_format_path(path)
  end

  defp native_ssh_key_format(_), do: :unknown

  defp native_ssh_key_format_path(path) when is_binary(path) do
    case File.read(path) do
      {:ok, "-----BEGIN OPENSSH PRIVATE KEY-----" <> _rest} -> :openssh
      {:ok, "-----BEGIN " <> _rest} -> :pem
      _ -> :unknown
    end
  end

  defp native_ssh_key_format_path(_), do: :unknown

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  @doc """
  Cancels all currently pending `start_app/2` or `reconnect/3` allocation waits.

  This is intended for interactive IEx workflows where keyboard interrupt handling
  may not reliably stop a long-running wait.

  Note: this helper works within the same BEAM VM (same IEx runtime).

  Returns the number of waiters signaled for cancellation.
  """
  @spec cancel_pending_waits() :: non_neg_integer()
  def cancel_pending_waits do
    ensure_pending_wait_table()

    @pending_wait_table
    |> :ets.tab2list()
    |> Enum.reduce(0, fn {pid, session, job_id}, acc ->
      send(pid, {:hpc_connect_cancel_wait, :manual})
      _ = safe_release_gpu_public(session, job_id)
      acc + 1
    end)
  end

  @doc """
  Cancels a specific pending allocation waiter process.

  Returns `:ok` when a waiter entry was found and signaled, otherwise `:not_found`.
  """
  @spec cancel_pending_wait(pid()) :: :ok | :not_found
  def cancel_pending_wait(pid) when is_pid(pid) do
    ensure_pending_wait_table()

    case :ets.lookup(@pending_wait_table, pid) do
      [{^pid, session, job_id}] ->
        send(pid, {:hpc_connect_cancel_wait, :manual})
        _ = safe_release_gpu_public(session, job_id)
        :ok

      _ ->
        :not_found
    end
  end

  defp ensure_pending_wait_table do
    case :ets.whereis(@pending_wait_table) do
      :undefined ->
        :ets.new(@pending_wait_table, [:named_table, :public, :set])
        :ok

      _tid ->
        :ok
    end
  rescue
    _ -> :ok
  end

  defp register_pending_wait(pid, %Session{} = session, job_id)
       when is_pid(pid) and job_id not in [nil, ""] do
    ensure_pending_wait_table()
    :ets.insert(@pending_wait_table, {pid, session, job_id})
    :ok
  rescue
    _ -> :ok
  end

  defp register_pending_wait(_pid, _session, _job_id), do: :ok

  defp unregister_pending_wait(pid) when is_pid(pid) do
    case :ets.whereis(@pending_wait_table) do
      :undefined ->
        :ok

      _ ->
        :ets.delete(@pending_wait_table, pid)
        :ok
    end
  rescue
    _ -> :ok
  end

  defp safe_release_gpu_public(%Session{} = session, job_id) do
    _ = release_gpu(session, job_id)
    :ok
  rescue
    _ -> :ok
  end

  defp auto_proxy_enabled?(%Session{} = session, opts) do
    native_requested? = Keyword.get(opts, :native_ssh, false)

    # Defaults:
    # - Native mode requested -> proxy only when fallback is explicitly allowed.
    # - Native mode not requested -> managed OS proxy is the default path.
    default =
      if native_requested? do
        Keyword.get(opts, :native_ssh_fallback_to_os, false) and is_nil(session.ssh_conn)
      else
        true
      end

    case Keyword.get(opts, :auto_proxy, default) do
      true -> true
      false -> false
      :auto -> default
      nil -> default
      _other -> false
    end
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
      interruptible_sleep(interval_ms)
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
      session: session,
      local_port: local_port,
      remote_port: remote_port,
      node: node,
      base_url: "http://localhost:#{local_port}",
      preview: command_preview(command),
      command: command
    }
  end

  @doc """
  Opens the SSH tunnel as a background OS process managed by `HpcConnect.TunnelManager`.

  Returns an Erlang `Port`. The tunnel stays alive independently of the caller
  (important for Livebook / short-lived eval processes).

  Close with `close_proxy/1`.
  """
  @spec open_proxy!(map()) :: port()
  def open_proxy!(
        %{command: %Command{binary: binary, args: args}, local_port: local_port} = proxy
      ) do
    port =
      case TunnelManager.open_port(binary, args, [:binary, :exit_status, :stderr_to_stdout]) do
        {:ok, managed_port} ->
          managed_port

        {:error, reason} ->
          raise RuntimeError, "failed to open SSH proxy process: #{inspect(reason)}"
      end

    try do
      wait_for_local_proxy_port!(port, local_port, 15_000)
      maybe_register_proxy(port, proxy)
      port
    rescue
      e ->
        _ = TunnelManager.close_port(port)
        reraise e, __STACKTRACE__
    end
  end

  def open_proxy!(%{command: %Command{binary: binary, args: args}} = proxy) do
    port =
      case TunnelManager.open_port(binary, args, [:binary, :exit_status, :stderr_to_stdout]) do
        {:ok, managed_port} ->
          managed_port

        {:error, reason} ->
          raise RuntimeError, "failed to open SSH proxy process: #{inspect(reason)}"
      end

    maybe_register_proxy(port, proxy)
    port
  end

  @doc """
  Kills tracked proxy tunnel(s) for a given `session` and compute `node`.

  Returns `:ok` if one or more tunnels were found and closed.
  Returns `{:error, :not_found}` if no tracked tunnel exists.
  """
  @spec kill_proxy(Session.t(), binary()) :: :ok | {:error, :not_found}
  def kill_proxy(%Session{} = session, node) when is_binary(node) and node != "" do
    ensure_proxy_table()
    key = proxy_key(session, node)

    case :ets.lookup(@proxy_table, key) do
      [] ->
        {:error, :not_found}

      rows ->
        Enum.each(rows, fn {_key, info} ->
          case info do
            %{port: port} when is_port(port) ->
              _ = TunnelManager.close_port(port)

            _ ->
              :ok
          end
        end)

        :ets.delete(@proxy_table, key)
        :ok
    end
  rescue
    _ -> {:error, :not_found}
  end

  def kill_proxy(%Session{}, _), do: {:error, :not_found}

  @doc """
  Returns tracked proxy metadata for a given `session` and compute `node`.
  """
  @spec proxy_info(Session.t(), binary()) :: [map()]
  def proxy_info(%Session{} = session, node) when is_binary(node) and node != "" do
    ensure_proxy_table()

    @proxy_table
    |> :ets.lookup(proxy_key(session, node))
    |> Enum.map(fn {_key, info} -> info end)
  rescue
    _ -> []
  end

  def proxy_info(%Session{}, _), do: []

  @doc """
  Closes a proxy tunnel opened by `open_proxy!/1`.
  """
  @spec close_proxy(port()) :: :ok
  def close_proxy(port) when is_port(port) do
    _ = TunnelManager.close_port(port)
    unregister_proxy_by_port(port)
    :ok
  end

  defp maybe_register_proxy(port, %{session: %Session{} = session, node: node} = proxy)
       when is_port(port) and is_binary(node) and node != "" do
    ensure_proxy_table()

    info = %{
      port: port,
      os_pid: proxy_os_pid(port),
      node: node,
      local_port: Map.get(proxy, :local_port),
      remote_port: Map.get(proxy, :remote_port),
      base_url: Map.get(proxy, :base_url),
      opened_at_ms: System.system_time(:millisecond)
    }

    :ets.insert(@proxy_table, {proxy_key(session, node), info})
    :ok
  rescue
    _ -> :ok
  end

  defp maybe_register_proxy(_port, _proxy), do: :ok

  defp unregister_proxy_by_port(port) when is_port(port) do
    ensure_proxy_table()

    @proxy_table
    |> :ets.tab2list()
    |> Enum.filter(fn {_key, info} -> Map.get(info, :port) == port end)
    |> Enum.each(fn row -> :ets.delete_object(@proxy_table, row) end)

    :ok
  rescue
    _ -> :ok
  end

  defp proxy_os_pid(port) when is_port(port) do
    case Port.info(port, :os_pid) do
      {:os_pid, os_pid} -> os_pid
      _ -> nil
    end
  rescue
    _ -> nil
  end

  defp proxy_os_pid(_), do: nil

  defp ensure_proxy_table do
    case :ets.whereis(@proxy_table) do
      :undefined ->
        :ets.new(@proxy_table, [:named_table, :public, :bag])
        :ok

      _tid ->
        :ok
    end
  rescue
    _ -> :ok
  end

  defp proxy_key(%Session{} = session, node) do
    {
      session.cluster.name,
      session.username || "",
      session.identity_file || "",
      session.work_dir || "",
      node
    }
  end

  defp open_proxy_with_retry!(%Session{} = session, node, opts) do
    attempts = max(1, Keyword.get(opts, :attempts, 5))
    requested_local_port = Keyword.get(opts, :local_port)
    remote_port = Keyword.get(opts, :remote_port, 8000)

    do_open_proxy_with_retry!(
      session,
      node,
      remote_port,
      requested_local_port,
      attempts,
      MapSet.new()
    )
  end

  defp do_open_proxy_with_retry!(
         _session,
         _node,
         _remote_port,
         _requested_local_port,
         0,
         _tried_ports
       ) do
    raise RuntimeError, "failed to open SSH proxy after multiple local port attempts"
  end

  defp do_open_proxy_with_retry!(
         session,
         node,
         remote_port,
         requested_local_port,
         attempts_left,
         tried_ports
       ) do
    local_port = select_proxy_local_port(requested_local_port, tried_ports)

    proxy = start_proxy(session, node, remote_port: remote_port, local_port: local_port)

    try do
      tunnel_port = open_proxy!(proxy)

      runtime_proxy =
        proxy
        |> Map.put(:port, tunnel_port)
        |> maybe_put(:pid, proxy_os_pid(tunnel_port))
        |> maybe_put(:os_pid, proxy_os_pid(tunnel_port))

      {runtime_proxy, tunnel_port}
    rescue
      err in RuntimeError ->
        if attempts_left > 1 do
          Process.sleep(150)

          do_open_proxy_with_retry!(
            session,
            node,
            remote_port,
            nil,
            attempts_left - 1,
            MapSet.put(tried_ports, local_port)
          )
        else
          reraise err, __STACKTRACE__
        end
    end
  end

  defp select_proxy_local_port(requested_local_port, tried_ports)
       when is_integer(requested_local_port) and requested_local_port > 0 do
    if MapSet.member?(tried_ports, requested_local_port) do
      next_free_local_port(tried_ports)
    else
      requested_local_port
    end
  end

  defp select_proxy_local_port(_requested_local_port, tried_ports) do
    next_free_local_port(tried_ports)
  end

  defp next_free_local_port(tried_ports) do
    port = SSH.find_free_local_port()

    if MapSet.member?(tried_ports, port) do
      next_free_local_port(tried_ports)
    else
      port
    end
  end

  defp wait_for_local_proxy_port!(port, local_port, timeout_ms, waited \\ 0)

  defp wait_for_local_proxy_port!(port, local_port, timeout_ms, waited)
       when waited >= timeout_ms do
    details = proxy_port_failure_details(port)

    raise RuntimeError,
          "SSH proxy did not open localhost:#{local_port} within #{timeout_ms}ms#{details}"
  end

  defp wait_for_local_proxy_port!(port, local_port, timeout_ms, waited) do
    case :gen_tcp.connect(~c"127.0.0.1", local_port, [:binary, {:active, false}], 500) do
      {:ok, sock} ->
        :gen_tcp.close(sock)
        :ok

      {:error, _reason} ->
        if Port.info(port) == nil do
          details = proxy_port_failure_details(port)

          raise RuntimeError,
                "SSH proxy process exited before localhost:#{local_port} became reachable#{details}"
        else
          Process.sleep(200)
          wait_for_local_proxy_port!(port, local_port, timeout_ms, waited + 200)
        end
    end
  end

  defp proxy_port_failure_details(port) when is_port(port) do
    case TunnelManager.port_info(port) do
      {:ok, %{output: output, exit_status: exit_status}} ->
        output_part =
          output
          |> to_string()
          |> String.trim()

        status_part = if is_integer(exit_status), do: " (exit #{exit_status})", else: ""

        case output_part do
          "" ->
            status_part

          text ->
            "#{status_part}: #{text}"
        end

      _ ->
        ""
    end
  rescue
    _ -> ""
  end

  defp proxy_port_failure_details(_), do: ""

  @doc """
  Sends a chat completion request to a running vLLM OpenAI-compatible endpoint.

  `app_or_url` can be:
  - the map returned by `start_app/2` (for `app: "vllm"`), or
  - a base URL string (e.g. `"http://localhost:58886"`).

  Options:
  - `:model` (optional; auto-detected from `/v1/models` when omitted)
  - `:max_tokens` (default: `256`)
  - `:temperature` (default: `0.2`)
  - `:top_p` (optional)
  - `:top_k` or `:max_k` (optional alias; forwarded as `top_k`)
  - `:min_p` (optional)
  - `:presence_penalty` (optional)
  - `:frequency_penalty` (optional)
  - `:stop` (optional; string or list)
  - `:seed` (optional)
  - `:extra_body` (optional map; merged into request payload)
  - `:system` (optional system instruction)
  - `:timeout` ms for HTTP calls (default: `60_000`)
  - `:stream` (default: `false`)
  - `:allow_os_ssh` (default: `false`) allow fallback to OS ssh only when
    the provided session has no native `ssh_conn`

  Returns a map containing at least `:answer`, `:content`, `:model`, and `:base_url`.
  """
  @spec vllm_chat(map() | binary(), binary(), keyword()) :: map()
  def vllm_chat(app_or_url, message, opts \\ []) when is_binary(message) do
    {base_url, request_fun} = resolve_vllm_endpoint(app_or_url, opts)
    timeout_ms = Keyword.get(opts, :timeout, 60_000)
    stream? = Keyword.get(opts, :stream, false)

    model = Keyword.get(opts, :model) || fetch_first_model!(base_url, timeout_ms, request_fun)
    max_tokens = Keyword.get(opts, :max_tokens, 256)
    temperature = Keyword.get(opts, :temperature, 0.2)
    top_p = Keyword.get(opts, :top_p)
    top_k = Keyword.get(opts, :top_k, Keyword.get(opts, :max_k))
    min_p = Keyword.get(opts, :min_p)
    presence_penalty = Keyword.get(opts, :presence_penalty)
    frequency_penalty = Keyword.get(opts, :frequency_penalty)
    stop = Keyword.get(opts, :stop)
    seed = Keyword.get(opts, :seed)
    extra_body = Keyword.get(opts, :extra_body, %{})
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

    payload =
      %{
        model: model,
        messages: messages,
        max_tokens: max_tokens,
        temperature: temperature,
        stream: stream?
      }
      |> maybe_put_payload(:top_p, top_p)
      |> maybe_put_payload(:top_k, top_k)
      |> maybe_put_payload(:min_p, min_p)
      |> maybe_put_payload(:presence_penalty, presence_penalty)
      |> maybe_put_payload(:frequency_penalty, frequency_penalty)
      |> maybe_put_payload(:stop, stop)
      |> maybe_put_payload(:seed, seed)
      |> merge_chat_extra_body(extra_body)

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
            answer: content,
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
            answer: content,
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

  @doc """
  Convenience wrapper returning only the model answer text.
  """
  @spec vllm_answer(map() | binary(), binary(), keyword()) :: binary()
  def vllm_answer(app_or_url, message, opts \\ []) when is_binary(message) do
    app_or_url
    |> vllm_chat(message, opts)
    |> Map.get(:answer, "")
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
    ensure_native_session_or_allow_os!(session, opts)

    {
      "http://#{node}:#{port}",
      Keyword.get(opts, :request_fun, ssh_http_request_fun(session, node, port, job_id))
    }
  end

  defp resolve_vllm_endpoint(
         %{access_mode: :native_ssh, session: %Session{} = session, node: node, port: port},
         opts
       ) do
    ensure_native_session_or_allow_os!(session, opts)

    {
      "http://#{node}:#{port}",
      Keyword.get(opts, :request_fun, ssh_http_request_fun(session, node, port, nil))
    }
  end

  # OS proxy fallback: tunnel is already open on localhost, use base_url directly.
  defp resolve_vllm_endpoint(%{access_mode: :openssh_proxy, base_url: base_url}, opts)
       when is_binary(base_url) do
    {base_url, Keyword.get(opts, :request_fun, &default_http_request/5)}
  end

  defp resolve_vllm_endpoint(%{session: %Session{} = session, node: node, port: port}, opts)
       when is_binary(node) do
    ensure_native_session_or_allow_os!(session, opts)

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

  defp ensure_native_session_or_allow_os!(%Session{ssh_conn: nil}, opts) do
    if Keyword.get(opts, :allow_os_ssh, false) do
      :ok
    else
      raise RuntimeError,
            "vllm_chat requires a native SSH session by default (session.ssh_conn is nil). " <>
              "Use the session returned by start_app/reconnect with native_ssh enabled, " <>
              "or explicitly opt in to OS fallback via allow_os_ssh: true."
    end
  end

  defp ensure_native_session_or_allow_os!(%Session{}, _opts), do: :ok

  defp maybe_put_payload(payload, _key, nil), do: payload
  defp maybe_put_payload(payload, key, value), do: Map.put(payload, key, value)

  defp merge_chat_extra_body(payload, extra_body) when is_map(extra_body) do
    Map.merge(payload, extra_body)
  end

  defp merge_chat_extra_body(payload, _), do: payload

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

  Native mode avoids OS ssh tunneling by default. If direct native connect needs
  an explicit OpenSSH ProxyJump tunnel fallback, pass `proxy_jump_via_os: true`.

  Close with `close_connection/1` or let the session be garbage-collected.

  Options:
  - `:timeout` (ms, default 20_000)
  - `:proxy_jump_via_native` (default `true`)
  - `:proxy_jump_via_os` (default `false`)
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

  # Sleep in small chunks so interactive interrupts (Ctrl+C in IEx) are handled promptly.
  defp interruptible_sleep(ms) when not is_integer(ms) or ms <= 0, do: :ok

  defp interruptible_sleep(ms) do
    step = min(ms, 200)

    receive do
      {:hpc_connect_cancel_wait, reason} ->
        throw({:hpc_connect_cancel_wait, reason})
    after
      step -> :ok
    end

    interruptible_sleep(ms - step)
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
