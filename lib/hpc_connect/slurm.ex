defmodule HpcConnect.Slurm do
  @moduledoc """
  SLURM helpers: query cluster partitions, parse sinfo output, find free nodes,
  check/submit jobs, and build module-load preambles for sbatch scripts.
  """

  alias HpcConnect.{Model, Scripts, Session, Shell, SSH}

  # Node states considered usable (idle = fully free, mix = partially free)
  @free_states ~w(idle mix)

  # Node states that are definitively unavailable
  @unavailable_states ~w(down drain)

  # ---------------------------------------------------------------------------
  # Types
  # ---------------------------------------------------------------------------

  @type node_info :: %{
          partition: binary(),
          avail: binary(),
          timelimit: binary(),
          nodes: non_neg_integer(),
          state: binary(),
          nodelist: binary()
        }

  @type partition_summary :: %{
          partition: binary(),
          free_nodes: non_neg_integer(),
          mixed_nodes: non_neg_integer(),
          idle_nodes: non_neg_integer(),
          alloc_nodes: non_neg_integer(),
          sample_idle: [binary()],
          sample_mixed: [binary()]
        }

  @type allocation_result :: %{
          raw: binary(),
          node: binary() | nil,
          partition: binary(),
          gpus: pos_integer(),
          walltime: binary()
        }

  @type quota_entry :: %{
          path: binary(),
          used: binary(),
          soft_quota: binary(),
          hard_quota: binary(),
          grace_time: binary(),
          file_count: binary(),
          file_quota: binary(),
          file_hard_quota: binary(),
          file_grace: binary(),
          warning?: boolean(),
          note: binary() | nil
        }

  @type job_entry :: %{
          job_id: binary(),
          partition: binary(),
          name: binary(),
          user: binary(),
          state: binary(),
          time: binary(),
          time_limit: binary(),
          nodes: binary(),
          cpus: binary(),
          node_list: binary()
        }

  # ---------------------------------------------------------------------------
  # Raw sinfo query
  # ---------------------------------------------------------------------------

  @doc """
  Runs `sinfo` on the remote cluster and returns the raw output string.

  Options:
  - `:partition` – limit to a specific partition (e.g. `"a100"`)
  - `:connect_opts` – keyword options forwarded to `SSH.run/2`
  """
  @spec query_sinfo(Session.t(), keyword()) :: binary()
  def query_sinfo(%Session{} = session, opts \\ []) do
    partition_filter =
      case Keyword.get(opts, :partition) do
        nil -> ""
        p -> " -p #{p}"
      end

    cmd = "sinfo#{partition_filter}"

    {output, _exit} =
      session |> SSH.ssh_command(cmd, "sinfo") |> SSH.run(Keyword.get(opts, :connect_opts, []))

    output
  end

  # ---------------------------------------------------------------------------
  # Parsing
  # ---------------------------------------------------------------------------

  @doc """
  Parses the text output of `sinfo` into a list of `node_info` maps.

  Handles the default `sinfo` columns:
      PARTITION AVAIL  TIMELIMIT  NODES  STATE NODELIST
  """
  @spec parse_sinfo(binary()) :: [node_info()]
  def parse_sinfo(raw) when is_binary(raw) do
    raw
    |> String.split("\n", trim: true)
    |> Enum.drop(1)
    |> Enum.flat_map(&parse_sinfo_line/1)
  end

  defp parse_sinfo_line(line) do
    case String.split(line) do
      [partition, avail, timelimit, nodes_str, state | nodelist_parts] ->
        [
          %{
            partition: String.trim_trailing(partition, "*"),
            avail: avail,
            timelimit: timelimit,
            nodes: parse_int(nodes_str),
            state: normalize_state(state),
            nodelist: Enum.join(nodelist_parts, " ")
          }
        ]

      _ ->
        []
    end
  end

  defp parse_int(s) do
    case Integer.parse(s) do
      {n, _} -> n
      :error -> 0
    end
  end

  # Normalise variant state strings like "mix-", "drain*", "down*" → base name
  defp normalize_state(state) do
    state
    |> String.trim_trailing("*")
    |> String.trim_trailing("-")
  end

  # ---------------------------------------------------------------------------
  # Free node detection
  # ---------------------------------------------------------------------------

  @doc """
  Returns all `node_info` rows where the state is `idle` or `mix`.

  Pass `state: :idle` to restrict to fully free nodes only.
  """
  @spec free_nodes([node_info()], keyword()) :: [node_info()]
  def free_nodes(rows, opts \\ []) do
    states =
      case Keyword.get(opts, :state) do
        :idle -> ["idle"]
        :mix -> ["mix"]
        _ -> @free_states
      end

    Enum.filter(rows, fn row ->
      row.state in states and row.nodes > 0 and row.avail == "up"
    end)
  end

  @doc """
  Returns `node_info` rows that are definitively unavailable (down / drain).
  """
  @spec unavailable_nodes([node_info()]) :: [node_info()]
  def unavailable_nodes(rows) do
    Enum.filter(rows, fn row -> row.state in @unavailable_states end)
  end

  @doc """
  Summarises free/mixed/alloc node counts per partition from a parsed sinfo list.
  """
  @spec partition_summary([node_info()]) :: [partition_summary()]
  def partition_summary(rows) do
    rows
    |> Enum.group_by(& &1.partition)
    |> Enum.map(fn {partition, partition_rows} ->
      idle_rows = Enum.filter(partition_rows, &(&1.state == "idle"))
      mix_rows = Enum.filter(partition_rows, &(&1.state == "mix"))
      alloc_rows = Enum.filter(partition_rows, &(&1.state == "alloc"))

      %{
        partition: partition,
        idle_nodes: Enum.sum(Enum.map(idle_rows, & &1.nodes)),
        mixed_nodes: Enum.sum(Enum.map(mix_rows, & &1.nodes)),
        alloc_nodes: Enum.sum(Enum.map(alloc_rows, & &1.nodes)),
        free_nodes:
          Enum.sum(Enum.map(idle_rows, & &1.nodes)) +
            Enum.sum(Enum.map(mix_rows, & &1.nodes)),
        sample_idle: Enum.map(idle_rows, & &1.nodelist),
        sample_mixed: Enum.map(mix_rows, & &1.nodelist)
      }
    end)
    |> Enum.sort_by(& &1.free_nodes, :desc)
  end

  @doc """
  One-call convenience: SSH → parse → summarise.

  Returns `{summaries, raw_output}`.
  """
  @spec check_free_nodes(Session.t(), keyword()) :: {[partition_summary()], binary()}
  def check_free_nodes(%Session{} = session, opts \\ []) do
    raw = query_sinfo(session, opts)
    rows = parse_sinfo(raw)
    summaries = partition_summary(rows)
    {summaries, raw}
  end

  @doc """
  Convenience alias for `check_free_nodes/2` with a more task-oriented name.
  """
  @spec available_gpus(Session.t(), keyword()) :: {[partition_summary()], binary()}
  def available_gpus(%Session{} = session, opts \\ []) do
    check_free_nodes(session, opts)
  end

  # ---------------------------------------------------------------------------
  # Quota query
  # ---------------------------------------------------------------------------

  @doc """
  Runs `shownicerquota.pl` on the remote and returns the raw output.
  """
  @spec query_quota(Session.t(), keyword()) :: binary()
  def query_quota(%Session{} = session, opts \\ []) do
    {output, _} =
      session
      |> SSH.ssh_command("shownicerquota.pl", "quota")
      |> SSH.run(Keyword.get(opts, :connect_opts, []))

    output
  end

  # ---------------------------------------------------------------------------
  # Module-load preamble builder
  # ---------------------------------------------------------------------------

  @default_modules [
    "python/3.12-conda",
    "cuda/12.6.2",
    "cudnn/9.2.0.82-12"
  ]

  @doc """
  Returns the default FAU/NHR HPC environment modules.
  """
  @spec default_modules() :: [binary()]
  def default_modules, do: @default_modules

  @doc """
  Builds a shell preamble that loads the given modules and optionally activates
  a conda environment.

  Options:
  - `:modules` – list of module names (default: `default_modules/0`)
  - `:conda_env` – name of the conda env to activate (optional)
  - `:source_bash_profile` – whether to source `~/.bash_profile` (default: `true`)
  """
  @spec module_load_preamble(keyword()) :: binary()
  def module_load_preamble(opts \\ []) do
    modules = Keyword.get(opts, :modules, @default_modules)
    conda_env = Keyword.get(opts, :conda_env)
    source_profile? = Keyword.get(opts, :source_bash_profile, true)

    lines =
      []
      |> maybe_prepend(
        source_profile?,
        "[ -f ~/.bash_profile ] && source ~/.bash_profile || true"
      )
      |> maybe_append(true, "[ -f ~/.bashrc ] && source ~/.bashrc || true")
      |> maybe_append(
        true,
        "[ -f /etc/profile.d/modules.sh ] && source /etc/profile.d/modules.sh || true"
      )
      |> Kernel.++(Enum.map(modules, fn m -> "module load #{m}" end))
      |> maybe_append(is_binary(conda_env), "eval \"$(conda shell.bash hook)\"")
      |> maybe_append(is_binary(conda_env), "conda activate #{conda_env}")

    Enum.join(lines, " && ")
  end

  defp maybe_prepend(list, true, item), do: [item | list]
  defp maybe_prepend(list, false, _item), do: list

  defp maybe_append(list, true, item), do: list ++ [item]
  defp maybe_append(list, false, _item), do: list

  # ---------------------------------------------------------------------------
  # Job status queries
  # ---------------------------------------------------------------------------

  @doc """
  Returns the output of `squeue -u <username>` for the session user.
  """
  @spec query_jobs(Session.t(), keyword()) :: binary()
  def query_jobs(%Session{} = session, opts \\ []) do
    user = session.username || raise ArgumentError, "session has no username"

    {output, _} =
      session
      |> SSH.ssh_command("squeue -u #{user}", "squeue")
      |> SSH.run(Keyword.get(opts, :connect_opts, []))

    output
  end

  @doc """
  Cancels a SLURM job by ID.
  """
  @spec cancel_job(Session.t(), binary() | pos_integer(), keyword()) :: binary()
  def cancel_job(%Session{} = session, job_id, opts \\ []) do
    {output, _} =
      session
      |> SSH.ssh_command("scancel #{job_id}", "scancel #{job_id}")
      |> SSH.run(Keyword.get(opts, :connect_opts, []))

    output
  end

  @doc """
  Parses `shownicerquota.pl` output into structured entries.
  """
  @spec parse_quota(binary()) :: [quota_entry()]
  def parse_quota(output) when is_binary(output) do
    output
    |> String.split("\n", trim: true)
    |> Enum.drop(1)
    |> Enum.map(&parse_quota_line/1)
    |> Enum.reject(&is_nil/1)
  end

  defp parse_quota_line(line) do
    warning? = String.starts_with?(line, "!!!")

    cleaned =
      line
      |> String.trim()
      |> String.trim_leading("!")
      |> String.trim()

    note =
      case Regex.run(~r/\(([^)]+)\)\s*$/, cleaned, capture: :all_but_first) do
        [value] -> value
        _ -> nil
      end

    cleaned = Regex.replace(~r/\s*\([^)]+\)\s*$/, cleaned, "")

    case Regex.split(~r/\s+/, cleaned, trim: true) do
      [path, used, soft_q, hard_q, grace_time, file_count, file_q, file_hard_q, file_grace] ->
        %{
          path: path,
          used: used,
          soft_quota: soft_q,
          hard_quota: hard_q,
          grace_time: grace_time,
          file_count: file_count,
          file_quota: file_q,
          file_hard_quota: file_hard_q,
          file_grace: file_grace,
          warning?: warning?,
          note: note
        }

      _ ->
        nil
    end
  end

  @doc """
  Parses `squeue -u <user>` output into structured entries.
  """
  @spec parse_jobs(binary()) :: [job_entry()]
  def parse_jobs(output) when is_binary(output) do
    output
    |> String.split("\n", trim: true)
    |> Enum.drop(1)
    |> Enum.map(&parse_job_line/1)
    |> Enum.reject(&is_nil/1)
  end

  defp parse_job_line(line) do
    case Regex.split(~r/\s+/, String.trim(line), parts: 10, trim: true) do
      [job_id, partition, name, user, state, time, time_limit, nodes, cpus, node_list] ->
        %{
          job_id: job_id,
          partition: partition,
          name: name,
          user: user,
          state: state,
          time: time,
          time_limit: time_limit,
          nodes: nodes,
          cpus: cpus,
          node_list: node_list
        }

      _ ->
        nil
    end
  end

  @doc """
  Allocates a GPU node persistently via `salloc --no-shell` and returns a map
  with the allocation details including a live Erlang Port that keeps the job alive.

  Returns `%{port, job_id, node, partition, gpus, walltime}`.

  The allocation remains active as long as the port lives. Release with
  `HpcConnect.release_gpu/2`, which closes the port and cancels the SLURM job.

  Options: `:partition`, `:gpus`, `:walltime`, `:ntasks`, `:timeout` (ms to wait for RUNNING, default 300_000), `:interval` (ms between squeue polls, default 10_000).
  """
  @spec allocate_gpu(Session.t(), keyword()) :: map()
  def allocate_gpu(%Session{} = session, opts \\ []) do
    partition = to_string(Keyword.get(opts, :partition, "a100"))
    gpus = Keyword.get(opts, :gpus, 1)
    walltime = Keyword.get(opts, :walltime, "01:00:00")
    ntasks = Keyword.get(opts, :ntasks, 1)

    # Submit a sleep-hold batch job.  sbatch exits immediately (no CMD window, no Port)
    # and the job keeps the allocation alive on the cluster for `walltime`.
    sleep_secs = walltime_to_seconds(walltime)

    log_dir = Path.join(session.work_dir, "sbatch_logs")

    sbatch_cmd =
      "mkdir -p #{Shell.escape(log_dir)}" <>
        " && sbatch --parsable --partition=#{partition} --gres=gpu:#{gpus}" <>
        " --ntasks=#{ntasks} --time=#{walltime} --job-name=hpc_connect_hold" <>
        " --output=#{Shell.escape(log_dir)}/hpc_connect_hold_%j.out" <>
        " --error=#{Shell.escape(log_dir)}/hpc_connect_hold_%j.err" <>
        " --wrap=\"sleep #{sleep_secs}\""

    {output, status} =
      session
      |> SSH.ssh_command(sbatch_cmd, "Allocate #{gpus} GPU(s) on #{partition}")
      |> SSH.run(Keyword.get(opts, :connect_opts, []))

    if status != 0, do: raise(RuntimeError, "sbatch failed: #{output}")

    job_id = output |> String.trim() |> String.split(";") |> List.first()

    unless job_id =~ ~r/^\d+$/ do
      raise RuntimeError, "sbatch returned unexpected output: #{inspect(output)}"
    end

    node =
      wait_for_job_node(session, job_id,
        timeout: Keyword.get(opts, :timeout, 300_000),
        interval: Keyword.get(opts, :interval, 10_000)
      )

    %{
      job_id: job_id,
      node: node,
      partition: partition,
      gpus: gpus,
      walltime: walltime,
      session: session
    }
  end

  # Convert "HH:MM:SS" (or "MM:SS" / "D-HH:MM:SS") to total seconds for the sleep command.
  defp walltime_to_seconds(walltime) do
    parts =
      walltime
      |> String.split(["-", ":"])
      |> Enum.map(&String.to_integer/1)

    case parts do
      [d, h, m, s] -> d * 86_400 + h * 3_600 + m * 60 + s
      [h, m, s] -> h * 3_600 + m * 60 + s
      [m, s] -> m * 60 + s
      [s] -> s
    end
  end

  @doc """
  Allocates a GPU node persistently by opening `salloc --no-shell` as a background
  OS process (Erlang Port). The allocation is held for the lifetime of the returned port.

  Returns `%{port, job_id, node, partition, gpus, walltime}`.

  The allocation is released by calling `release_gpu/2`, which closes the port and
  cancels the SLURM job. It is also automatically released if the calling process dies.

  Options: same as `allocate_gpu/2` plus `:timeout` (ms, default 300_000).

  Deprecated: use `allocate_gpu/2` instead.
  """
  @spec hold_gpu(Session.t(), keyword()) :: map()
  def hold_gpu(%Session{} = session, opts \\ []), do: allocate_gpu(session, opts)

  # ---------------------------------------------------------------------------
  # vLLM batch job submission
  # ---------------------------------------------------------------------------

  @type vllm_job_result :: %{
          job_id: binary(),
          partition: binary(),
          gpus: pos_integer(),
          walltime: binary(),
          port: pos_integer(),
          logs_dir: binary()
        }

  @doc """
  Submits a vLLM batch job via `sbatch` and returns the SLURM job ID.

  Options:
  - `:partition` – GPU partition (default: `"a100"`)
  - `:gpus` – number of GPUs (default: `1`)
  - `:walltime` – time limit (default: `"02:00:00"`)
  - `:port` – port for vLLM server (default: `8000`)
  - `:cpus_per_task` – CPUs per task (default: `8`)
  - `:modules` – list of modules to load (default: `[]`)
  - `:conda_env` – conda env to activate (optional)
  """
  @spec submit_vllm_job(Session.t(), Model.t(), keyword()) :: vllm_job_result()
  def submit_vllm_job(%Session{} = session, %Model{} = model, opts \\ []) do
    partition =
      to_string(Keyword.get(opts, :partition, session.cluster.default_partition || "a100"))

    gpus = Keyword.get(opts, :gpus, 1)
    walltime = Keyword.get(opts, :walltime, "02:00:00")
    port = Keyword.get(opts, :port, 8000)
    ntasks = Keyword.get(opts, :ntasks, 1)
    cpus = Keyword.get(opts, :cpus_per_task, 8)
    modules = Keyword.get(opts, :modules, [])
    conda_env = Keyword.get(opts, :conda_env)

    script = Path.join(Scripts.remote_script_dir(session), "start_vllm.sh")
    model_dir = Model.remote_dir(session, model)
    logs_dir = Path.join(session.work_dir, "sbatch_logs")
    job_script_path = Path.join(session.work_dir, "vllm_submit.sh")

    module_lines =
      if modules == [] do
        ""
      else
        modules |> Enum.map(&"module load #{&1}") |> Enum.join("\n")
      end

    conda_line =
      if conda_env do
        "eval \"$(conda shell.bash hook)\"\nconda activate #{conda_env}"
      else
        ""
      end

    job_script = """
    #!/bin/bash -l
    #SBATCH --job-name=hpc_connect_vllm
    #SBATCH --partition=#{partition}
    #SBATCH --gres=gpu:#{gpus}
    #SBATCH --ntasks=#{ntasks}
    #SBATCH --cpus-per-task=#{cpus}
    #SBATCH --time=#{walltime}
    #SBATCH --output=#{logs_dir}/vllm_%j.out
    #SBATCH --error=#{logs_dir}/vllm_%j.err

    #{module_lines}
    #{conda_line}

    exec bash #{script} --model #{model_dir} --port #{port}
    """

    b64 = job_script |> Base.encode64() |> String.replace("\n", "")

    remote =
      "mkdir -p #{Shell.escape(logs_dir)} && " <>
        "echo #{b64} | base64 -d > #{Shell.escape(job_script_path)} && " <>
        "sbatch --parsable #{Shell.escape(job_script_path)}"

    {output, status} =
      session
      |> SSH.ssh_command(remote, "Submit vLLM batch job")
      |> SSH.run(Keyword.get(opts, :connect_opts, []))

    if status != 0, do: raise(RuntimeError, "sbatch failed: #{output}")

    %{
      job_id: String.trim(output),
      partition: partition,
      gpus: gpus,
      walltime: walltime,
      port: port,
      logs_dir: logs_dir
    }
  end

  # ---------------------------------------------------------------------------
  # Job node queries
  # ---------------------------------------------------------------------------

  @doc """
  Returns the compute node for a running SLURM job, or `nil` if not yet running.

  Uses `squeue -j <job_id> -h -o "%N"`.
  """
  @spec get_job_node(Session.t(), binary() | pos_integer(), keyword()) :: binary() | nil
  def get_job_node(%Session{} = session, job_id, opts \\ []) do
    {output, _} =
      session
      |> SSH.ssh_command(
        "squeue -j #{job_id} -h -o '%N' 2>/dev/null || true",
        "Get node for job #{job_id}"
      )
      |> SSH.run(Keyword.get(opts, :connect_opts, []))

    node = String.trim(output)

    if node in ["", "(null)", "None"] do
      nil
    else
      node
    end
  end

  @doc """
  Polls `squeue` until the compute node is assigned (job state RUNNING).

  Options:
  - `:timeout` – ms to wait total (default: `300_000` = 5 min)
  - `:interval` – ms between polls (default: `10_000` = 10 s)
  """
  @spec wait_for_job_node(Session.t(), binary() | pos_integer(), keyword()) :: binary()
  def wait_for_job_node(%Session{} = session, job_id, opts \\ []) do
    timeout_ms = Keyword.get(opts, :timeout, 300_000)
    interval_ms = Keyword.get(opts, :interval, 10_000)
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_node(session, job_id, interval_ms, deadline, opts)
  end

  defp do_wait_for_node(session, job_id, interval_ms, deadline, opts) do
    case get_job_node(session, job_id, opts) do
      nil ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(interval_ms)
          do_wait_for_node(session, job_id, interval_ms, deadline, opts)
        else
          raise RuntimeError,
                "Job #{job_id} did not reach RUNNING state within the timeout"
        end

      node ->
        node
    end
  end

  # ---------------------------------------------------------------------------
  # Singularity / Apptainer image management
  # ---------------------------------------------------------------------------

  @doc """
  Returns the remote path to a Singularity definition file.

  Files live under `<work_dir>/singularity_def_files/<name>.def`.
  """
  @spec remote_def_path(Session.t(), binary()) :: binary()
  def remote_def_path(%Session{} = session, name \\ "vllm") do
    Path.join([session.work_dir, "singularity_def_files", "#{name}.def"])
  end

  @doc """
  Returns the remote path to a built Singularity image.

  Images live under `<work_dir>/singularity_images/<name>.sif`.
  """
  @spec remote_sif_path(Session.t(), binary()) :: binary()
  def remote_sif_path(%Session{} = session, name \\ "vllm") do
    Path.join([session.work_dir, "singularity_images", "#{name}.sif"])
  end

  @doc """
  Uploads a local Singularity definition file to the remote cluster.

  `local_def_path` defaults to the bundled `priv/scripts/<name>.def` when `nil`.
  Creates the remote `singularity_def_files/` directory if missing.

  Returns the remote path the file was written to.
  """
  @spec upload_def_file(Session.t(), binary(), binary() | nil) :: binary()
  def upload_def_file(%Session{} = session, name \\ "vllm", local_def_path \\ nil) do
    local_path =
      local_def_path ||
        Path.join([
          to_string(:code.priv_dir(:hpc_connect)),
          "scripts",
          "#{name}.def"
        ])

    remote_dir = Path.join(session.work_dir, "singularity_def_files")
    remote_path = remote_def_path(session, name)

    {_, 0} =
      session
      |> SSH.ssh_command("mkdir -p #{Shell.escape(remote_dir)}", "Create def files dir")
      |> SSH.run()

    SSH.scp_to_command(session, local_path, remote_path, "Upload #{name}.def")
    |> SSH.run()

    remote_path
  end

  @doc """
  Submits an Apptainer build job via `sbatch` (CPU-only, no GPU needed).

  Uploads the definition file, then submits `build_sif.sh` as a batch job.
  Returns `%{job_id, sif_path, def_path}` immediately — use `wait_for_job_done/3`
  or poll `squeue -j <job_id>` to know when the build finishes.

  Options:
  - `:name`            – image/def stem name (default: `"vllm"`)
  - `:local_def_path`  – override local .def file (default: bundled `priv/scripts/<name>.def`)
  - `:partition`       – partition for build job (default: `"standard"` or cluster default)
  - `:walltime`        – build time limit (default: `"02:00:00"`)
  - `:cpus`            – CPUs for build (default: `4`)
  - `:force_rebuild`   – set `"1"` to rebuild even if .sif exists (default: `"0"`)
  """
  @spec build_sif_job(Session.t(), keyword() | binary()) :: map()
  def build_sif_job(session, args \\ [])

  def build_sif_job(%Session{} = session, name) when is_binary(name) do
    build_sif_job(session, name: name)
  end

  def build_sif_job(%Session{} = session, opts) do
    {name, opts} =
      case opts do
        name when is_binary(name) -> {name, []}
        opts when is_list(opts) -> {Keyword.get(opts, :name, "vllm"), opts}
      end

    local_def = Keyword.get(opts, :local_def_path)

    def_path =
      if is_binary(local_def) do
        upload_def_file(session, name, local_def)
      else
        remote_path = remote_def_path(session, name)

        if remote_def_exists?(session, remote_path) do
          remote_path
        else
          upload_def_file(session, name, nil)
        end
      end

    sif_path = remote_sif_path(session, name)
    tmp_dir = Path.join(session.work_dir, "apptainer_tmp")
    logs_dir = Path.join(session.work_dir, "logs")
    force_rebuild = Keyword.get(opts, :force_rebuild, false)

    if not force_rebuild and remote_file_exists?(session, sif_path) do
      %{
        job_id: nil,
        sif_path: sif_path,
        def_path: def_path,
        name: name
      }
    else
      build_cmd =
        "mkdir -p #{Shell.escape(tmp_dir)} #{Shell.escape(logs_dir)} && " <>
          "export APPTAINER_TMPDIR=#{Shell.escape(tmp_dir)} && " <>
          "apptainer build #{if force_rebuild, do: "--force ", else: ""}#{Shell.escape(sif_path)} #{Shell.escape(def_path)} " <>
          ">> #{Shell.escape(logs_dir)}/build_sif_#{name}.log 2>&1 && " <>
          "echo #{Shell.escape(sif_path)}"

      {output, status} =
        session
        |> SSH.ssh_command(build_cmd, "Build Apptainer image for #{name}.sif")
        |> SSH.run(Keyword.get(opts, :connect_opts, []))

      if status != 0, do: raise(RuntimeError, "apptainer build failed: #{output}")

      %{
        job_id: nil,
        sif_path: String.trim(output),
        def_path: def_path,
        name: name
      }
    end
  end

  defp remote_def_exists?(session, remote_path) do
    {output, _} =
      session
      |> SSH.ssh_command(
        "test -f #{Shell.escape(remote_path)} && echo ok || true",
        "Check remote def exists"
      )
      |> SSH.run()

    String.trim(output) == "ok"
  end

  defp remote_file_exists?(session, remote_path) do
    {output, _} =
      session
      |> SSH.ssh_command(
        "test -f #{Shell.escape(remote_path)} && echo ok || true",
        "Check remote file exists"
      )
      |> SSH.run()

    String.trim(output) == "ok"
  end

  @doc """
  Uploads the .def file, submits the build job, then **blocks** until the job
  finishes, and returns the sif path.

  Options: same as `build_sif_job/2` plus `:timeout` (ms, default 3_600_000 = 1h),
  `:interval` (ms between polls, default 15_000).

  Raises if the job does not finish within the timeout.
  """
  @spec build_sif_blocking(Session.t(), keyword() | binary()) :: binary()
  def build_sif_blocking(session, opts \\ [])

  def build_sif_blocking(%Session{} = session, name) when is_binary(name) do
    build_sif_blocking(session, name: name)
  end

  def build_sif_blocking(%Session{} = session, opts) do
    %{sif_path: sif_path} = build_sif_job(session, opts)
    sif_path
  end

  @doc """
  Submits a generalized Apptainer application job via `sbatch`.

  The app name is used to find the startup script at `work_dir/scripts/start_<app>.sh`.
  All dynamic variables are injected as `--export` into sbatch.

  Returns `%{job_id, node, partition, gpus, walltime, port, sif_path, logs_dir}`.
  The `node` field is populated after up to 10 seconds of polling; if still nil,
  the job is waiting for resource availability.

  Options:
  - `:partition`     – GPU partition (default: cluster default or `"a100"`)
  - `:gpus`          – number of GPUs (default: `1`)
  - `:walltime`      – time limit (default: `"02:00:00"`)
  - `:port`          – app port (default: `8000`)
  - `:cpus`          – cpus-per-task (default: `8`)
  - `:sif_name`      – stem name of the .sif (default: app name)
  - `:sif_path`      – override full remote sif path
  - `:app_env`       – extra env vars to pass (map, default: `%{}`)
  """
  @spec submit_apptainer(Session.t(), keyword()) :: map()
  def submit_apptainer(%Session{} = session, opts \\ []) do
    app = Keyword.get(opts, :app) || raise ArgumentError, ":app is required"

    partition =
      to_string(Keyword.get(opts, :partition, session.cluster.default_partition || "a100"))

    gpus = Keyword.get(opts, :gpus, 1)
    walltime = Keyword.get(opts, :walltime, "02:00:00")
    port = Keyword.get(opts, :port, 8000)
    cpus = Keyword.get(opts, :cpus, 8)
    sif_name = Keyword.get(opts, :sif_name, app)
    sif_path = Keyword.get(opts, :sif_path) || remote_sif_path(session, sif_name)
    app_env = Keyword.get(opts, :app_env, %{})
    args = Keyword.get(opts, :args, [])

    logs_dir = Path.join(session.work_dir, "sbatch_logs")
    run_script = Path.join(Scripts.remote_script_dir(session), "start_#{app}.sh")

    app_args = args_to_cli_string(args)

    export_vars =
      [
        "HPC_MODELS_DIR=#{Shell.escape(session.vault_dir)}",
        "HPC_WORK_DIR=#{Shell.escape(session.work_dir)}",
        "HPC_SIF_PATH=#{Shell.escape(sif_path)}",
        "APP_PORT=#{port}"
      ]
      |> Kernel.++(Enum.map(app_env, fn {k, v} -> "#{k}=#{Shell.escape(to_string(v))}" end))
      |> Kernel.++(if app_args != "", do: ["APP_ARGS=#{Shell.escape(app_args)}"], else: [])
      |> Enum.join(",")

    sbatch_cmd =
      "mkdir -p #{Shell.escape(logs_dir)}" <>
        " && sbatch --parsable" <>
        " --job-name=hpc_connect_#{app}" <>
        " --partition=#{partition}" <>
        " --gres=gpu:#{gpus}" <>
        " --ntasks=1" <>
        " --cpus-per-task=#{cpus}" <>
        " --time=#{walltime}" <>
        " --output=#{Shell.escape(logs_dir)}/#{app}_%j.out" <>
        " --error=#{Shell.escape(logs_dir)}/#{app}_%j.err" <>
        " --export=#{export_vars}" <>
        " #{Shell.escape(run_script)}"

    {output, status} =
      session
      |> SSH.ssh_command(sbatch_cmd, "Submit #{app} Apptainer job")
      |> SSH.run(Keyword.get(opts, :connect_opts, []))

    if status != 0, do: raise(RuntimeError, "sbatch failed: #{output}")

    job_id = output |> String.trim() |> String.split(";") |> List.first()

    node =
      case get_job_node_quick(session, job_id, timeout_ms: 10_000) do
        {:ok, node} -> node
        :timeout -> nil
      end

    %{
      job_id: job_id,
      node: node,
      partition: partition,
      gpus: gpus,
      walltime: walltime,
      port: port,
      sif_path: sif_path,
      logs_dir: logs_dir
    }
  end

  defp get_job_node_quick(session, job_id, opts) do
    timeout_ms = Keyword.get(opts, :timeout_ms, 10_000)
    interval_ms = Keyword.get(opts, :interval_ms, 1_000)
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_poll_node_quick(session, job_id, interval_ms, deadline)
  end

  defp do_poll_node_quick(session, job_id, interval_ms, deadline) do
    case get_job_node(session, job_id) do
      nil ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(interval_ms)
          do_poll_node_quick(session, job_id, interval_ms, deadline)
        else
          :timeout
        end

      node ->
        {:ok, node}
    end
  end

  defp args_to_cli_string(args) when is_list(args) do
    args
    |> Enum.map(fn
      {k, v} -> "--#{k} #{Shell.escape(to_string(v))}"
      arg when is_binary(arg) -> arg
      other -> to_string(other)
    end)
    |> Enum.join(" ")
  end

  @doc """
  Submits a vLLM inference job using a pre-built Apptainer SIF image.

  Deprecated: use `submit_apptainer/2` instead for generalized app submission.
  This is now a wrapper around `submit_apptainer/2`.

  Options:
  - `:partition`     – GPU partition (default: cluster default or `"a100"`)
  - `:gpus`          – number of GPUs (default: `1`)
  - `:walltime`      – time limit (default: `"02:00:00"`)
  - `:port`          – vLLM port (default: `8000`)
  - `:cpus`          – cpus-per-task (default: `8`)
  - `:sif_name`      – stem name of the .sif (default: `"vllm"`)
  - `:sif_path`      – override full remote sif path
  - `:tensor_parallel` – TP size (default: `1`)
  - `:gpu_mem_util`  – GPU memory fraction (default: `"0.90"`)
  - `:max_model_len` – max context tokens (default: `8192`)
  - `:hf_token`      – HuggingFace token env value
  """
  @spec submit_vllm_apptainer(Session.t(), Model.t() | binary(), keyword()) :: map()
  def submit_vllm_apptainer(%Session{} = session, model_or_repo, opts \\ []) do
    model_repo =
      if is_binary(model_or_repo) do
        model_or_repo
      else
        model_or_repo.repo_id
      end

    tp = Keyword.get(opts, :tensor_parallel, 1)
    gpu_mem = Keyword.get(opts, :gpu_mem_util, "0.90")
    max_len = Keyword.get(opts, :max_model_len, 8192)
    hf_token = Keyword.get(opts, :hf_token) || Map.get(session.env, "HUGGINGFACE_HUB_TOKEN", "")

    app_env =
      %{
        "VLLM_MODEL" => model_repo,
        "VLLM_TP" => tp,
        "VLLM_GPU_MEM" => gpu_mem,
        "VLLM_MAX_LEN" => max_len
      }
      |> then(fn env ->
        if hf_token != "", do: Map.put(env, "HF_TOKEN", hf_token), else: env
      end)

    submit_apptainer(session,
      app: "vllm",
      app_env: app_env,
      partition: Keyword.get(opts, :partition),
      gpus: Keyword.get(opts, :gpus),
      walltime: Keyword.get(opts, :walltime),
      port: Keyword.get(opts, :port),
      cpus: Keyword.get(opts, :cpus),
      sif_name: Keyword.get(opts, :sif_name),
      sif_path: Keyword.get(opts, :sif_path)
    )
  end
end
