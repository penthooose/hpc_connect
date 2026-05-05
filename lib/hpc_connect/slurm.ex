defmodule HpcConnect.Slurm do
  @moduledoc """
  SLURM helpers: query cluster partitions, parse sinfo output, find free nodes,
  check/submit jobs, and build module-load preambles for sbatch scripts.
  """

  require Logger
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

    {output, status} =
      session |> SSH.ssh_command(cmd, "sinfo") |> SSH.run(Keyword.get(opts, :connect_opts, []))

    if status != 0, do: raise(RuntimeError, "sinfo failed (exit #{status}): #{output}")
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
    |> Enum.map(&strip_ansi/1)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.drop_while(&header_or_warning_sinfo_line?/1)
    |> Enum.flat_map(&parse_sinfo_line/1)
  end

  defp parse_sinfo_line(line) do
    case String.split(line) do
      [partition, avail, timelimit, nodes_str, state | nodelist_parts] ->
        if valid_sinfo_row?(partition, avail, timelimit, nodes_str) do
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
        else
          []
        end

      _ ->
        []
    end
  end

  defp valid_sinfo_row?(_partition, _avail, _timelimit, nodes_str) do
    nodes_str =~ ~r/^\d+$/ and not String.contains?(nodes_str, "WARNING")
  end

  defp header_or_warning_sinfo_line?(line) do
    line == "" or
      String.starts_with?(line, "PARTITION") or
      String.starts_with?(line, "WARNING") or
      String.starts_with?(line, "You are over quota") or
      String.starts_with?(line, "!!!") or
      String.starts_with?(line, "Path")
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
    {output, status} =
      session
      |> SSH.ssh_command("shownicerquota.pl", "quota")
      |> SSH.run(Keyword.get(opts, :connect_opts, []))

    if status != 0,
      do: raise(RuntimeError, "shownicerquota.pl failed (exit #{status}): #{output}")

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

    {output, status} =
      session
      |> SSH.ssh_command("squeue -u #{user} -o '%i %P %j %u %t %M %l %D %C %R'", "squeue")
      |> SSH.run(Keyword.get(opts, :connect_opts, []))

    if status != 0, do: raise(RuntimeError, "squeue failed (exit #{status}): #{output}")
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
    |> Enum.map(&strip_ansi/1)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.reject(&header_or_warning_line?/1)
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
        if path == "Path" or used == "Used" or not String.starts_with?(path, "/") do
          nil
        else
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
        end

      _ ->
        nil
    end
  end

  defp header_or_warning_line?(line) do
    line == "" or
      String.starts_with?(line, "Path") or
      String.starts_with?(line, "WARNING") or
      String.starts_with?(line, "You are over quota") or
      String.starts_with?(line, "!!!")
  end

  defp strip_ansi(value) do
    Regex.replace(~r/\e\[[0-9;]*m/, value, "")
  end

  @doc """
  Parses `squeue -u <user>` output into structured entries.
  """
  @spec parse_jobs(binary()) :: [job_entry()]
  def parse_jobs(output) when is_binary(output) do
    output
    |> String.split("\n", trim: true)
    |> Enum.map(&strip_ansi/1)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.filter(&valid_squeue_line?/1)
    |> Enum.map(&parse_job_line/1)
    |> Enum.reject(&is_nil/1)
  end

  defp valid_squeue_line?(line) do
    case String.split(line, ~r/\s+/, parts: 2, trim: true) do
      [job_id, _rest] -> String.match?(job_id, ~r/^\d+$/)
      _ -> false
    end
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

      [job_id, partition, name, user, state, time, nodes, node_list] ->
        %{
          job_id: job_id,
          partition: partition,
          name: name,
          user: user,
          state: state,
          time: time,
          time_limit: "",
          nodes: nodes,
          cpus: "",
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

  Options: `:partition`, `:gpus`, `:walltime`, `:ntasks` (optional),
  `:timeout` (ms to wait for RUNNING, default 300_000),
  `:interval` (ms between squeue polls, default 10_000).
  """
  @spec allocate_gpu(Session.t(), keyword()) :: map()
  def allocate_gpu(%Session{} = session, opts \\ []) do
    partition = to_string(Keyword.get(opts, :partition, "a100"))
    gpus = Keyword.get(opts, :gpus, 1)
    walltime = Keyword.get(opts, :walltime, "01:00:00")
    ntasks = Keyword.get(opts, :ntasks)

    # Submit a sleep-hold batch job.  sbatch exits immediately (no CMD window, no Port)
    # and the job keeps the allocation alive on the cluster for `walltime`.
    sleep_secs = walltime_to_seconds(walltime)

    log_dir = Path.join(session.work_dir, "sbatch_logs")

    gres_option = gres_cmd_option(session, opts, partition, gpus)

    sbatch_cmd =
      "mkdir -p #{Shell.escape(log_dir)}" <>
        " && sbatch --parsable --partition=#{partition} #{gres_option}" <>
        if(ntasks, do: " --ntasks=#{ntasks}", else: "") <>
        " --time=#{walltime} --job-name=hpc_connect_hold" <>
        " --output=#{Shell.escape(log_dir)}/hpc_connect_hold_%j.out" <>
        " --error=#{Shell.escape(log_dir)}/hpc_connect_hold_%j.err" <>
        " --wrap=\"sleep #{sleep_secs}\""

    {output, status} =
      session
      |> SSH.ssh_command(sbatch_cmd, "Allocate #{gpus} GPU(s) on #{partition}")
      |> SSH.run(Keyword.get(opts, :connect_opts, []))

    if status != 0, do: raise(RuntimeError, "sbatch failed: #{output}")

    job_id = extract_job_id!(output)

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
  - `:ntasks` – number of tasks (optional)
  - `:cpus_per_task` – CPUs per task (optional)
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
    ntasks = Keyword.get(opts, :ntasks)
    cpus = Keyword.get(opts, :cpus_per_task)
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

    gres_option = gres_option(session, opts, partition, gpus)
    ntasks_line = if ntasks, do: "#SBATCH --ntasks=#{ntasks}", else: ""
    cpus_line = if cpus, do: "#SBATCH --cpus-per-task=#{cpus}", else: ""

    job_script = """
    #!/bin/bash -l
    #SBATCH --job-name=hpc_connect_vllm
    #SBATCH --partition=#{partition}
    #{gres_option}
    #{ntasks_line}
    #{cpus_line}
    #SBATCH --time=#{walltime}
    #SBATCH --output=#{logs_dir}/vllm_%j.out
    #SBATCH --error=#{logs_dir}/vllm_%j.err

    #{module_lines}
    #{conda_line}

    exec bash #{script} --model #{model_dir} --port #{port}
    """

    b64 = job_script |> String.replace("\r", "") |> Base.encode64() |> String.replace("\n", "")

    remote =
      "mkdir -p #{Shell.escape(logs_dir)} && " <>
        "echo #{b64} | base64 -d > #{Shell.escape(job_script_path)} && " <>
        "sbatch --parsable #{Shell.escape(job_script_path)}"

    {output, status} = SSH.exec(session, remote, [])

    if status != 0, do: raise(RuntimeError, "sbatch failed: #{output}")

    %{
      job_id: extract_job_id!(output),
      partition: partition,
      gpus: gpus,
      walltime: walltime,
      port: port,
      logs_dir: logs_dir
    }
  end

  defp extract_job_id!(output) do
    case output
         |> String.split("\n", trim: true)
         |> Enum.map(&strip_ansi/1)
         |> Enum.map(&String.trim/1)
         |> Enum.map(fn line ->
           line
           |> String.split(";", parts: 2)
           |> List.first()
           |> String.trim()
         end)
         |> Enum.find(&String.match?(&1, ~r/^\d+$/)) do
      nil -> raise RuntimeError, "sbatch returned no valid job ID: #{inspect(output)}"
      id -> id
    end
  end

  defp gres_cmd_option(%Session{} = session, opts, partition, gpus) do
    gpu_type =
      Keyword.get(opts, :gpu_type) ||
        infer_gpu_type_from_partition(partition) ||
        session.cluster.gpu_type

    if gpu_type do
      "--gres=gpu:#{gpu_type}:#{gpus}"
    else
      "--gres=gpu:#{gpus}"
    end
  end

  defp gres_option(%Session{} = session, opts, partition, gpus) do
    gpu_type =
      Keyword.get(opts, :gpu_type) ||
        infer_gpu_type_from_partition(partition) ||
        session.cluster.gpu_type

    if gpu_type do
      "#SBATCH --gres=gpu:#{gpu_type}:#{gpus}"
    else
      "#SBATCH --gres=gpu:#{gpus}"
    end
  end

  defp infer_gpu_type_from_partition(partition) do
    type = partition |> to_string() |> String.trim() |> String.downcase()

    cond do
      type == "" ->
        nil

      type in ["standard", "cpu", "login", "batch", "debug", "shared"] ->
        nil

      Regex.match?(~r/^gpu[-_]?([a-z0-9]+)$/i, type) ->
        case Regex.run(~r/^gpu[-_]?([a-z0-9]+)$/i, type, capture: :all_but_first) do
          [suffix] when suffix != "" -> suffix
          _ -> "gpu"
        end

      Regex.match?(~r/^(a|h|v|p|t|l)\d+[a-z0-9-]*$/i, type) ->
        type

      Regex.match?(~r/^rtx\d+[a-z0-9-]*$/i, type) ->
        type

      Regex.match?(~r/^mi\d+[a-z0-9-]*$/i, type) ->
        type

      Regex.match?(~r/\d/, type) ->
        type

      true ->
        nil
    end
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
    {output, status} =
      SSH.exec(
        session,
        "squeue -j #{job_id} -h -o '%N' 2>/dev/null || true",
        timeout: Keyword.get(opts, :timeout, 30_000)
      )

    if status != 0 do
      nil
    else
      output
      |> String.split("\n", trim: true)
      |> Enum.map(&strip_ansi/1)
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&header_or_warning_line?/1)
      |> Enum.map(&normalize_nodelist_value/1)
      |> Enum.reject(&is_nil/1)
      |> List.first()
    end
  end

  defp normalize_nodelist_value(value) do
    down = String.downcase(value)

    cond do
      value in ["", "(null)", "None"] ->
        nil

      String.starts_with?(value, "(") ->
        nil

      String.starts_with?(down, "ssh:") ->
        nil

      String.contains?(down, "connect to host") ->
        nil

      String.contains?(down, "connection refused") ->
        nil

      String.contains?(down, "error") ->
        nil

      String.contains?(value, " ") ->
        nil

      Regex.match?(~r/^[A-Za-z0-9][A-Za-z0-9_,.-]*$/, value) ->
        value

      true ->
        nil
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

  `local_def_path` defaults to the bundled `priv/def_files/*.def` when `nil`.
  Creates the remote `singularity_def_files/` directory if missing.

  Returns the remote path the file was written to.
  """
  @spec upload_def_file(Session.t(), binary(), binary() | nil) :: binary()
  def upload_def_file(%Session{} = session, name \\ "vllm", local_def_path \\ nil) do
    remote_dir = Path.join(session.work_dir, "singularity_def_files")

    ssh_with_retry!(
      session,
      "mkdir -p #{Shell.escape(remote_dir)}",
      "Create def files dir"
    )

    if is_binary(local_def_path) do
      remote_path = remote_def_path(session, name)
      upload_with_retry!(session, local_def_path, remote_path, [])
      remote_path
    else
      local_dir = Path.join([to_string(:code.priv_dir(:hpc_connect)), "def_files"])

      local_dir
      |> File.ls!()
      |> Enum.filter(&String.ends_with?(&1, ".def"))
      |> Enum.each(fn file ->
        local_path = Path.join(local_dir, file)
        def_name = Path.basename(file, ".def")
        remote_path = remote_def_path(session, def_name)

        upload_with_retry!(session, local_path, remote_path, [])
      end)

      remote_def_path(session, name)
    end
  end

  @transient_upload_errors [
    "Connection refused",
    "Connection closed",
    "connect to host",
    "status 255"
  ]

  # Runs an SSH command, retrying on transient errors. Raises on non-zero exit.
  # Uses the persistent Erlang :ssh connection when available (no new OS process).
  defp ssh_with_retry!(session, cmd, summary, retries \\ 3, delay_ms \\ 3_000) do
    case SSH.exec(session, cmd, []) do
      {_, 0} ->
        :ok

      {output, status} ->
        transient? = Enum.any?(@transient_upload_errors, &String.contains?(output, &1))

        if transient? and retries > 0 do
          Process.sleep(delay_ms)
          ssh_with_retry!(session, cmd, summary, retries - 1, delay_ms)
        else
          raise RuntimeError, "#{summary} failed (exit #{status}): #{output}"
        end
    end
  end

  # Like ssh_with_retry! but also returns output on success (for commands with stdout).
  # Uses the persistent Erlang :ssh connection when available (no new OS process).
  defp ssh_run_with_retry!(session, cmd, summary, retries \\ 3, delay_ms \\ 3_000) do
    case SSH.exec(session, cmd, []) do
      {output, 0} ->
        output

      {output, status} ->
        transient? = Enum.any?(@transient_upload_errors, &String.contains?(output, &1))

        if transient? and retries > 0 do
          Process.sleep(delay_ms)
          ssh_run_with_retry!(session, cmd, summary, retries - 1, delay_ms)
        else
          raise RuntimeError, "#{summary} failed (exit #{status}): #{output}"
        end
    end
  end

  defp upload_with_retry!(session, local, remote, opts, retries \\ 3, delay_ms \\ 3_000) do
    SSH.upload!(session, local, remote, opts)
  rescue
    e in RuntimeError ->
      msg = Exception.message(e)
      transient? = Enum.any?(@transient_upload_errors, &String.contains?(msg, &1))

      if transient? and retries > 0 do
        Process.sleep(delay_ms)
        upload_with_retry!(session, local, remote, opts, retries - 1, delay_ms)
      else
        reraise e, __STACKTRACE__
      end
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
    force_rebuild = Keyword.get(opts, :force_rebuild, false)
    use_slurm = Keyword.get(opts, :use_slurm, false)
    # Default to :woody — AlmaLinux login nodes support user namespaces for apptainer build.
    # Ubuntu-based systems (TinyX, Testcluster) do not. Pass `build_cluster: nil` to
    # disable the redirect and build on the session's own cluster.
    build_cluster = Keyword.get(opts, :build_cluster, :woody)

    if not force_rebuild and remote_file_exists?(session, sif_path) do
      Logger.info("[HpcConnect] SIF already exists: #{sif_path}")
      %{job_id: nil, sif_path: sif_path, def_path: def_path, name: name}
    else
      force_env = if force_rebuild, do: "export FORCE_REBUILD=1 && ", else: ""

      build_session =
        if build_cluster do
          clone_session_for_cluster(session, build_cluster)
        else
          session
        end

      if use_slurm do
        build_sif_via_slurm(build_session, name, def_path, sif_path, force_env, opts)
      else
        build_sif_direct(build_session, name, def_path, sif_path, force_env)
      end
    end
  end

  # Clones a session's credentials onto a different cluster.
  # Used to run apptainer builds on a cluster that supports user namespaces
  # (e.g. woody) while the primary session targets another cluster (e.g. tinyx).
  # Because all NHR FAU clusters share the same home/vault filesystem, the resulting
  # SIF will be visible from the original session without any extra copying.
  defp clone_session_for_cluster(%Session{} = session, cluster_name) do
    target_cluster = HpcConnect.Cluster.fetch!(cluster_name)

    Logger.info(
      "[HpcConnect] Routing apptainer build via #{target_cluster.name} " <>
        "(shared FS — SIF will be visible from #{session.cluster.name})"
    )

    %Session{
      session
      | cluster: target_cluster,
        ssh_alias: target_cluster.ssh_alias,
        proxy_jump: target_cluster.proxy_jump
    }
  end

  # Runs `apptainer build` directly on the login node via a background nohup process.
  # Returns immediately with a log_file path. No SLURM job — no internet issues.
  # build_sif_blocking polls remote_file_exists? to detect completion.
  defp build_sif_direct(session, name, def_path, sif_path, force_env) do
    script_dir = Scripts.remote_script_dir(session)
    logs_dir = Path.join(session.work_dir, "sbatch_logs")
    log_file = Path.join(logs_dir, "build_sif_#{name}_direct.log")

    proxy_exports = proxy_env_exports(session)

    remote =
      "mkdir -p #{Shell.escape(logs_dir)} && " <>
        "chmod 755 #{Shell.escape(logs_dir)} && " <>
        proxy_exports <>
        force_env <>
        "export HPC_WORK_DIR=#{Shell.escape(session.work_dir)} && " <>
        "export DEF_NAME=#{Shell.escape(name)} && " <>
        "nohup bash #{Shell.escape(Path.join(script_dir, "build_sif.sh"))} " <>
        "</dev/null > #{Shell.escape(log_file)} 2>&1 & disown && echo launched"

    ssh_run_with_retry!(session, remote, "Start direct apptainer build for #{name}.sif")

    Logger.info("[HpcConnect] Apptainer build started in background on login node.")
    Logger.info("[HpcConnect] Log: #{log_file}")
    Logger.info("[HpcConnect] Tail with: ssh #{session.cluster.ssh_alias} 'tail -f #{log_file}'")

    Logger.info(
      "[HpcConnect] Check process: ssh #{session.cluster.ssh_alias} 'ps aux | grep apptainer'"
    )

    %{job_id: nil, sif_path: sif_path, def_path: def_path, name: name, log_file: log_file}
  end

  # Returns proxy export statements if the cluster has an http_proxy configured.
  defp proxy_env_exports(%{cluster: %{http_proxy: nil}}), do: ""

  defp proxy_env_exports(%{cluster: %{http_proxy: proxy}}) do
    "export http_proxy=#{proxy} && export https_proxy=#{proxy} && "
  end

  # Submits `build_sif.sh` as an sbatch job and returns the job ID immediately.
  # Requires internet access from the compute node (not available on all clusters).
  # Use `use_slurm: true` to opt in.
  defp build_sif_via_slurm(session, name, def_path, sif_path, force_env, opts) do
    partition = Keyword.get(opts, :partition)
    walltime = Keyword.get(opts, :walltime, "02:00:00")
    cpus = Keyword.get(opts, :cpus, 4)

    logs_dir = Path.join(session.work_dir, "sbatch_logs")
    script_dir = Scripts.remote_script_dir(session)
    job_script_path = Path.join(script_dir, "build_sif_submit.sh")

    partition_line = if partition, do: "#SBATCH --partition=#{partition}", else: ""
    gres = Keyword.get(opts, :gres, session.cluster.require_gres)
    gres_line = if gres, do: "#SBATCH --gres=#{gres}", else: ""

    job_script = """
    #!/bin/bash -l
    #SBATCH --job-name=hpc_connect_build_sif
    #{partition_line}
    #{gres_line}
    #SBATCH --ntasks=1
    #SBATCH --cpus-per-task=#{cpus}
    #SBATCH --time=#{walltime}
    #SBATCH --output=#{logs_dir}/build_sif_#{name}_%j.out
    #SBATCH --error=#{logs_dir}/build_sif_#{name}_%j.err

    #{proxy_script_exports(session)}
    #{force_env}export HPC_WORK_DIR=#{session.work_dir}
    export DEF_NAME=#{name}

    bash #{Path.join(Scripts.remote_script_dir(session), "build_sif.sh")}
    """

    b64 = job_script |> String.replace("\r", "") |> Base.encode64() |> String.replace("\n", "")

    remote =
      "mkdir -p #{Shell.escape(logs_dir)} #{Shell.escape(script_dir)} && " <>
        "echo #{b64} | base64 -d > #{Shell.escape(job_script_path)} && " <>
        "sbatch --parsable #{Shell.escape(job_script_path)}"

    output = ssh_run_with_retry!(session, remote, "Submit Apptainer build job for #{name}.sif")

    %{
      job_id: extract_job_id!(output),
      sif_path: sif_path,
      def_path: def_path,
      name: name,
      logs_dir: logs_dir
    }
  end

  # Returns proxy export shell lines for use inside sbatch job scripts.
  defp proxy_script_exports(%{cluster: %{http_proxy: nil}}), do: ""

  defp proxy_script_exports(%{cluster: %{http_proxy: proxy}}) do
    "export http_proxy=#{proxy}\nexport https_proxy=#{proxy}"
  end

  defp remote_def_exists?(session, remote_path) do
    output =
      ssh_run_with_retry!(
        session,
        "test -f #{Shell.escape(remote_path)} && echo ok || true",
        "Check remote def exists"
      )

    String.trim(output) == "ok"
  end

  defp remote_file_exists?(session, remote_path) do
    output =
      ssh_run_with_retry!(
        session,
        "test -f #{Shell.escape(remote_path)} && echo ok || true",
        "Check remote file exists"
      )

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
    timeout = Keyword.get(opts, :timeout, 3_600_000)
    interval = Keyword.get(opts, :interval, 15_000)

    result = build_sif_job(session, opts)
    %{job_id: job_id, sif_path: sif_path} = result

    cond do
      # SIF already existed before we started
      is_nil(job_id) and remote_file_exists?(session, sif_path) ->
        Logger.info("[HpcConnect] SIF already exists: #{sif_path}")
        sif_path

      # Direct background build started — poll by file existence
      is_nil(job_id) ->
        log_hint = Map.get(result, :log_file)

        Logger.info("[HpcConnect] Waiting for direct build to complete. SIF: #{sif_path}")

        if log_hint do
          Logger.info(
            "[HpcConnect] Tail the build log: ssh #{session.cluster.ssh_alias} 'tail -f #{log_hint}'"
          )
        end

        wait_for_sif_direct!(session, sif_path, timeout, interval)

      # SLURM job submitted — poll via sacct + file existence
      true ->
        Logger.info("[HpcConnect] Build job #{job_id} submitted. Waiting for SIF at #{sif_path}")

        Logger.info(
          "[HpcConnect] Monitor: squeue -j #{job_id} | Logs: tail -f #{Map.get(result, :logs_dir, "")}/build_sif_#{Map.get(result, :name, "")}_#{job_id}.out"
        )

        wait_for_sif!(session, job_id, sif_path, timeout, interval)
    end
  end

  defp wait_for_sif_direct!(_session, _sif_path, timeout, _interval) when timeout <= 0 do
    raise RuntimeError, "Timed out waiting for direct apptainer build to complete"
  end

  defp wait_for_sif_direct!(session, sif_path, timeout, interval) do
    Logger.debug("[HpcConnect] Waiting for direct build (#{div(timeout, 1000)}s remaining)...")
    Process.sleep(interval)

    if remote_file_exists?(session, sif_path) do
      Logger.info("[HpcConnect] SIF ready: #{sif_path}")
      sif_path
    else
      # Tail recent build log lines so errors surface in IEx
      log_dir = Path.join(session.work_dir, "sbatch_logs")
      log_glob = Path.join(log_dir, "build_sif_*_direct.log")

      {tail_out, _} =
        session
        |> SSH.ssh_command(
          "ls -t #{Shell.escape(log_glob)} 2>/dev/null | head -1 | xargs -I{} tail -n 10 {}",
          "Tail build log"
        )
        |> SSH.run([])

      trimmed = String.trim(tail_out)
      if trimmed != "", do: Logger.info("[HpcConnect] Build log tail:\n#{trimmed}")

      wait_for_sif_direct!(session, sif_path, timeout - interval, interval)
    end
  end

  defp wait_for_sif!(_session, _job_id, _sif_path, timeout, _interval) when timeout <= 0 do
    raise RuntimeError, "Timed out waiting for Apptainer build to complete"
  end

  defp wait_for_sif!(session, job_id, sif_path, timeout, interval) do
    Logger.debug(
      "[HpcConnect] Waiting for build job #{job_id} (#{div(timeout, 1000)}s remaining)..."
    )

    Process.sleep(interval)

    state = job_state(session, job_id)
    Logger.debug("[HpcConnect] Job #{job_id} state: #{inspect(state)}")

    cond do
      remote_file_exists?(session, sif_path) ->
        Logger.info("[HpcConnect] SIF ready: #{sif_path}")
        sif_path

      state in ~w(FAILED CANCELLED TIMEOUT NODE_FAIL OUT_OF_MEMORY) ->
        raise RuntimeError,
              "Apptainer build job #{job_id} failed (state: #{state}). " <>
                "Check logs with: sacct -j #{job_id} --format=JobID,State,ExitCode,Reason"

      state == "COMPLETED" ->
        raise RuntimeError,
              "Apptainer build job #{job_id} COMPLETED but SIF not found at #{sif_path}. " <>
                "Check job output: sacct -j #{job_id} --format=JobID,State,ExitCode,Reason"

      true ->
        wait_for_sif!(session, job_id, sif_path, timeout - interval, interval)
    end
  end

  defp job_state(session, job_id) do
    {output, _} =
      SSH.exec(
        session,
        # Strip ANSI escape codes, skip quota warning lines, take the first real state line
        "sacct -j #{job_id} --noheader --format=State --parsable2 2>/dev/null " <>
          "| sed 's/\\x1b\\[[0-9;]*m//g' | grep -v '^$' | grep -v 'WARNING' | grep -v 'Path' | grep -v '!!!' " <>
          "| head -1 || true",
        []
      )

    String.trim(output)
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
    cpus = Keyword.get(opts, :cpus)
    sif_name = Keyword.get(opts, :sif_name, app)
    sif_path = Keyword.get(opts, :sif_path) || remote_sif_path(session, sif_name)
    app_env = Keyword.get(opts, :app_env, %{})
    args = Keyword.get(opts, :args, [])

    logs_dir = Path.join(session.work_dir, "sbatch_logs")
    run_script = Path.join(Scripts.remote_script_dir(session), "start_#{app}.sh")

    app_args = args_to_cli_string(args)

    export_vars =
      [
        "HPC_MODELS_DIR=#{Shell.escape(Model.models_root(session))}",
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
        " #{gres_cmd_option(session, opts, partition, gpus)}" <>
        if(cpus, do: " --cpus-per-task=#{cpus}", else: "") <>
        " --time=#{walltime}" <>
        " --output=#{Shell.escape(logs_dir)}/#{app}_%j.out" <>
        " --error=#{Shell.escape(logs_dir)}/#{app}_%j.err" <>
        " --export=#{export_vars}" <>
        " #{Shell.escape(run_script)}"

    {output, status} =
      SSH.exec(session, sbatch_cmd, timeout: Keyword.get(opts, :timeout, 120_000))

    if status != 0, do: raise(RuntimeError, "sbatch failed: #{output}")

    job_id = extract_job_id!(output)

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
