defmodule HpcConnect.Batch do
  @moduledoc """
  Runs several independent remote shell commands in a **single SSH round-trip**
  and returns each command's output and exit status separately.

  This reduces the number of SSH connections/handshakes for complete worksteps
  (e.g. the bootstrap startup summary) from N down to 1. When the steady
  connection is enabled the multiplexing overhead is already low, but combining
  still avoids N sequential round-trips.

  The remote script wraps every command in a bash `{ ...; }` group followed by
  marker lines that carry the command's exit code:

      { <cmd>; printf '\\n__HPC_BATCH_EXIT_1_<suffix>__%s\\n' "$?"; printf '__HPC_BATCH_END_1_<suffix>__\\n'; }

  The combined output is split on those markers in `parse_output/4`. Markers are
  randomised per call so they cannot collide with real command output.
  """

  alias HpcConnect.{Backoff, Session, Shell, SSH}

  @type result :: %{label: binary(), output: binary(), status: non_neg_integer()}

  @doc """
  Runs `commands` (`[{label, remote_command}]`) in a single remote invocation.

  Returns a list of `%{label, output, status}` maps, one per command, in the
  given order.

  Options:
    * `:retries` – transient SSH retries (default `3`)
    * `:connect_opts` – forwarded to `SSH.exec/3` on native sessions
    * `:run_fun` – test hook: `(script -> {output, status})` overriding execution
    * `:suffix` – test hook: fixed marker suffix (default random)
  """
  @spec run(Session.t(), [{binary(), binary()}], keyword()) :: [result()]
  def run(%Session{} = session, commands, opts \\ []) when is_list(commands) do
    suffix = Keyword.get(opts, :suffix) || random_suffix()
    script = build_script(commands, suffix)

    {output, status} =
      case Keyword.get(opts, :run_fun) do
        nil -> run_remote(session, script, opts)
        fun when is_function(fun, 1) -> fun.(script)
      end

    parse_output(output, commands, suffix, status)
  end

  # ---------------------------------------------------------------------------
  # Script building
  # ---------------------------------------------------------------------------

  defp build_script(commands, suffix) do
    commands
    |> Enum.with_index(1)
    |> Enum.map_join("\n", fn {{_label, cmd}, i} ->
      "{ " <>
        cmd <>
        "; printf '\\n__HPC_BATCH_EXIT_#{i}_#{suffix}__%s\\n' \"$?\";" <>
        " printf '__HPC_BATCH_END_#{i}_#{suffix}__\\n'; }"
    end)
  end

  # ---------------------------------------------------------------------------
  # Execution
  # ---------------------------------------------------------------------------

  defp run_remote(%Session{ssh_conn: conn} = session, script, opts) when not is_nil(conn) do
    # Native :ssh session – already persistent, single exec via bash for the
    # `{ ...; }` group semantics to match the OS path.
    SSH.exec(session, "bash -lc #{Shell.escape(script)}", Keyword.get(opts, :connect_opts, []))
  end

  defp run_remote(%Session{} = session, script, opts) do
    retries = Keyword.get(opts, :retries, 3)
    cmd = SSH.ssh_command(session, script, "batch remote commands")
    do_run_remote(cmd, retries, 0)
  end

  defp do_run_remote(cmd, retries_left, attempt) do
    {output, status} = SSH.run(cmd, retries: 0)

    if retries_left > 0 and transient_batch_status?(output, status) do
      Process.sleep(Backoff.delay(attempt))
      do_run_remote(cmd, retries_left - 1, attempt + 1)
    else
      {output, status}
    end
  end

  defp transient_batch_status?(output, status) do
    (status == 255 and is_binary(output) and SSH.transient_failure?(output)) or
      status in [:closed, :timeout]
  end

  # ---------------------------------------------------------------------------
  # Output parsing
  # ---------------------------------------------------------------------------

  defp parse_output(output, commands, suffix, overall_status) do
    commands
    |> Enum.with_index(1)
    |> Enum.map(fn {{label, _cmd}, i} ->
      {cmd_output, cmd_status} = extract_segment(output, suffix, i)

      %{
        label: label,
        output: cmd_output,
        status: cmd_status || overall_status || -1
      }
    end)
  end

  defp extract_segment(output, suffix, i) do
    end_marker = "__HPC_BATCH_END_#{i}_#{suffix}__"
    prev_end = if i == 1, do: nil, else: "__HPC_BATCH_END_#{i - 1}_#{suffix}__"

    segment =
      output
      |> cut_after(prev_end)
      |> cut_before(end_marker)

    exit_marker = "__HPC_BATCH_EXIT_#{i}_#{suffix}__"
    split_exit_marker(segment, exit_marker)
  end

  defp cut_after(text, nil), do: text

  defp cut_after(text, marker) do
    case String.split(text, marker, parts: 2) do
      [_, rest] -> rest
      [_] -> ""
    end
  end

  defp cut_before(text, marker) do
    case String.split(text, marker, parts: 2) do
      [content, _rest] -> content
      [content] -> content
    end
  end

  # Splits a segment into its command output (everything before the exit-marker
  # line) and the captured exit status.
  defp split_exit_marker(segment, exit_marker) do
    lines = String.split(segment, "\n")

    {output_lines, status} =
      lines
      |> Enum.with_index()
      |> Enum.reduce({[], nil}, fn {line, idx}, {_acc, _status} = state ->
        case parse_exit_line(line, exit_marker) do
          {:ok, code} -> {Enum.take(lines, idx), code}
          :error -> state
        end
      end)

    {output_lines |> Enum.join("\n") |> String.trim("\n"), status}
  end

  defp parse_exit_line(line, exit_marker) do
    if String.starts_with?(line, exit_marker) do
      code = line |> String.replace_prefix(exit_marker, "") |> String.trim()

      case Integer.parse(code) do
        {n, _} -> {:ok, n}
        :error -> {:ok, -1}
      end
    else
      :error
    end
  end

  defp random_suffix do
    :crypto.strong_rand_bytes(4)
    |> Base.encode16(case: :lower)
  end
end
