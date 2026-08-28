defmodule HpcConnect.SteadyConnection.Server do
  @moduledoc """
  A per-session persistent OS SSH shell.

  Holds one long-lived `ssh <opts> <target> "bash -s"` process as an Erlang
  Port. Remote commands are sent over the Port's stdin and framed with a unique
  marker that carries the command's exit status, so output of successive commands
  never bleeds into each other:

      <command>
      printf '__HPC_STEADY_<suffix>_%s__\n' "$?"

  The connection survives across calls (no per-command TCP handshake / key
  exchange) and works on any platform, including Win32-OpenSSH, which does not
  support OpenSSH `ControlMaster` multiplexing.

  Commands are serialized (one at a time) by the GenServer. If the ssh process
  dies (network drop, jump-host hiccup) the next command transparently reopens
  it via `ensure_port/1`.
  """

  use GenServer

  require Logger
  alias HpcConnect.{Session, SSH}

  @default_command_timeout_ms 120_000
  @max_output_chars 1_000_000

  # Public API

  @spec start_link(Session.t()) :: GenServer.on_start()
  def start_link(%Session{} = session) do
    GenServer.start_link(__MODULE__, session)
  end

  @doc """
  Starts the persistent shell **unlinked** from the caller, so it survives
  short-lived callers (Livebook cells, scripts) and its crash does not take the
  caller down. Use this for the steady-connection registry.
  """
  @spec start(Session.t()) :: GenServer.on_start()
  def start(%Session{} = session) do
    GenServer.start(__MODULE__, session)
  end

  @doc """
  Runs `remote_command` on the persistent shell.

  Returns `{output, status}` where `status` is the remote exit code, `:timeout`
  when the command exceeded `timeout_ms`, or `:closed` when the shell died.
  """
  @spec run_command(pid(), binary(), non_neg_integer()) :: {binary(), term()}
  def run_command(server, remote_command, timeout_ms \\ @default_command_timeout_ms)
      when is_pid(server) and is_binary(remote_command) do
    GenServer.call(server, {:run_command, remote_command, timeout_ms}, timeout_ms + 15_000)
  end

  @doc """
  Force-closes the underlying ssh process so the next command reconnects.
  """
  @spec reconnect(pid()) :: :ok
  def reconnect(server) when is_pid(server) do
    GenServer.call(server, :reconnect)
  end

  @doc """
  Closes the persistent shell and its ssh process for good.
  """
  @spec close(pid()) :: :ok
  def close(server) when is_pid(server) do
    if Process.alive?(server) do
      GenServer.stop(server, :normal)
    end

    :ok
  end

  @doc """
  Test hook: kills the underlying ssh process so the next command reconnects.
  The `Server` process itself stays alive.
  """
  @spec debug_kill_port(pid()) :: :ok
  def debug_kill_port(server) when is_pid(server) do
    GenServer.call(server, :debug_kill_port)
  end

  # GenServer callbacks

  @impl true
  def init(%Session{} = session) do
    Process.flag(:trap_exit, true)
    state = %{session: session, port: nil}
    {:ok, ensure_port(state)}
  end

  @impl true
  def handle_call({:run_command, remote_command, timeout_ms}, _from, state) do
    state = ensure_port(state)

    case state.port do
      nil ->
        {:reply, {"", :closed}, state}

      port ->
        suffix = random_suffix()
        payload = "#{remote_command}\nprintf '__HPC_STEADY_#{suffix}_%s__\\n' \"$?\"\n"

        _ = Port.command(port, payload)

        {output, status} = collect_until_marker(port, suffix, timeout_ms)
        {:reply, {output, status}, state}
    end
  end

  @impl true
  def handle_call(:reconnect, _from, state) do
    state = force_reconnect(state)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:debug_kill_port, _from, state) do
    safe_close_port(state.port)
    {:reply, :ok, %{state | port: nil}}
  end

  @impl true
  def handle_info({port, {:data, data}}, %{port: port} = state) do
    # Data arriving outside a run_command call (leftover keepalive output or an
    # abandoned slow command) is dropped.
    _ = data
    {:noreply, state}
  end

  def handle_info({port, {:exit_status, _code}}, %{port: port} = state) do
    # The ssh process exited; the port object is dead but Port.info still sees
    # the process briefly. Mark it for reconnect on the next command.
    {:noreply, %{state | port: port_after_exit(port)}}
  end

  def handle_info({:EXIT, port, _reason}, %{port: port} = state) do
    {:noreply, %{state | port: nil}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    safe_close_port(state.port)
    :ok
  end

  # Port lifecycle

  defp ensure_port(%{port: port} = state) do
    if live_port?(port) do
      state
    else
      safe_close_port(port)
      %{state | port: open_shell_port(state.session)}
    end
  end

  defp force_reconnect(%{port: port} = state) do
    safe_close_port(port)
    %{state | port: open_shell_port(state.session)}
  end

  defp live_port?(nil), do: false

  defp live_port?(port) do
    is_port(port) and Port.info(port) != nil
  end

  defp open_shell_port(%Session{} = session) do
    args = shell_args(session)

    Port.open(
      {:spawn_executable, SSH.ssh_binary()},
      [:binary, :exit_status, :stderr_to_stdout] ++ hide_on_windows() ++ [args: args]
    )
  rescue
    e ->
      Logger.error("[HpcConnect] steady shell spawn failed: #{Exception.message(e)}")
      nil
  end

  # On Windows, ssh.exe would otherwise flash a console window. System.cmd adds
  # :hide automatically, but raw Port.open needs it explicitly.
  defp hide_on_windows do
    case :os.type() do
      {:win32, _} -> [:hide]
      _ -> []
    end
  end

  defp shell_args(%Session{} = session) do
    proxy_jump =
      if include_explicit_proxy_jump?(session), do: proxy_jump_target(session), else: nil

    base =
      []
      |> maybe_append_option("-F", session.ssh_config_file)
      |> maybe_append_option("-J", proxy_jump)
      |> maybe_append_option("-i", session.identity_file)

    base ++
      [
        "-o",
        "BatchMode=yes",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "PasswordAuthentication=no",
        "-o",
        "PreferredAuthentications=publickey",
        "-o",
        "NumberOfPasswordPrompts=0",
        "-o",
        "ConnectTimeout=#{connect_timeout(session)}",
        "-o",
        "ServerAliveInterval=30",
        "-o",
        "ServerAliveCountMax=3",
        "-o",
        "ExitOnForwardFailure=no",
        Session.target(session),
        "bash -s"
      ]
  end

  defp connect_timeout(%Session{} = session) do
    value = Session.fetch_env(session, "HPC_CONNECT_STEADY_TIMEOUT_SECONDS")

    case value do
      nil ->
        30

      other ->
        case Integer.parse(String.trim(other)) do
          {n, _} when n > 0 -> n
          _ -> 30
        end
    end
  rescue
    _ -> 30
  end

  defp maybe_append_option(args, _flag, nil), do: args
  defp maybe_append_option(args, _flag, ""), do: args
  defp maybe_append_option(args, flag, value), do: args ++ [flag, value]

  defp include_explicit_proxy_jump?(%Session{ssh_config_file: config, credential_dir: dir})
       when is_binary(config) and config != "" and is_binary(dir) and dir != "" do
    false
  end

  defp include_explicit_proxy_jump?(_session), do: true

  defp proxy_jump_target(%Session{} = session) do
    cond do
      is_nil(session.proxy_jump) or session.proxy_jump == "" ->
        nil

      String.contains?(session.proxy_jump, "@") ->
        session.proxy_jump

      session.username ->
        "#{session.username}@#{session.proxy_jump}"

      true ->
        session.proxy_jump
    end
  end

  # Output framing

  defp collect_until_marker(port, suffix, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    marker_re = ~r/__HPC_STEADY_#{suffix}_(\d+)__/
    do_collect(port, marker_re, "", nil, deadline)
  end

  defp do_collect(port, marker_re, acc, _status, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      {acc, :timeout}
    else
      receive do
        {^port, {:data, data}} ->
          text = IO.iodata_to_binary(data)
          merged = acc <> text

          case extract_marker(merged, marker_re) do
            {:ok, code, output} ->
              {output, code}

            :not_found ->
              do_collect(port, marker_re, tail_buffer(merged), nil, deadline)
          end

        {^port, {:exit_status, _code}} ->
          do_collect(port, marker_re, acc, nil, deadline)

        {:EXIT, ^port, _reason} ->
          {acc, :closed}

        _other ->
          do_collect(port, marker_re, acc, nil, deadline)
      after
        remaining -> {acc, :timeout}
      end
    end
  end

  # The marker is the last thing printed for a command. Everything before it is
  # the command's output; the capture group is the remote exit status.
  defp extract_marker(buffer, marker_re) do
    case Regex.run(marker_re, buffer, capture: :all, return: :index) do
      [{match_start, _match_len}, {code_start, code_len}] ->
        code = binary_part(buffer, code_start, code_len)

        case Integer.parse(code) do
          {n, _} ->
            output =
              buffer
              |> binary_part(0, match_start)
              |> String.trim_trailing("\n")

            {:ok, n, output}

          :error ->
            :not_found
        end

      _ ->
        :not_found
    end
  end

  # Keep only the tail of a large buffer so we don't hold unbounded memory and
  # marker matching stays fast, but enough to always match a marker that spans
  # chunk boundaries.
  defp tail_buffer(buffer) do
    if byte_size(buffer) > @max_output_chars do
      binary_part(buffer, byte_size(buffer) - @max_output_chars, @max_output_chars)
    else
      buffer
    end
  end

  defp port_after_exit(port) do
    if Port.info(port) == nil, do: nil, else: port
  end

  defp safe_close_port(nil), do: :ok

  defp safe_close_port(port) do
    if is_port(port) and Port.info(port) != nil do
      _ = Port.close(port)
    end

    :ok
  end

  defp random_suffix do
    :crypto.strong_rand_bytes(4)
    |> Base.encode16(case: :lower)
  end
end
