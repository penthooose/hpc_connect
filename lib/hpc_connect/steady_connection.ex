defmodule HpcConnect.SteadyConnection do
  @moduledoc """
  Steady OS SSH connection: a persistent `ssh <target> "bash -s"` shell that
  all OS SSH commands multiplex over, with auto-reconnect and exponential
  backoff on transient failures.

  When `steady_connection` is enabled on a session, every `ssh` command built by
  `HpcConnect.SSH` is routed through a long-lived shell owned by a
  `HpcConnect.SteadyConnection.Server` process instead of spawning a fresh `ssh`
  process per command. This eliminates the per-command TCP handshake + key
  exchange (~1–2 s saved per command) and works on every platform — including
  Win32-OpenSSH, which does **not** support OpenSSH `ControlMaster` multiplexing.

  Reliability:
    * keepalive options (`ServerAliveInterval`/`ServerAliveCountMax`) keep the
      connection from being dropped by idle timeouts;
    * if the shell dies (network drop, jump-host hiccup) the next command
      transparently reopens it;
    * transient failures are retried with exponential backoff
      (`HpcConnect.Backoff`).

  Activation: `steady_connection: true` on the session (usually
  `HPC_CONNECT_STEADY_CONNECTION=true` in `.env`). Tuning:

    * `HPC_CONNECT_STEADY_TIMEOUT_SECONDS` – ssh connect timeout (default `30`)
    * `:timeout` per command (default `120_000` ms)

  The registry (ETS) maps a stable per-session key to its `Server` process and
  is owned by a long-lived keeper GenServer started on demand, so steady shells
  survive short-lived callers (Livebook cells, scripts).
  """

  require Logger

  alias HpcConnect.{Backoff, Session, SSH}
  alias HpcConnect.SteadyConnection.Server

  @registry_name __MODULE__.Registry
  @table :hpc_connect_steady

  # ---------------------------------------------------------------------------
  # Introspection
  # ---------------------------------------------------------------------------

  @doc """
  Returns a stable identity key for a session (used for registry lookup).
  """
  @spec session_key(Session.t()) :: binary()
  def session_key(%Session{} = session) do
    identity = "#{Session.target(session)}|#{session.identity_file || session.username || ""}"

    "steady|" <>
      (:crypto.hash(:md5, identity) |> Base.encode16(case: :lower))
  end

  @spec enabled?(Session.t()) :: boolean()
  def enabled?(%Session{} = session), do: session.steady_connection == true

  # ---------------------------------------------------------------------------
  # Registry
  # ---------------------------------------------------------------------------

  @doc """
  Ensures the registry keeper (ETS owner) is running.
  """
  @spec ensure_registry() :: :ok
  def ensure_registry do
    case Process.whereis(@registry_name) do
      nil ->
        case GenServer.start(__MODULE__, :ok, name: @registry_name) do
          {:ok, _pid} ->
            :ok

          {:error, {:already_started, _pid}} ->
            :ok

          {:error, reason} ->
            raise RuntimeError, "steady registry failed to start: #{inspect(reason)}"
        end

      _pid ->
        :ok
    end
  end

  @doc """
  Returns the steady `Server` pid for the session, or `nil` (also when a stale
  registry entry points at a dead process).
  """
  @spec lookup_server(Session.t() | binary()) :: pid() | nil
  def lookup_server(%Session{} = session), do: lookup_server(session_key(session))

  def lookup_server(key) when is_binary(key) do
    case ets_lookup(key) do
      pid when is_pid(pid) -> if Process.alive?(pid), do: pid, else: nil
      nil -> nil
    end
  end

  @doc """
  Ensures a steady `Server` is running for the session and returns it.
  Starts the persistent shell if needed.
  """
  @spec ensure_server(Session.t()) :: pid()
  def ensure_server(%Session{} = session) do
    ensure_registry()
    key = session_key(session)

    case ets_lookup(key) do
      pid when is_pid(pid) ->
        if Process.alive?(pid), do: pid, else: start_server(key, session)

      nil ->
        start_server(key, session)
    end
  end

  defp start_server(key, %Session{} = session) do
    case Server.start(session) do
      {:ok, pid} ->
        ets_insert(key, pid)
        pid

      {:error, reason} ->
        raise RuntimeError, "steady shell start failed: #{inspect(reason)}"
    end
  end

  @doc """
  Removes the registry entry for a session (does not stop a live server).
  """
  @spec drop_server(Session.t() | binary()) :: :ok
  def drop_server(%Session{} = session), do: drop_server(session_key(session))
  def drop_server(key) when is_binary(key), do: ets_delete(key)

  # ---------------------------------------------------------------------------
  # Connection lifecycle
  # ---------------------------------------------------------------------------

  @doc """
  Ensures a steady shell is established for the session and verifies it with a
  lightweight probe. Returns the session. A no-op when steady is disabled.
  """
  @spec ensure_connected!(Session.t(), keyword()) :: Session.t()
  def ensure_connected!(%Session{} = session, opts \\ []) do
    if enabled?(session) do
      _server = ensure_server(session)
      retries = Keyword.get(opts, :retries, 3)
      timeout = Keyword.get(opts, :timeout, 30_000)

      run_remote(session, "echo steady-ok", retries: retries, timeout: timeout)
      session
    else
      session
    end
  end

  @doc """
  Returns `true` when a live steady shell is registered for the session.
  """
  @spec connected?(Session.t()) :: boolean()
  def connected?(%Session{} = session) do
    case lookup_server(session) do
      pid when is_pid(pid) -> Process.alive?(pid)
      nil -> false
    end
  end

  @doc """
  Stops the steady shell for the session and removes its registry entry.
  Returns `:ok`. A no-op when steady is disabled or not connected.
  """
  @spec disconnect(Session.t()) :: :ok
  def disconnect(%Session{} = session) do
    key = session_key(session)

    case ets_lookup(key) do
      pid when is_pid(pid) ->
        _ = Server.close(pid)
        ets_delete(key)
        :ok

      nil ->
        :ok
    end
  end

  # ---------------------------------------------------------------------------
  # Execution
  # ---------------------------------------------------------------------------

  @doc """
  Runs a remote command over the steady shell with auto-reconnect and
  exponential backoff. Returns `{output, status}` (status may be `:timeout` or
  `:closed`).

  Falls back to a plain OS `ssh` run when no steady server is registered
  (session not pre-warmed) — graceful degradation, no crash.
  """
  @spec run_remote(Session.t() | binary(), binary(), keyword()) :: {binary(), term()}
  def run_remote(session_or_key, remote_command, opts \\ [])

  def run_remote(%Session{} = session, remote_command, opts) do
    key = session_key(session)

    case lookup_server(key) do
      nil ->
        Logger.debug("[HpcConnect] steady shell not connected; using one-shot OS ssh")
        cmd = SSH.ssh_command(session, remote_command, remote_command)
        SSH.run(cmd, opts)

      _pid ->
        retries = Keyword.get(opts, :retries, 3)
        forever? = Keyword.get(opts, :retry_forever, session.retry_forever || false)
        timeout_ms = Keyword.get(opts, :timeout, 120_000)
        do_run_remote(key, remote_command, retries, forever?, 0, timeout_ms)
    end
  end

  def run_remote(key, remote_command, opts)
      when is_binary(key) and is_binary(remote_command) do
    retries = Keyword.get(opts, :retries, 3)
    forever? = Keyword.get(opts, :retry_forever, false)
    timeout_ms = Keyword.get(opts, :timeout, 120_000)
    do_run_remote(key, remote_command, retries, forever?, 0, timeout_ms)
  end

  defp do_run_remote(key, remote_command, retries_left, forever?, attempt, timeout_ms) do
    server = ets_lookup(key) || raise RuntimeError, "steady shell not found for #{key}"

    {output, status} = Server.run_command(server, remote_command, timeout_ms)

    cond do
      status == 0 ->
        {output, 0}

      transient_steady_failure?(output, status) and forever? ->
        Logger.warning(
          "[HpcConnect] steady shell transient failure (attempt #{attempt + 1}); " <>
            "reconnecting and retrying until it works"
        )

        _ = Server.reconnect(server)
        Process.sleep(steady_backoff(attempt, forever?))
        do_run_remote(key, remote_command, retries_left, forever?, attempt + 1, timeout_ms)

      retries_left > 0 and transient_steady_failure?(output, status) ->
        # Shell died mid-command – force reconnect and retry with backoff.
        _ = Server.reconnect(server)
        Process.sleep(steady_backoff(attempt, forever?))
        do_run_remote(key, remote_command, retries_left - 1, forever?, attempt + 1, timeout_ms)

      true ->
        {output, status}
    end
  rescue
    e ->
      if call_timeout?(e) and (forever? or retries_left > 0) do
        if forever? do
          Logger.warning(
            "[HpcConnect] steady shell busy (attempt #{attempt + 1}); retrying until it works"
          )
        end

        # The server was busy (GenServer.call timeout) – it stays registered, so
        # retrying with backoff is safe.
        Process.sleep(steady_backoff(attempt, forever?))

        do_run_remote(
          key,
          remote_command,
          if(forever?, do: retries_left, else: retries_left - 1),
          forever?,
          attempt + 1,
          timeout_ms
        )
      else
        reraise e, __STACKTRACE__
      end
  end

  defp transient_steady_failure?(output, status) do
    (status == 255 and is_binary(output) and SSH.transient_failure?(output)) or
      status in [:closed, :timeout]
  end

  # Retry-forever uses the long linear 10 s → 60 s gateway-wait schedule so a
  # throttled jump gateway can clear between attempts; finite retries keep the
  # fast exponential backoff.
  defp steady_backoff(attempt, forever?) do
    opts = if forever?, do: Backoff.forever_options([]), else: []
    Backoff.delay(attempt, opts)
  end

  defp call_timeout?(e) do
    match?(%{__exception__: true}, e) and Exception.message(e) =~ "timeout"
  end

  # ---------------------------------------------------------------------------
  # Keeper GenServer (owns the ETS table)
  # ---------------------------------------------------------------------------

  def init(:ok) do
    _table =
      if :ets.whereis(@table) == :undefined do
        :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
      end

    {:ok, %{}}
  end

  def terminate(_reason, _state) do
    :ets.delete(@table)
    :ok
  end

  defp ets_lookup(key) do
    case :ets.whereis(@table) do
      :undefined ->
        nil

      _table_id ->
        case :ets.lookup(@table, key) do
          [{^key, pid}] -> pid
          [] -> nil
        end
    end
  end

  defp ets_insert(key, pid) do
    :ets.insert(@table, {key, pid})
  end

  defp ets_delete(key) do
    case :ets.whereis(@table) do
      :undefined -> :ok
      _table_id -> :ets.delete(@table, key)
    end
  end
end
