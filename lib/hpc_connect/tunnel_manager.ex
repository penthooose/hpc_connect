defmodule HpcConnect.TunnelManager do
  @moduledoc """
  A global GenServer to keep OS ports alive and unlinked from the calling process.
  This is extremely important for Livebook or scripts where the evaluating process
  (and any linked Ports) dies immediately after the cell completes.
  """
  use GenServer
  require Logger

  @name __MODULE__
  @max_output_chars 16_384

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: @name)
  end

  def ensure_started do
    case Process.whereis(@name) do
      nil ->
        case GenServer.start(__MODULE__, %{}, name: @name) do
          {:ok, _pid} -> :ok
          {:error, {:already_started, _pid}} -> :ok
          {:error, reason} -> {:error, reason}
        end

      _pid ->
        :ok
    end
  end

  @doc """
  Spawns an OS process via `Port.open/2` but the Port belongs to this long-lived GenServer.
  If the calling process dies (e.g. Livebook cell finishes), the Port stays alive.
  """
  def open_port(command_binary, args, port_opts \\ [:binary, :exit_status, :stderr_to_stdout]) do
    case ensure_started() do
      :ok -> GenServer.call(@name, {:open_port, command_binary, args, port_opts})
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Closes a Port managed by this GenServer.
  """
  def close_port(port) when is_port(port) do
    case ensure_started() do
      :ok -> GenServer.call(@name, {:close_port, port})
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Returns status info for a managed port.
  """
  def port_info(port) when is_port(port) do
    case ensure_started() do
      :ok -> GenServer.call(@name, {:port_info, port})
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def init(state) do
    # Trap exits so we can handle Port terminations gracefully.
    Process.flag(:trap_exit, true)
    {:ok, state}
  end

  @impl true
  def handle_call({:open_port, command_binary, args, port_opts}, _from, state) do
    try do
      options =
        port_opts
        |> Enum.reject(fn
          {:args, _} -> true
          _ -> false
        end)
        |> Kernel.++(args: args)

      port =
        Port.open({:spawn_executable, command_binary}, options)

      entry = %{command: command_binary, output: "", exit_status: nil}
      {:reply, {:ok, port}, Map.put(state, port, entry)}
    rescue
      e -> {:reply, {:error, Exception.message(e)}, state}
    end
  end

  def handle_call({:close_port, port}, _from, state) do
    if Port.info(port) != nil, do: Port.close(port)
    {:reply, :ok, Map.delete(state, port)}
  end

  def handle_call({:port_info, port}, _from, state) do
    case Map.get(state, port) do
      nil ->
        {:reply, {:error, :not_found}, state}

      %{output: output, exit_status: exit_status} = _entry ->
        alive? = Port.info(port) != nil
        {:reply, {:ok, %{alive?: alive?, output: output, exit_status: exit_status}}, state}
    end
  end

  @impl true
  def handle_info({port, {:data, data}}, state) when is_port(port) do
    new_state =
      Map.update(
        state,
        port,
        %{command: "unknown", output: to_string(data), exit_status: nil},
        fn entry ->
          updated_output =
            [entry.output, data]
            |> IO.iodata_to_binary()
            |> trim_output()

          %{entry | output: updated_output}
        end
      )

    {:noreply, new_state}
  end

  def handle_info({port, {:exit_status, status}}, state) when is_port(port) do
    cmd = state |> Map.get(port, %{command: "unknown command"}) |> Map.get(:command)

    Logger.debug(
      "[HpcConnect.TunnelManager] Background port for '#{cmd}' exited with status #{status}"
    )

    new_state =
      Map.update(state, port, %{command: cmd, output: "", exit_status: status}, fn entry ->
        %{entry | exit_status: status}
      end)

    {:noreply, new_state}
  end

  def handle_info({:EXIT, port, reason}, state) when is_port(port) do
    Logger.debug("[HpcConnect.TunnelManager] Port #{inspect(port)} exited: #{inspect(reason)}")

    new_state =
      Map.update(state, port, %{command: "unknown", output: "", exit_status: reason}, fn entry ->
        %{entry | exit_status: reason}
      end)

    {:noreply, new_state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp trim_output(output) when is_binary(output) do
    size = String.length(output)

    if size <= @max_output_chars do
      output
    else
      String.slice(output, size - @max_output_chars, @max_output_chars)
    end
  end
end
