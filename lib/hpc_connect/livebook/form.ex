defmodule HpcConnect.Livebook.Form do
  @moduledoc """
  A persistent owner process for a `Kino.Control.form` that keeps the form's
  event subscription alive **across cell runs**.

  A plain form only delivers submit events to the process that subscribed to it.
  When the setup cell finishes, that process dies, so pressing the submit button
  again does nothing. This module fixes that: the first cell run renders the
  form, attaches it to the owner, and blocks (`await/2`) until the first submit.
  The owner stays subscribed afterwards, so the user can **edit the values and
  press submit again** without re-running the cell. Each submit re-invokes the
  handler and re-renders the status frame (if any) with fresh feedback.

  ## Usage

      owner = HpcConnect.Livebook.Form.ensure(:my_setup)
      :ok = HpcConnect.Livebook.Form.attach(owner, form, status_frame, handler)
      result = HpcConnect.Livebook.Form.await(owner)

  The handler is `fun.(data, event) -> {result, frame_content}`, where:

    * `data` - the form values map (`event.data`)
    * `event` - the full submit event (includes `:origin`)
    * `result` - returned to the `await/2` caller
    * `frame_content` - a Kino term rendered into `status_frame` (or `nil`)

  The owner is registered under a `:global` name derived from the given `name`,
  so it is created once per runtime and survives Livebook cell processes.
  """

  use GenServer

  @type handler :: (map(), map() -> {term(), term() | nil})

  @doc """
  Returns the owner process for `name`, starting it on first use.
  """
  @spec ensure(term()) :: pid()
  def ensure(name) do
    reg = {:global, {__MODULE__, name}}

    case :global.whereis_name(reg) do
      :undefined ->
        case GenServer.start(__MODULE__, name, name: reg) do
          {:ok, pid} -> pid
          {:error, {:already_started, pid}} -> pid
        end

      pid when is_pid(pid) ->
        pid
    end
  end

  @doc """
  Attaches `form` to the owner, replacing any previous subscription, and
  registers the handler + status frame used for every submit.
  """
  @spec attach(pid(), term(), term() | nil, handler()) :: :ok
  def attach(pid, form, status_frame, handler) do
    GenServer.call(pid, {:attach, form, status_frame, handler}, :infinity)
  end

  @doc """
  Blocks the caller until the next submit event has been handled; returns the
  handler's `result`. Re-run this from a re-executed cell to get fresh values.
  """
  @spec await(pid(), timeout()) :: term()
  def await(pid, timeout \\ :infinity) do
    GenServer.call(pid, :await, timeout)
  end

  @doc false
  # Test/diagnostic helper: drives the same handler path as a real client submit
  # by delivering an event tagged for the currently attached form.
  @spec submit(pid(), map()) :: :ok
  def submit(pid, data) do
    GenServer.call(pid, {:submit, data}, :infinity)
  end

  @impl true
  def init(name) do
    {:ok, %{name: name, form: nil, tag: nil, frame: nil, handler: nil, waiter: nil}}
  end

  @impl true
  def handle_call({:attach, form, status_frame, handler}, _from, state) do
    # Drop a previous subscription (best-effort; the old widget may already be
    # destroyed after a cell re-run, which is harmless).
    if state.tag, do: safe_unsubscribe(state.form)

    tag = {__MODULE__, make_ref()}
    :ok = kino_control_subscribe(form, tag)

    {:reply, :ok,
     %{state | form: form, tag: tag, frame: status_frame, handler: handler, waiter: nil}}
  end

  def handle_call(:await, from, state) do
    {:noreply, %{state | waiter: from}}
  end

  def handle_call({:submit, data}, _from, state) do
    send(self(), {state.tag, %{type: :submit, data: data, origin: nil}})
    {:reply, :ok, state}
  end

  @impl true
  def handle_info({tag, %{type: :submit} = event}, %{tag: tag} = state) do
    data = Map.get(event, :data, %{})

    # The handler returns either `{result, frame_content}` (valid; reply to the
    # awaiting cell) or `{:retry, frame_content}` (invalid; render the feedback
    # into the frame and KEEP the waiter so the cell stays blocked until a valid
    # submit). A raised handler is treated the same way as `:retry`.
    waiter =
      try do
        case state.handler.(data, event) do
          {:retry, frame_content} ->
            render_frame(state.frame, frame_content, Map.get(event, :origin))
            state.waiter

          {result, frame_content} ->
            render_frame(state.frame, frame_content, Map.get(event, :origin))
            if state.waiter, do: GenServer.reply(state.waiter, result)
            nil
        end
      rescue
        error ->
          render_frame(state.frame, error_content(Exception.message(error)), Map.get(event, :origin))
          state.waiter
      end

    {:noreply, %{state | waiter: waiter}}
  end

  def handle_info(_message, state) do
    {:noreply, state}
  end

  defp error_content(message) do
    case :persistent_term.get({__MODULE__, :error_content_fun}, nil) do
      fun when is_function(fun, 1) -> fun.(message)
      _ -> apply(Module.concat([Kino, Markdown]), :new, ["**Setup error:** #{message}"])
    end
  end

  defp render_frame(nil, _content, _origin), do: :ok
  defp render_frame(_frame, nil, _origin), do: :ok

  defp render_frame(frame, content, origin) do
    opts = if origin, do: [to: origin], else: []
    apply(Module.concat([Kino, Frame]), :render, [frame, content, opts])
  end

  defp safe_unsubscribe(form) do
    try do
      apply(Module.concat([Kino, Control]), :unsubscribe, [form])
    rescue
      _ -> :ok
    end
  end

  # Overridable via :persistent_term for hermetic unit tests (hpc_connect does
  # not depend on Kino); in Livebook the real Kino.Control.subscribe/2 is used.
  defp kino_control_subscribe(control, tag) do
    case :persistent_term.get({__MODULE__, :subscribe_fun}, nil) do
      fun when is_function(fun, 2) -> fun.(control, tag)
      _ -> apply(Module.concat([Kino, Control]), :subscribe, [control, tag])
    end
  end
end
