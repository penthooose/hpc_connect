defmodule HpcConnect.Livebook do
  @moduledoc """
  Helpers for using `HpcConnect` from a shared Livebook with an uploaded team SSH key.

  This module does not depend on Kino directly; callers only need to pass the
  uploaded file path returned by `Kino.Input.file_path/1`.
  """

  alias HpcConnect.{Cluster, Credentials, Session}

  @type setup_mode :: :livebook | :local

  @spec new_session(atom() | binary() | Cluster.t(), binary(), binary(), keyword()) :: Session.t()
  def new_session(cluster_or_name, username, uploaded_key_path, opts \\ []) do
    cluster = resolve_cluster(cluster_or_name)
    credentials = Credentials.create(username, uploaded_key_path, opts)

    session_opts = [
      username: username,
      ssh_alias: cluster.ssh_alias || cluster.host,
      uploaded_key_path: uploaded_key_path,
      identity_file: credentials.identity_file,
      ssh_config_file: credentials.ssh_config_file,
      known_hosts_file: credentials.known_hosts_file,
      credential_dir: credentials.credential_dir,
      proxy_jump: nil,
      work_dir: Keyword.get(opts, :work_dir),
      vault_dir: Keyword.get(opts, :vault_dir),
      port_range: Keyword.get(opts, :port_range)
    ]

    cluster
    |> Session.new(compact_opts(session_opts))
  end

  @doc """
  One-call setup workflow for both notebook and local execution.

  Modes:

  - `mode: :livebook` (default):
    - if `username`, `cluster`, and `uploaded_key_path` are provided, they are used directly
    - otherwise, Kino inputs are rendered and read interactively
  - `mode: :local`:
    - expects `username` and `key_path` (or `uploaded_key_path`)
    - performs no Kino actions

  Shared options:
  - `:cluster` (default: `"fritz"`)
  - `:remote_command` (default: `"hostname && whoami"`)
  - `:work_dir`, `:vault_dir`, `:port_range`
  - `:connect_fun`, `:connect_opts`
  """
  @spec connection_setup(keyword()) :: map()
  def connection_setup(opts \\ []) do
    mode = Keyword.get(opts, :mode, :livebook)

    case mode do
      :livebook -> connection_setup_livebook(opts)
      :local -> connection_setup_local(opts)
      other -> raise ArgumentError, "unsupported mode #{inspect(other)}; use :livebook or :local"
    end
  end

  @doc """
  Convenience cleanup that accepts either a setup result map or a session.
  """
  @spec connection_cleanup(map() | Session.t(), keyword()) :: :ok
  def connection_cleanup(result_or_session, opts \\ [])
  def connection_cleanup(%{session: %Session{} = session}, opts), do: cleanup(session, opts)
  def connection_cleanup(%Session{} = session, opts), do: cleanup(session, opts)

  @doc """
  One-call workflow for Livebook:

  1. Builds a temporary session from an uploaded key path
  2. Executes a probe command over SSH
  3. Returns display-ready metadata for notebook rendering

  Options:
  - `:remote_command` (default: `"hostname && whoami"`)
  - `:uploaded_filename` for display purposes only
  - `:connect_fun` custom callback `(session, remote_command, connect_opts) -> binary()`
  - `:connect_opts` keyword options passed to `connect_fun`
  - Any options supported by `new_session/4`
  """
  @spec connect(atom() | binary() | Cluster.t(), binary(), binary(), keyword()) :: map()
  def connect(cluster_or_name, username, uploaded_key_path, opts \\ []) do
    remote_command = Keyword.get(opts, :remote_command, "hostname && whoami")
    uploaded_filename = Keyword.get(opts, :uploaded_filename)
    connect_opts = Keyword.get(opts, :connect_opts, [])

    connect_fun =
      Keyword.get(opts, :connect_fun) ||
        fn session, command, inner_opts ->
          HpcConnect.connect!(session, command, inner_opts)
        end

    session =
      new_session(
        cluster_or_name,
        username,
        uploaded_key_path,
        Keyword.drop(opts, [:remote_command, :uploaded_filename, :connect_fun, :connect_opts])
      )

    probe = connect_fun.(session, remote_command, connect_opts)

    command_preview =
      session |> HpcConnect.connect_command(remote_command) |> HpcConnect.command_preview()

    %{
      session: session,
      probe: probe,
      command_preview: command_preview,
      details: %{
        cluster: session.cluster.name,
        host: session.cluster.host,
        ssh_alias: session.ssh_alias,
        uploaded_filename: uploaded_filename,
        key_source_runtime_path: uploaded_key_path,
        credential_dir: session.credential_dir
      }
    }
  end

  @spec cleanup(Session.t() | binary() | map(), keyword()) :: :ok
  def cleanup(arg, opts \\ [])

  def cleanup(%Session{} = session, opts) do
    delete_uploaded? = Keyword.get(opts, :delete_uploaded, false)
    sweep_orphans? = Keyword.get(opts, :sweep_orphans, true)
    force_uploaded_delete? = Keyword.get(opts, :force_uploaded_delete, false)

    :ok =
      Credentials.cleanup_registered(session.credential_dir,
        delete_uploaded: delete_uploaded?,
        force_uploaded_delete: force_uploaded_delete?
      )

    if sweep_orphans? do
      :ok =
        Credentials.sweep_cleanup_registry(
          delete_uploaded: delete_uploaded?,
          force_uploaded_delete: force_uploaded_delete?
        )
    end

    :ok
  end

  def cleanup(arg, _opts), do: Credentials.cleanup(arg)

  @doc """
  Sweeps orphaned temp artifacts tracked in the cleanup registry.

  Useful after interrupted/paused notebook sessions where a previous `session`
  variable is no longer available.
  """
  @spec cleanup_orphans(keyword()) :: :ok
  def cleanup_orphans(opts \\ []) do
    Credentials.sweep_cleanup_registry(opts)
  end

  defp connection_setup_livebook(opts) do
    direct_username = Keyword.get(opts, :username)
    direct_cluster = Keyword.get(opts, :cluster, "fritz")
    direct_key_path = Keyword.get(opts, :uploaded_key_path)
    direct_uploaded_filename = Keyword.get(opts, :uploaded_filename)

    {username, cluster, uploaded_key_path, uploaded_filename, ui_rendered?} =
      if is_binary(direct_username) and is_binary(direct_key_path) do
        {
          String.trim(direct_username),
          normalize_cluster(direct_cluster),
          direct_key_path,
          direct_uploaded_filename || Path.basename(direct_key_path),
          false
        }
      else
        ensure_kino_available!()

        username_input = kino_input_text("HPC username", default: "your_username")

        cluster_input =
          kino_input_text(
            "Cluster alias or host (csnhr/fritz/alex/helma/tinyx/woody/meggie)",
            default: to_string(direct_cluster)
          )

        upload_input =
          kino_input_file(
            "Upload team SSH private key from your browser",
            accept: ~w(.pem .key id_rsa id_ed25519)
          )

        _layout =
          kino_layout_grid(
            [
              kino_markdown("### Fill in username and cluster"),
              username_input,
              cluster_input,
              kino_markdown("### Upload SSH private key file"),
              upload_input
            ],
            columns: 1,
            boxed: true
          )

        username = username_input |> kino_read() |> String.trim()
        cluster = cluster_input |> kino_read() |> normalize_cluster()
        uploaded = kino_read(upload_input)
        uploaded_key_path = kino_file_path(uploaded.file_ref)

        {
          username,
          cluster,
          uploaded_key_path,
          Map.get(uploaded, :client_name, Path.basename(uploaded_key_path)),
          true
        }
      end

    result =
      connect(cluster, username, uploaded_key_path,
        remote_command: Keyword.get(opts, :remote_command, "hostname && whoami"),
        uploaded_filename: uploaded_filename,
        connect_fun: Keyword.get(opts, :connect_fun),
        connect_opts: Keyword.get(opts, :connect_opts, []),
        work_dir: Keyword.get(opts, :work_dir),
        vault_dir: Keyword.get(opts, :vault_dir),
        port_range: Keyword.get(opts, :port_range)
      )

    Map.merge(result, %{mode: :livebook, ui_rendered?: ui_rendered?})
  end

  defp connection_setup_local(opts) do
    username =
      case Keyword.get(opts, :username) do
        value when is_binary(value) and value != "" -> value
        _ -> raise ArgumentError, "local mode requires :username"
      end

    cluster_name = normalize_cluster(Keyword.get(opts, :cluster, "fritz"))
    cluster = resolve_cluster(cluster_name)

    key_path =
      case Keyword.get(opts, :key_path, Keyword.get(opts, :uploaded_key_path)) do
        value when is_binary(value) and value != "" -> value
        _ -> raise ArgumentError, "local mode requires :key_path (or :uploaded_key_path)"
      end

    # In local mode we do NOT copy the key to a temp dir.
    # The user's ~/.ssh/config already handles ProxyJump etc. We just reference
    # the key directly, avoiding stale-temp-file failures across IEx restarts.
    session_opts =
      compact_opts(
        username: username,
        identity_file: key_path,
        work_dir: Keyword.get(opts, :work_dir),
        vault_dir: Keyword.get(opts, :vault_dir),
        port_range: Keyword.get(opts, :port_range)
      )

    session = Session.new(cluster, session_opts)

    remote_command = Keyword.get(opts, :remote_command, "hostname && whoami")

    connect_fun =
      Keyword.get(opts, :connect_fun) ||
        fn s, cmd, copts -> HpcConnect.connect!(s, cmd, copts) end

    probe = connect_fun.(session, remote_command, Keyword.get(opts, :connect_opts, []))

    command_preview =
      session |> HpcConnect.connect_command(remote_command) |> HpcConnect.command_preview()

    %{
      session: session,
      probe: probe,
      command_preview: command_preview,
      mode: :local,
      ui_rendered?: false,
      details: %{
        cluster: session.cluster.name,
        host: session.cluster.host,
        ssh_alias: session.ssh_alias,
        uploaded_filename: Keyword.get(opts, :uploaded_filename, Path.basename(key_path)),
        key_source_runtime_path: key_path,
        credential_dir: nil
      }
    }
  end

  defp normalize_cluster(value) when is_binary(value), do: String.trim(value)
  defp normalize_cluster(value), do: value

  defp ensure_kino_available! do
    unless Code.ensure_loaded?(kino_input_module()) and Code.ensure_loaded?(kino_layout_module()) and
             Code.ensure_loaded?(kino_markdown_module()) do
      raise ArgumentError,
            "Kino is required for mode: :livebook interactive setup. Add {:kino, \"~> 0.19\"} to Mix.install/1."
    end
  end

  defp kino_input_text(label, opts), do: apply(kino_input_module(), :text, [label, opts])
  defp kino_input_file(label, opts), do: apply(kino_input_module(), :file, [label, opts])
  defp kino_layout_grid(items, opts), do: apply(kino_layout_module(), :grid, [items, opts])
  defp kino_markdown(text), do: apply(kino_markdown_module(), :new, [text])
  defp kino_read(input), do: apply(kino_input_module(), :read, [input])
  defp kino_file_path(file_ref), do: apply(kino_input_module(), :file_path, [file_ref])

  defp kino_input_module, do: Module.concat([Kino, Input])
  defp kino_layout_module, do: Module.concat([Kino, Layout])
  defp kino_markdown_module, do: Module.concat([Kino, Markdown])

  defp resolve_cluster(%Cluster{} = cluster), do: cluster
  defp resolve_cluster(cluster_or_name), do: Cluster.fetch!(cluster_or_name)

  defp compact_opts(opts) do
    Enum.reject(opts, fn {_key, value} -> is_nil(value) end)
  end
end
