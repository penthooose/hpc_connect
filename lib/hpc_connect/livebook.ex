defmodule HpcConnect.Livebook do
  @moduledoc """
  Helpers for using `HpcConnect` from a shared Livebook with an uploaded team SSH key.

  This module does not depend on Kino directly; callers only need to pass the
  uploaded file path returned by `Kino.Input.file_path/1`.
  """

  alias HpcConnect.{Cluster, Credentials, Session}

  @type setup_mode :: :livebook | :local
  @default_remote_command "hostname && whoami"
  @default_submit_label "Prepare Livebook session"

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
      port_range: Keyword.get(opts, :port_range)
    ]

    cluster
    |> Session.new(compact_opts(session_opts))
  end

  @doc """
  Renders a single Livebook form and returns bootstrap-ready options.

  This is the compact notebook-oriented entrypoint intended to keep Livebook
  setup to one cell:

      boot =
        HpcConnect.prepare_livebook_session(cluster: :alex)
        |> HpcConnect.bootstrap()

  If `:username` and `:uploaded_key_path` are already provided, no UI is shown
  and the options are only normalized/validated.

  Options:
  - `:cluster` – built-in cluster selection default (default: `"fritz"`)
  - `:username` – optional pre-filled username
  - `:uploaded_key_path` – optional direct uploaded-key path to skip UI
  - `:uploaded_filename` – optional display name for the uploaded key
  - `:hf_token` – optional Hugging Face token for gated model access
  - `:remote_command` – probe command (default: `"hostname && whoami"`)
  - `:submit_label` – submit button label for the form
  - `:persist_form` – when `true`, persist non-secret form defaults for reuse
  - `:persist_path` – custom path for persisted defaults

  Persisted defaults include cluster, username, and remote command.
  The uploaded SSH key and optional Hugging Face token are intentionally never persisted.
  """
  @spec prepare_session(keyword()) :: keyword()
  def prepare_session(opts \\ []) do
    direct_username = Keyword.get(opts, :username)
    direct_cluster = Keyword.get(opts, :cluster, "fritz")
    direct_key_path = Keyword.get(opts, :uploaded_key_path)
    direct_uploaded_filename = Keyword.get(opts, :uploaded_filename)

    prepared_opts =
      if is_binary(direct_username) and is_binary(direct_key_path) do
        build_prepared_session_opts(
          opts,
          %{
            username: direct_username,
            cluster: direct_cluster,
            uploaded_key_path: direct_key_path,
            uploaded_filename: direct_uploaded_filename,
            hf_token: Keyword.get(opts, :hf_token),
            remote_command: Keyword.get(opts, :remote_command)
          },
          false
        )
      else
        prepare_session_interactive(opts)
      end

    validate_prepared_session_opts!(prepared_opts)
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
  - `:port_range`
  - `:connect_fun`, `:connect_opts`
  """
  @spec connection_setup(keyword()) :: map()
  def connection_setup(opts \\ []) do
    mode = Keyword.get(opts, :mode, :livebook)
    clear_prepare_status? = Keyword.get(opts, :clear_prepare_status, true)

    try do
      case mode do
        :livebook ->
          connection_setup_livebook(opts)

        :local ->
          connection_setup_local(opts)

        other ->
          raise ArgumentError, "unsupported mode #{inspect(other)}; use :livebook or :local"
      end
    after
      if clear_prepare_status? do
        clear_prepare_status(opts)
      end
    end
  end

  @doc """
  Convenience cleanup that accepts either a setup result map or a session.

  `HpcConnect.cleanup_livebook_session/2`, which defaults to deleting the
  original uploaded temp key too.
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

    probe =
      connect_fun.(session, remote_command, connect_opts)
      |> sanitize_probe()

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
    prepared_opts = prepare_session(opts)
    username = Keyword.fetch!(prepared_opts, :username)
    cluster = Keyword.get(prepared_opts, :cluster, "fritz")
    uploaded_key_path = Keyword.fetch!(prepared_opts, :uploaded_key_path)
    uploaded_filename = Keyword.get(prepared_opts, :uploaded_filename)
    ui_rendered? = Keyword.get(prepared_opts, :ui_rendered?, false)

    result =
      connect(cluster, username, uploaded_key_path,
        remote_command: Keyword.get(prepared_opts, :remote_command, @default_remote_command),
        uploaded_filename: uploaded_filename,
        connect_fun: Keyword.get(prepared_opts, :connect_fun),
        connect_opts: Keyword.get(prepared_opts, :connect_opts, []),
        work_dir: Keyword.get(prepared_opts, :work_dir),
        vault_dir: Keyword.get(prepared_opts, :vault_dir),
        port_range: Keyword.get(prepared_opts, :port_range)
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

    key_path = Path.expand(key_path)

    unless File.exists?(key_path) do
      raise RuntimeError,
            "SSH private key not found: #{key_path}. Aborting before opening any SSH/proxy connection."
    end

    # Local mode references the key in place instead of copying it to a temp dir.
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

    connect_opts = Keyword.get(opts, :connect_opts, [])
    native_ssh? = Keyword.get(opts, :native_ssh, true)
    native_ssh_fallback_to_os? = Keyword.get(opts, :native_ssh_fallback_to_os, false)
    open_connection_opts = Keyword.get(opts, :open_connection_opts, [])

    {session, probe} =
      case Keyword.get(opts, :connect_fun) do
        fun when is_function(fun, 3) ->
          probe = fun.(session, remote_command, connect_opts) |> sanitize_probe()
          {session, probe}

        _ when native_ssh? ->
          try do
            {native_session, _tunnel} = HpcConnect.open_connection!(session, open_connection_opts)

            probe =
              HpcConnect.connect!(native_session, remote_command, connect_opts)
              |> sanitize_probe()

            {native_session, probe}
          rescue
            error ->
              if native_ssh_fallback_to_os? do
                probe =
                  HpcConnect.connect!(session, remote_command, connect_opts) |> sanitize_probe()

                {session, probe}
              else
                reraise error, __STACKTRACE__
              end
          end

        _ ->
          probe = HpcConnect.connect!(session, remote_command, connect_opts) |> sanitize_probe()
          {session, probe}
      end

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

  defp prepare_session_interactive(opts) do
    ensure_kino_available!()
    persisted = load_prepare_defaults(opts)
    default_cluster = prepare_default_cluster(opts, persisted)

    cluster_input =
      kino_input_select(
        "Cluster",
        livebook_cluster_options(),
        default: default_cluster
      )

    username_input =
      kino_input_text("HPC username",
        default: persisted["username"] || Keyword.get(opts, :username, "")
      )

    upload_input =
      kino_input_file(
        "Upload the team SSH private key",
        accept: :any
      )

    hf_token_input =
      kino_input_text("HF token (optional)",
        default: Keyword.get(opts, :hf_token, "")
      )

    remote_command_input =
      kino_input_text("Probe command",
        default:
          persisted["remote_command"] ||
            Keyword.get(opts, :remote_command, @default_remote_command)
      )

    form =
      kino_control_form(
        [
          cluster: cluster_input,
          username: username_input,
          ssh_key: upload_input,
          hf_token: hf_token_input,
          remote_command: remote_command_input
        ],
        submit: Keyword.get(opts, :submit_label, @default_submit_label)
      )

    status_frame = kino_frame_new(placeholder: false)

    layout =
      kino_layout_grid(
        [
          kino_markdown("""
          ## HPC Connect Livebook setup

          Fill out the form once, submit, and the returned options can go
          straight into `HpcConnect.bootstrap/1`.

          Non-secret defaults can be persisted for the next notebook run when
          `persist_form: true` is enabled. The uploaded SSH key is **never**
          persisted and must be selected again. The optional HF token is used
          for the current session only.
          """),
          form,
          status_frame,
          kino_markdown("""
          ### Recommended cleanup

          After you finish the notebook, use:

          `HpcConnect.cleanup_livebook_session(boot)`

          Use `HpcConnect.cleanup_livebook_orphans/1` only for crash recovery,
          when you no longer have the original `boot`/`session` variable.
          """)
        ],
        columns: 1,
        boxed: true,
        gap: 12
      )

    kino_render(layout)

    event = await_form_submit!(form)
    render_connecting_status(status_frame, event)
    data = Map.fetch!(event, :data)

    persist_prepare_defaults(opts, data)

    build_prepared_session_opts(
      opts,
      %{
        username: Map.get(data, :username),
        cluster: Map.get(data, :cluster),
        uploaded_key_path: uploaded_key_path_from_form!(Map.get(data, :ssh_key)),
        uploaded_filename: uploaded_filename_from_form(Map.get(data, :ssh_key)),
        hf_token: Map.get(data, :hf_token),
        remote_command: Map.get(data, :remote_command)
      },
      true
    )
    |> attach_prepare_status(status_frame, event)
  end

  defp build_prepared_session_opts(opts, values, ui_rendered?) do
    username =
      values |> Map.get(:username) |> normalize_required_text("Livebook setup requires :username")

    cluster =
      values |> Map.get(:cluster, Keyword.get(opts, :cluster, "fritz")) |> normalize_cluster()

    uploaded_key_path =
      values
      |> Map.get(:uploaded_key_path)
      |> normalize_required_text("Livebook setup requires :uploaded_key_path")
      |> Path.expand()

    uploaded_filename =
      values
      |> Map.get(:uploaded_filename)
      |> default_if_blank(Path.basename(uploaded_key_path))

    remote_command =
      values
      |> Map.get(:remote_command, Keyword.get(opts, :remote_command))
      |> default_if_blank(@default_remote_command)

    hf_token =
      values
      |> Map.get(:hf_token, Keyword.get(opts, :hf_token))
      |> normalize_optional_text()

    opts
    |> Keyword.delete(:work_dir)
    |> Keyword.delete(:vault_dir)
    |> Keyword.delete(:hf_token)
    |> Keyword.put(:mode, :livebook)
    |> Keyword.put(:cluster, cluster)
    |> Keyword.put(:username, username)
    |> Keyword.put(:uploaded_key_path, uploaded_key_path)
    |> Keyword.put(:uploaded_filename, uploaded_filename)
    |> Keyword.put(:remote_command, remote_command)
    |> Keyword.put(:ui_rendered?, ui_rendered?)
    |> maybe_put_opt(:hf_token, hf_token)
  end

  defp validate_prepared_session_opts!(prepared_opts) do
    uploaded_key_path = Keyword.fetch!(prepared_opts, :uploaded_key_path)

    unless File.exists?(uploaded_key_path) do
      raise RuntimeError,
            "SSH private key not found: #{uploaded_key_path}. Aborting before opening any SSH/proxy connection."
    end

    prepared_opts
  end

  defp await_form_submit!(form) do
    tag = {:hpc_connect_livebook_prepare, make_ref()}
    :ok = kino_control_subscribe(form, tag)

    receive do
      {^tag, %{type: :submit} = event} -> event
    end
  end

  defp render_connecting_status(status_frame, event) do
    render_opts =
      case Map.get(event, :origin) do
        nil -> []
        origin -> [to: origin]
      end

    kino_frame_render(status_frame, connecting_spinner_html(), render_opts)
  end

  @spec clear_prepare_status(keyword()) :: :ok
  def clear_prepare_status(opts) when is_list(opts) do
    case Keyword.get(opts, :ui_status_frame) do
      nil ->
        :ok

      status_frame ->
        render_opts =
          case Keyword.get(opts, :ui_status_origin) do
            nil -> []
            origin -> [to: origin]
          end

        kino_frame_clear(status_frame, render_opts)
        :ok
    end
  end

  defp attach_prepare_status(opts, status_frame, event) do
    opts
    |> Keyword.put(:ui_status_frame, status_frame)
    |> maybe_put_status_origin(Map.get(event, :origin))
  end

  defp maybe_put_opt(opts, _key, nil), do: opts
  defp maybe_put_opt(opts, _key, ""), do: opts
  defp maybe_put_opt(opts, key, value), do: Keyword.put(opts, key, value)

  defp maybe_put_status_origin(opts, nil), do: opts
  defp maybe_put_status_origin(opts, origin), do: Keyword.put(opts, :ui_status_origin, origin)

  defp uploaded_key_path_from_form!(nil) do
    raise ArgumentError, "Livebook setup requires an uploaded SSH private key"
  end

  defp uploaded_key_path_from_form!(%{file_ref: file_ref}) do
    kino_file_path(file_ref)
  end

  defp uploaded_filename_from_form(nil), do: nil
  defp uploaded_filename_from_form(%{} = upload), do: Map.get(upload, :client_name)

  defp livebook_cluster_options do
    Enum.map(Cluster.defaults(), fn cluster ->
      {cluster.name, cluster_option_label(cluster)}
    end)
  end

  defp cluster_option_label(%Cluster{} = cluster) do
    [Atom.to_string(cluster.name), cluster.host, cluster.notes]
    |> Enum.reject(&is_nil_or_blank?/1)
    |> Enum.join(" — ")
  end

  defp prepare_default_cluster(opts, persisted) do
    persisted_cluster = Map.get(persisted, "cluster")
    requested = persisted_cluster || Keyword.get(opts, :cluster, :fritz)

    case resolve_cluster_choice(requested) do
      nil -> :fritz
      value -> value
    end
  end

  defp resolve_cluster_choice(nil), do: nil
  defp resolve_cluster_choice(value) when is_atom(value), do: value

  defp resolve_cluster_choice(value) when is_binary(value) do
    cluster = value |> normalize_cluster() |> resolve_cluster_choice_from_string()
    cluster && cluster.name
  end

  defp resolve_cluster_choice(_value), do: nil

  defp resolve_cluster_choice_from_string(value) do
    Enum.find(Cluster.defaults(), fn cluster ->
      Atom.to_string(cluster.name) == value or cluster.host == value or value in cluster.aliases
    end)
  end

  defp load_prepare_defaults(opts) do
    if persist_prepare_defaults?(opts) do
      path = prepare_defaults_path(opts)

      with {:ok, json} <- File.read(path),
           {:ok, data} <- Jason.decode(json),
           true <- is_map(data) do
        data
      else
        _ -> %{}
      end
    else
      %{}
    end
  end

  defp persist_prepare_defaults(opts, data) do
    if persist_prepare_defaults?(opts) do
      defaults = %{
        "cluster" => cluster_value_for_persistence(Map.get(data, :cluster)),
        "username" => normalize_optional_text(Map.get(data, :username)),
        "remote_command" => normalize_optional_text(Map.get(data, :remote_command))
      }

      path = prepare_defaults_path(opts)
      _ = File.mkdir_p(Path.dirname(path))
      _ = File.write(path, Jason.encode!(defaults, pretty: true))
    end

    :ok
  end

  defp persist_prepare_defaults?(opts) do
    Keyword.get(opts, :persist_form, false) or is_binary(Keyword.get(opts, :persist_path))
  end

  defp prepare_defaults_path(opts) do
    Keyword.get(
      opts,
      :persist_path,
      Path.join([System.tmp_dir!(), "hpc_connect", "livebook_prepare_defaults.json"])
    )
  end

  defp cluster_value_for_persistence(value) when is_atom(value), do: Atom.to_string(value)

  defp cluster_value_for_persistence(value) when is_binary(value),
    do: normalize_optional_text(value)

  defp cluster_value_for_persistence(_value), do: nil

  defp normalize_required_text(value, error_message) do
    case normalize_optional_text(value) do
      nil -> raise ArgumentError, error_message
      normalized -> normalized
    end
  end

  defp normalize_optional_text(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      normalized -> normalized
    end
  end

  defp normalize_optional_text(value), do: value

  defp default_if_blank(value, default) do
    case normalize_optional_text(value) do
      nil -> default
      normalized -> normalized
    end
  end

  defp is_nil_or_blank?(nil), do: true
  defp is_nil_or_blank?(value) when is_binary(value), do: String.trim(value) == ""
  defp is_nil_or_blank?(_value), do: false

  defp ensure_kino_available! do
    unless Code.ensure_loaded?(kino_module()) and Code.ensure_loaded?(kino_input_module()) and
             Code.ensure_loaded?(kino_control_module()) and
             Code.ensure_loaded?(kino_frame_module()) and
             Code.ensure_loaded?(kino_html_module()) and
             Code.ensure_loaded?(kino_layout_module()) and
             Code.ensure_loaded?(kino_markdown_module()) do
      raise ArgumentError,
            "Kino is required for mode: :livebook interactive setup. Add {:kino, \"~> 0.19\"} to Mix.install/1."
    end
  end

  defp kino_render(term), do: apply(kino_module(), :render, [term])
  defp kino_frame_new(opts), do: apply(kino_frame_module(), :new, [opts])

  defp kino_frame_render(frame, term, opts),
    do: apply(kino_frame_module(), :render, [frame, term, opts])

  defp kino_frame_clear(frame, opts), do: apply(kino_frame_module(), :clear, [frame, opts])

  defp kino_html_new(html), do: apply(kino_html_module(), :new, [html])
  defp kino_input_text(label, opts), do: apply(kino_input_module(), :text, [label, opts])

  defp kino_input_select(label, options, opts),
    do: apply(kino_input_module(), :select, [label, options, opts])

  defp kino_input_file(label, opts), do: apply(kino_input_module(), :file, [label, opts])
  defp kino_control_form(fields, opts), do: apply(kino_control_module(), :form, [fields, opts])

  defp kino_control_subscribe(control, tag),
    do: apply(kino_control_module(), :subscribe, [control, tag])

  defp kino_layout_grid(items, opts), do: apply(kino_layout_module(), :grid, [items, opts])
  defp kino_markdown(text), do: apply(kino_markdown_module(), :new, [text])
  defp kino_file_path(file_ref), do: apply(kino_input_module(), :file_path, [file_ref])

  defp connecting_spinner_html do
    kino_html_new("""
    <style>
      @keyframes hpc-connect-spinner {
        from { transform: rotate(0deg); }
        to { transform: rotate(360deg); }
      }
    </style>
    <div style="display:flex;align-items:center;gap:12px;padding:12px 14px;margin:4px 0 2px 0;border:1px solid #d8dee4;border-radius:12px;background:#f6f8fa;">
      <div style="width:18px;height:18px;min-width:18px;border:2px solid #bfd8ff;border-top-color:#0969da;border-radius:50%;animation:hpc-connect-spinner 0.8s linear infinite;"></div>
      <div>
        <div style="font-weight:600;color:#24292f;">Connecting to HPC…</div>
        <div style="font-size:0.92rem;color:#57606a;">Preparing the SSH session, running the probe command, and collecting the initial startup summary.</div>
      </div>
    </div>
    """)
  end

  defp kino_module, do: Kino
  defp kino_frame_module, do: Module.concat([Kino, Frame])
  defp kino_html_module, do: Module.concat([Kino, HTML])
  defp kino_input_module, do: Module.concat([Kino, Input])
  defp kino_control_module, do: Module.concat([Kino, Control])
  defp kino_layout_module, do: Module.concat([Kino, Layout])
  defp kino_markdown_module, do: Module.concat([Kino, Markdown])

  defp resolve_cluster(%Cluster{} = cluster), do: cluster
  defp resolve_cluster(cluster_or_name), do: Cluster.fetch!(cluster_or_name)

  defp compact_opts(opts) do
    Enum.reject(opts, fn {_key, value} -> is_nil(value) end)
  end

  defp sanitize_probe(probe) when is_binary(probe) do
    probe
    |> String.replace(~r/\e\[[0-9;]*m/, "")
    |> String.split("\n")
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&header_or_warning_probe_line?/1)
    |> Enum.join("\n")
  end

  defp header_or_warning_probe_line?("") do
    true
  end

  defp header_or_warning_probe_line?(line) do
    String.contains?(line, ["WARNING:", "Path", "over quota", "!!!"]) or
      String.starts_with?(line, "ssh:") or
      String.starts_with?(line, "Connection")
  end
end
