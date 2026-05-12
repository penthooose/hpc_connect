defmodule HpcConnect.SSH do
  @moduledoc """
  Helpers for building and executing portable SSH / SCP commands.
  """

  require Logger
  alias HpcConnect.{Command, Session, Shell, TunnelManager}

  @spec ssh_command(Session.t(), binary(), binary()) :: Command.t()
  def ssh_command(%Session{} = session, remote_command, summary) do
    remote_command =
      case Session.remote_env_prefix(session) do
        "" -> remote_command
        prefix -> prefix <> " && " <> remote_command
      end

    %Command{
      binary: ssh_binary(),
      args:
        ssh_option_args(session) ++
          [Session.target(session), "bash -lc #{Shell.escape(remote_command)}"],
      summary: summary,
      remote_command: remote_command
    }
  end

  @spec scp_to_command(Session.t(), binary(), binary(), binary(), keyword()) :: Command.t()
  def scp_to_command(%Session{} = session, local_path, remote_path, summary, opts \\ []) do
    recursive? = Keyword.get(opts, :recursive, false)
    recursive_args = if recursive?, do: ["-r"], else: []

    %Command{
      binary: scp_binary(),
      args:
        recursive_args ++
          scp_option_args(session) ++ [local_path, "#{Session.target(session)}:#{remote_path}"],
      summary: summary,
      remote_command: nil
    }
  end

  @spec run(Command.t(), keyword()) :: {binary(), non_neg_integer()}
  def run(%Command{binary: binary, args: args}, opts \\ []) do
    retries = Keyword.get(opts, :retries, 1)
    delay_ms = Keyword.get(opts, :retry_delay_ms, 2_000)
    cmd_opts = [stderr_to_stdout: true] ++ Keyword.drop(opts, [:retries, :retry_delay_ms])

    run_with_retry(binary, args, cmd_opts, retries, delay_ms)
  end

  @transient_openssh_errors [
    "connection refused",
    "connection closed",
    "connection timed out",
    "timed out during banner exchange",
    "connect to host",
    "no route to host",
    "network is unreachable",
    "kex_exchange_identification",
    "unknown port 65535",
    "connection reset by peer",
    "temporary failure in name resolution",
    "could not resolve hostname"
  ]

  defp run_with_retry(binary, args, cmd_opts, retries_left, delay_ms) do
    {output, status} = System.cmd(binary, args, cmd_opts)

    if retries_left > 0 and retryable_openssh_failure?(binary, status, output) do
      Process.sleep(delay_ms)
      run_with_retry(binary, args, cmd_opts, retries_left - 1, delay_ms)
    else
      {output, status}
    end
  end

  defp retryable_openssh_failure?(binary, status, output)
       when is_binary(binary) and is_integer(status) and is_binary(output) do
    status == 255 and openssh_binary?(binary) and transient_openssh_error?(output)
  end

  defp retryable_openssh_failure?(_, _, _), do: false

  defp openssh_binary?(binary) do
    case binary |> Path.basename() |> String.downcase() do
      "ssh" -> true
      "ssh.exe" -> true
      "scp" -> true
      "scp.exe" -> true
      _ -> false
    end
  end

  defp transient_openssh_error?(message) when is_binary(message) do
    down = String.downcase(message)
    Enum.any?(@transient_openssh_errors, &String.contains?(down, &1))
  end

  @spec preview_arg(binary()) :: binary()
  def preview_arg(arg) do
    if String.contains?(arg, [" ", "\t", "\n", "\"", "'"]) do
      Shell.escape(arg)
    else
      arg
    end
  end

  @doc false
  @spec preview_binary(binary()) :: binary()
  def preview_binary(binary) do
    binary
    |> to_string()
    |> Path.basename()
    |> String.replace_suffix(".exe", "")
    |> preview_arg()
  end

  @spec ssh_binary() :: binary()
  def ssh_binary, do: System.find_executable("ssh") || "ssh"

  @spec scp_binary() :: binary()
  def scp_binary, do: System.find_executable("scp") || "scp"

  @doc """
  Returns an equivalent OpenSSH command preview for the native login connection.

  This is intended for diagnostics only.
  """
  @spec native_login_connect_preview(Session.t()) :: binary()
  def native_login_connect_preview(%Session{} = session) do
    # Native Erlang :ssh connects DIRECTLY to the login node (no -J / no proxy).
    # The ~/.ssh/config ProxyJump is an OS-level convenience that :ssh does not use.
    direct_target =
      case session.username do
        u when is_binary(u) and u != "" -> "#{u}@#{session.cluster.host}"
        _ -> session.cluster.host
      end

    args =
      []
      |> maybe_append_option("-F", session.ssh_config_file)
      |> maybe_append_option("-i", session.identity_file)
      |> Kernel.++([
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
        "ConnectTimeout=30",
        direct_target
      ])

    preview_command(ssh_binary(), args)
  end

  @doc """
  Returns an equivalent OpenSSH command preview for compute-node port forwarding.

  `local_port` may be `nil` to indicate auto-port selection.
  """
  @spec native_compute_tunnel_preview(Session.t(), binary(), pos_integer(), pos_integer() | nil) ::
          binary()
  def native_compute_tunnel_preview(%Session{} = session, node, remote_port, local_port \\ nil) do
    jump_chain_target = compute_jump_target(session)

    compute_target =
      case session.username do
        username when is_binary(username) and username != "" -> "#{username}@#{node}"
        _ -> node
      end

    local_port_arg =
      if is_integer(local_port) and local_port > 0, do: to_string(local_port), else: "<auto>"

    args =
      []
      |> maybe_append_option("-i", session.identity_file)
      |> maybe_append_option("-J", jump_chain_target)
      |> Kernel.++(maybe_user_known_hosts_args(session))
      |> Kernel.++([
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
        "ConnectTimeout=30",
        "-o",
        "ExitOnForwardFailure=yes",
        "-N",
        "-L",
        "#{local_port_arg}:localhost:#{remote_port}",
        compute_target
      ])

    preview_command(ssh_binary(), args)
  end

  @doc false
  @spec master_command_preview(Session.t(), binary()) :: binary()
  def master_command_preview(%Session{} = session, socket_path \\ "<socket>") do
    preview_command(ssh_binary(), open_master_args(session, socket_path))
  end

  @doc """
  Builds an SSH local port-forwarding command.

  The tunnel connects `local_port` on localhost to `remote_port` on the **compute node**
  by opening an SSH session to that node and hopping through the login node via `-J`.
  This matches the common OpenSSH pattern:

      ssh -N -L <local_port>:localhost:<remote_port> -J <login> <user>@<compute_node>

  The returned `%Command{}` can be inspected with `command_preview/1` or run as a
  background Port via `open_proxy!/1`.
  """
  @spec port_forward_command(Session.t(), binary(), pos_integer(), pos_integer()) :: Command.t()
  def port_forward_command(%Session{} = session, node, local_port, remote_port) do
    # Use an explicit jump chain so port forwarding works without depending on
    # any generated ssh_config file. For FAU clusters this may be:
    #   user@csnhr,user@alex
    jump_chain_target = compute_jump_target(session)

    compute_target =
      case session.username do
        username when is_binary(username) and username != "" -> "#{username}@#{node}"
        _ -> node
      end

    args =
      []
      |> maybe_append_option("-i", session.identity_file)
      |> maybe_append_option("-J", jump_chain_target)
      |> Kernel.++(maybe_user_known_hosts_args(session))
      |> Kernel.++([
        ["-o", "BatchMode=yes"],
        ["-o", "IdentitiesOnly=yes"],
        ["-o", "PasswordAuthentication=no"],
        ["-o", "PreferredAuthentications=publickey"],
        ["-o", "NumberOfPasswordPrompts=0"],
        ["-o", "ConnectTimeout=30"],
        ["-o", "ExitOnForwardFailure=yes"],
        ["-o", "StrictHostKeyChecking=accept-new"],
        ["-o", "ServerAliveInterval=30"],
        ["-o", "ServerAliveCountMax=3"],
        ["-o", "LogLevel=ERROR"],
        ["-N"],
        ["-L", "#{local_port}:localhost:#{remote_port}"],
        [compute_target]
      ])
      |> List.flatten()

    %Command{
      binary: ssh_binary(),
      args: args,
      summary:
        "Tunnel localhost:#{local_port} -> #{node}:#{remote_port} via jump #{jump_chain_target}",
      remote_command: nil
    }
  end

  @doc """
  Finds a free local TCP port by binding to port 0.
  """
  @spec find_free_local_port() :: pos_integer()
  def find_free_local_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, ip: :loopback, reuseaddr: true])
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end

  defp ssh_option_args(session) do
    proxy_jump =
      if include_explicit_proxy_jump?(session), do: proxy_jump_target(session), else: nil

    base =
      []
      |> maybe_append_option("-F", session.ssh_config_file)
      |> maybe_append_option("-J", proxy_jump)
      |> maybe_append_option("-i", session.identity_file)

    base =
      base ++
        [
          "-o",
          "IdentitiesOnly=yes",
          "-o",
          "PasswordAuthentication=no",
          "-o",
          "PreferredAuthentications=publickey",
          "-o",
          "NumberOfPasswordPrompts=0",
          "-o",
          "ConnectTimeout=30"
        ]

    case session.master_socket do
      nil -> base
      socket -> base ++ ["-o", "ControlPath=#{socket}"]
    end
  end

  defp scp_option_args(session) do
    proxy_jump =
      if include_explicit_proxy_jump?(session), do: proxy_jump_target(session), else: nil

    base =
      []
      |> maybe_append_option("-F", session.ssh_config_file)
      |> maybe_append_option("-J", proxy_jump)
      |> maybe_append_option("-i", session.identity_file)

    base =
      base ++
        [
          "-o",
          "IdentitiesOnly=yes",
          "-o",
          "PasswordAuthentication=no",
          "-o",
          "PreferredAuthentications=publickey",
          "-o",
          "NumberOfPasswordPrompts=0",
          "-o",
          "ConnectTimeout=30"
        ]

    case session.master_socket do
      nil -> base
      socket -> base ++ ["-o", "ControlPath=#{socket}"]
    end
  end

  @doc """
  Opens an SSH ControlMaster background connection and returns `{updated_session, master_port}`.

  All subsequent SSH/SCP commands built from `updated_session` will multiplex over the
  established connection — no new TCP handshake or key exchange per command.

  The connection lives as long as the returned port. Close with `close_master/1` or by
  closing the port directly.

  **Requires OpenSSH ≥ 6.7 on all platforms. On Windows ≥ 10 (build 1803) the
  Win32-OpenSSH Unix socket support must be present.**
  """
  @spec open_master!(Session.t(), keyword()) :: {Session.t(), port()}
  def open_master!(%Session{} = session, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 15_000)

    socket_path =
      Path.join(
        System.tmp_dir!(),
        "hpc_ctl_#{:erlang.unique_integer([:positive, :monotonic])}"
      )

    args = open_master_args(session, socket_path)

    port =
      case TunnelManager.open_port(ssh_binary(), args, [:binary, :exit_status, :stderr_to_stdout]) do
        {:ok, managed_port} ->
          managed_port

        {:error, reason} ->
          raise RuntimeError, "failed to open SSH master process: #{inspect(reason)}"
      end

    :ok = wait_for_socket(socket_path, timeout)
    {%{session | master_socket: socket_path}, port}
  end

  @doc """
  Closes an SSH ControlMaster port opened by `open_master!/1`.
  """
  @spec close_master(port()) :: :ok
  def close_master(master_port) when is_port(master_port) do
    _ = TunnelManager.close_port(master_port)
    :ok
  end

  # Polls for the socket file to appear (created by SSH when the master is ready).
  defp wait_for_socket(_path, timeout) when timeout <= 0,
    do: {:error, :timeout}

  defp wait_for_socket(path, timeout) do
    if File.exists?(path) do
      :ok
    else
      Process.sleep(200)
      wait_for_socket(path, timeout - 200)
    end
  end

  defp maybe_append_option(args, _flag, nil), do: args
  defp maybe_append_option(args, _flag, ""), do: args
  defp maybe_append_option(args, flag, value), do: args ++ [flag, value]

  defp preview_command(binary, args) do
    ([preview_binary(binary)] ++ args)
    |> Enum.map(&preview_arg(to_string(&1)))
    |> Enum.join(" ")
  end

  # Livebook sessions use generated ssh_config files that already define
  # ProxyJump per host. Avoid appending an extra `-J` in that case.
  defp include_explicit_proxy_jump?(%Session{
         ssh_config_file: config,
         credential_dir: credential_dir
       })
       when is_binary(config) and config != "" and is_binary(credential_dir) and
              credential_dir != "" do
    false
  end

  defp include_explicit_proxy_jump?(_session), do: true

  defp compute_jump_target(%Session{} = session) do
    login_target = direct_login_target(session)

    case proxy_jump_target(session) do
      nil -> login_target
      jump -> "#{jump},#{login_target}"
    end
  end

  defp open_master_args(%Session{} = session, socket_path) do
    proxy_jump =
      if include_explicit_proxy_jump?(session), do: proxy_jump_target(session), else: nil

    []
    |> maybe_append_option("-i", session.identity_file)
    |> maybe_append_option("-F", session.ssh_config_file)
    |> maybe_append_option("-J", proxy_jump)
    |> Kernel.++(maybe_user_known_hosts_args(session))
    |> Kernel.++([
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
      "ConnectTimeout=30",
      "-o",
      "StrictHostKeyChecking=accept-new",
      "-o",
      "LogLevel=ERROR",
      "-M",
      "-N",
      "-S",
      socket_path,
      "-o",
      "ControlPersist=no",
      Session.target(session)
    ])
  end

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

  # ---------------------------------------------------------------------------
  # Persistent Erlang :ssh connection
  # ---------------------------------------------------------------------------

  @doc """
  Opens a **persistent native SSH connection** using Erlang's built-in `:ssh` application.

  Returns `{updated_session, tunnel_port_or_nil}`. `updated_session` carries
  `ssh_conn` and (when ProxyJump is needed) `tunnel_port`. Pass it to all subsequent
  API calls — commands will run over the connection without spawning any new OS
  processes.

  ## Why this is better than per-command OpenSSH

  - One TCP handshake and key exchange for the whole session
  - No external processes per command → no CMD windows, works in Livebook
  - Connection-drop is detectable (`:ssh` sends `{:EXIT, conn, reason}`)
  - SCP upload goes over the same connection via SFTP — no external `scp` process

  ## ProxyJump handling

  By default, native mode does **not** open any OS OpenSSH process for proxying.
  If direct native connect fails and a proxy jump is required, set
  `proxy_jump_via_os: true` explicitly to allow the managed OpenSSH tunnel.

  Close with `close_connection/1`.

  Options:
  - `:timeout` (ms, default 20_000)
  - `:proxy_jump_via_native` (default `true`)
  - `:proxy_jump_via_os` (default `false`)
  """
  @spec open_connection!(Session.t(), keyword()) :: {Session.t(), port() | nil}
  def open_connection!(%Session{} = session, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 20_000)
    attempts = Keyword.get(opts, :attempts, 3)
    proxy_jump_via_native = Keyword.get(opts, :proxy_jump_via_native, true)
    proxy_jump_via_os = Keyword.get(opts, :proxy_jump_via_os, false)

    # Suppress :ssh application logger output during connection attempts.
    # Erlang :ssh emits [notice]/[debug] messages on auth failures which are
    # confusing when a retry immediately succeeds. We restore the original
    # level (or remove the filter) after the final attempt, whether it
    # succeeds or raises.
    filter_id = :hpc_connect_ssh_silence
    :logger.add_primary_filter(filter_id, {&suppress_ssh_logs/2, []})

    try do
      do_open_connection!(session, timeout, attempts, proxy_jump_via_native, proxy_jump_via_os)
    after
      :logger.remove_primary_filter(filter_id)
    end
  end

  # Logger primary filter: drops all log events from the :ssh OTP application.
  # Matches on application: :ssh (covers all domains and log levels emitted by
  # Erlang SSH, including the disconnect notice and KEX debug messages).
  defp suppress_ssh_logs(%{meta: %{application: :ssh}} = _event, _extra), do: :stop
  defp suppress_ssh_logs(_event, _extra), do: :ignore

  defp do_open_connection!(
         session,
         timeout,
         attempts_left,
         proxy_jump_via_native,
         proxy_jump_via_os
       ) do
    proxy_jump =
      session.proxy_jump || session.cluster.proxy_jump

    has_proxy_jump = is_binary(proxy_jump) and proxy_jump != ""

    ssh_opts = build_erlang_ssh_opts(session, timeout)

    direct_host = String.to_charlist(session.cluster.host)

    direct_result = :ssh.connect(direct_host, 22, ssh_opts, timeout)

    {conn, tunnel_os_port, jump_conn, jump_tunnel_port} =
      case direct_result do
        {:ok, conn} ->
          {conn, nil, nil, nil}

        {:error, direct_reason} ->
          cond do
            has_proxy_jump and proxy_jump_via_native ->
              case connect_via_native_proxy_jump(session, ssh_opts, timeout) do
                {:ok, proxy_conn, proxy_jump_conn, proxy_tunnel_port} ->
                  {proxy_conn, nil, proxy_jump_conn, proxy_tunnel_port}

                {:error, native_reason} ->
                  if has_proxy_jump and proxy_jump_via_os do
                    case connect_via_os_proxy_jump(session, ssh_opts, timeout) do
                      {:ok, proxy_conn, os_port} ->
                        {proxy_conn, os_port, nil, nil}

                      {:error, os_reason} ->
                        maybe_retry_or_raise!(
                          session,
                          timeout,
                          attempts_left,
                          proxy_jump_via_native,
                          proxy_jump_via_os,
                          direct_reason,
                          native_reason,
                          os_reason
                        )
                    end
                  else
                    maybe_retry_or_raise!(
                      session,
                      timeout,
                      attempts_left,
                      proxy_jump_via_native,
                      proxy_jump_via_os,
                      direct_reason,
                      native_reason,
                      nil
                    )
                  end
              end

            has_proxy_jump and proxy_jump_via_os ->
              case connect_via_os_proxy_jump(session, ssh_opts, timeout) do
                {:ok, proxy_conn, os_port} ->
                  {proxy_conn, os_port, nil, nil}

                {:error, os_reason} ->
                  maybe_retry_or_raise!(
                    session,
                    timeout,
                    attempts_left,
                    proxy_jump_via_native,
                    proxy_jump_via_os,
                    direct_reason,
                    nil,
                    os_reason
                  )
              end

            has_proxy_jump ->
              maybe_retry_or_raise!(
                session,
                timeout,
                attempts_left,
                proxy_jump_via_native,
                proxy_jump_via_os,
                direct_reason,
                nil,
                nil
              )

            true ->
              if attempts_left > 1 and retryable_connect_error?(direct_reason) do
                Process.sleep(1_000)

                do_open_connection!(
                  session,
                  timeout,
                  attempts_left - 1,
                  proxy_jump_via_native,
                  proxy_jump_via_os
                )
              else
                raise RuntimeError, "SSH connect failed: #{inspect(direct_reason)}"
              end
          end
      end

    updated =
      %{
        session
        | ssh_conn: conn,
          tunnel_port: tunnel_os_port,
          jump_ssh_conn: jump_conn,
          jump_tunnel_port: jump_tunnel_port
      }

    {updated, tunnel_os_port}
  end

  defp connect_via_native_proxy_jump(%Session{} = session, ssh_opts, timeout) do
    {jump_user, jump_host} = proxy_jump_identity(session)

    jump_opts = build_erlang_ssh_opts(session, timeout, jump_user)

    Logger.debug(
      "[HpcConnect] native proxy-jump equivalent OpenSSH: #{native_login_connect_preview(session)}"
    )

    target_candidates =
      [session.cluster.host, session.cluster.ssh_alias]
      |> Enum.filter(&(is_binary(&1) and &1 != ""))
      |> Enum.uniq()

    Logger.debug(
      "[HpcConnect] native proxy-jump: connecting to jump host #{jump_host} as #{jump_user}"
    )

    case :ssh.connect(String.to_charlist(jump_host), 22, jump_opts, timeout) do
      {:ok, jump_conn} ->
        Logger.debug(
          "[HpcConnect] native proxy-jump: jump host connected, trying targets: #{inspect(target_candidates)}"
        )

        case try_native_proxy_targets(jump_conn, target_candidates, ssh_opts, timeout) do
          {:ok, conn, local_tunnel_port} ->
            Logger.debug(
              "[HpcConnect] native proxy-jump: tunnel established on local port #{local_tunnel_port}"
            )

            {:ok, conn, jump_conn, local_tunnel_port}

          {:error, reason} ->
            Logger.debug("[HpcConnect] native proxy-jump: all targets failed: #{inspect(reason)}")
            :ssh.close(jump_conn)
            {:error, {:native_proxy_jump_failed, reason}}
        end

      {:error, reason} ->
        Logger.debug(
          "[HpcConnect] native proxy-jump: jump host connect failed: #{inspect(reason)}"
        )

        {:error, {:native_proxy_jump_failed, reason}}
    end
  end

  defp try_native_proxy_targets(jump_conn, targets, ssh_opts, timeout) do
    do_try_native_proxy_targets(jump_conn, targets, ssh_opts, timeout, [])
  end

  defp do_try_native_proxy_targets(_jump_conn, [], _ssh_opts, _timeout, reasons) do
    {:error, {:native_proxy_connect_failed, Enum.reverse(reasons)}}
  end

  defp do_try_native_proxy_targets(jump_conn, [target | rest], ssh_opts, timeout, reasons) do
    Logger.debug("[HpcConnect] native proxy-jump: tcpip_tunnel_to_server -> #{target}:22")

    case :ssh.tcpip_tunnel_to_server(
           jump_conn,
           ~c"127.0.0.1",
           0,
           String.to_charlist(target),
           22,
           timeout
         ) do
      {:ok, local_tunnel_port} ->
        Logger.debug(
          "[HpcConnect] native proxy-jump: tunnel port #{local_tunnel_port} open, " <>
            "using :gen_tcp then :ssh.connect/3 to avoid Erlang SSH self-connect deadlock"
        )

        # Use :gen_tcp for the TCP hop — avoids a deadlock where the Erlang :ssh
        # application would be on both sides of the loopback tunnel simultaneously.
        # Then use :ssh.connect/3 (socket form) to do SSH over the raw TCP socket.
        case :gen_tcp.connect(~c"127.0.0.1", local_tunnel_port, [:binary, active: false], timeout) do
          {:ok, tcp_sock} ->
            Logger.debug(
              "[HpcConnect] native proxy-jump: TCP connected to tunnel, starting SSH handshake to #{target}..."
            )

            # The socket form :ssh.connect/3 does NOT accept connect_timeout — only
            # the host/port form :ssh.connect/4 does. Including it causes the SSH FSM
            # to stall or crash. Strip it before handing opts to the socket form.
            handshake_opts = Keyword.delete(ssh_opts, :connect_timeout)

            Logger.debug("[HpcConnect] ssh_opts for handshake: #{inspect(handshake_opts)}")

            # Put the socket into the exact mode Erlang's SSH FSM expects.
            :inet.setopts(tcp_sock, active: false, packet: :raw, mode: :binary)

            case :ssh.connect(tcp_sock, handshake_opts, timeout) do
              {:ok, conn} ->
                Logger.debug(
                  "[HpcConnect] native proxy-jump: SSH handshake to #{target} succeeded!"
                )

                {:ok, conn, local_tunnel_port}

              {:error, reason} ->
                Logger.debug(
                  "[HpcConnect] native proxy-jump: SSH handshake to #{target} failed: #{inspect(reason)}"
                )

                :gen_tcp.close(tcp_sock)

                do_try_native_proxy_targets(
                  jump_conn,
                  rest,
                  ssh_opts,
                  timeout,
                  [{target, {:ssh_handshake_failed, reason}} | reasons]
                )
            end

          {:error, reason} ->
            Logger.debug(
              "[HpcConnect] native proxy-jump: :gen_tcp.connect to tunnel port #{local_tunnel_port} failed: #{inspect(reason)}"
            )

            do_try_native_proxy_targets(
              jump_conn,
              rest,
              ssh_opts,
              timeout,
              [{target, {:tcp_connect_failed, reason}} | reasons]
            )
        end

      {:error, reason} ->
        Logger.debug(
          "[HpcConnect] native proxy-jump: tcpip_tunnel_to_server to #{target} failed: #{inspect(reason)}"
        )

        do_try_native_proxy_targets(
          jump_conn,
          rest,
          ssh_opts,
          timeout,
          [{target, {:tunnel_setup_failed, reason}} | reasons]
        )
    end
  end

  defp connect_via_os_proxy_jump(%Session{} = session, ssh_opts, timeout) do
    local_port = find_free_local_port()
    tunnel_args = build_proxy_tunnel_args(session, local_port)

    os_port =
      case TunnelManager.open_port(ssh_binary(), tunnel_args, [
             :binary,
             :exit_status,
             :stderr_to_stdout
           ]) do
        {:ok, managed_port} ->
          managed_port

        {:error, reason} ->
          {:error, {:os_proxy_tunnel_spawn_failed, reason}}
      end

    with port when is_port(port) <- os_port do
      try do
        # Poll until the tunnel's local port is accepting connections.
        wait_for_local_port(local_port, port, 15_000)

        case :ssh.connect(~c"127.0.0.1", local_port, ssh_opts, timeout) do
          {:ok, conn} ->
            {:ok, conn, port}

          {:error, reason} ->
            safe_port_close(port)
            {:error, {:os_proxy_connect_failed, reason}}
        end
      rescue
        e ->
          safe_port_close(port)
          {:error, {:os_proxy_tunnel_failed, Exception.message(e)}}
      end
    else
      {:error, _reason} = err -> err
    end
  end

  defp maybe_retry_or_raise!(
         session,
         timeout,
         attempts_left,
         proxy_jump_via_native,
         proxy_jump_via_os,
         direct_reason,
         native_reason,
         os_reason
       ) do
    retryable? =
      Enum.any?([direct_reason, native_reason, os_reason], fn
        nil -> false
        reason -> retryable_connect_error?(reason)
      end)

    if attempts_left > 1 and retryable? do
      Process.sleep(1_000)

      do_open_connection!(
        session,
        timeout,
        attempts_left - 1,
        proxy_jump_via_native,
        proxy_jump_via_os
      )
    else
      cond do
        not is_nil(native_reason) and not is_nil(os_reason) ->
          raise RuntimeError,
                "SSH connect failed (direct: #{inspect(direct_reason)}, " <>
                  "native_proxy_jump: #{inspect(native_reason)}, " <>
                  "os_proxy_jump: #{inspect(os_reason)})."

        not is_nil(native_reason) ->
          raise RuntimeError,
                "SSH connect failed (direct: #{inspect(direct_reason)}, " <>
                  "native_proxy_jump: #{inspect(native_reason)}). " <>
                  "If you want OS proxy fallback, pass proxy_jump_via_os: true explicitly."

        not is_nil(os_reason) ->
          raise RuntimeError,
                "SSH connect failed (direct: #{inspect(direct_reason)}, " <>
                  "os_proxy_jump: #{inspect(os_reason)})."

        true ->
          raise RuntimeError,
                "SSH connect failed: #{inspect(direct_reason)}. " <>
                  "This cluster may require ProxyJump; native proxy-jump tunneling is enabled by default, " <>
                  "and OS proxy fallback is disabled unless proxy_jump_via_os: true is set."
      end
    end
  end

  @doc """
  Closes a persistent SSH connection opened by `open_connection!/1`.

  Also closes the proxy tunnel OS port if present.
  Returns the session with `ssh_conn` and `tunnel_port` cleared.
  """
  @spec close_connection(Session.t()) :: Session.t()
  def close_connection(
        %Session{
          ssh_conn: conn,
          tunnel_port: tunnel,
          jump_ssh_conn: jump_conn,
          compute_ssh_conn: compute_conn
        } = session
      ) do
    if conn, do: :ssh.close(conn)
    if jump_conn, do: :ssh.close(jump_conn)
    if compute_conn, do: :ssh.close(compute_conn)
    safe_port_close(tunnel)

    %{
      session
      | ssh_conn: nil,
        tunnel_port: nil,
        jump_ssh_conn: nil,
        jump_tunnel_port: nil,
        compute_ssh_conn: nil
    }
  end

  defp safe_port_close(nil), do: :ok

  defp safe_port_close(port) when is_port(port) do
    _ = TunnelManager.close_port(port)
    :ok
  end

  @doc """
  Opens a native Erlang SSH TCP port-forward tunnel on an existing connection.

  Asks the SSH server (`conn`) to forward TCP connections from `local_port` on
  `127.0.0.1` to `target_host:target_port` through the established SSH channel.
  When `local_port` is `0` the OS picks a free port and the actual port is returned.

  This is the native equivalent of the OS `ssh -N -L local_port:target_host:target_port`
  command. The tunnel stays alive as long as `conn` is alive — no background
  process or OS port is required.

  Returns `{:ok, actual_local_port}` or `{:error, reason}`.
  """
  @spec open_native_tunnel(term(), binary(), pos_integer(), non_neg_integer(), non_neg_integer()) ::
          {:ok, pos_integer()} | {:error, term()}
  def open_native_tunnel(conn, target_host, target_port, local_port \\ 0, timeout \\ 10_000) do
    Logger.debug(
      "[HpcConnect] native tunnel: tcpip_tunnel_to_server conn=#{inspect(conn)} -> #{target_host}:#{target_port}"
    )

    case :ssh.tcpip_tunnel_to_server(
           conn,
           ~c"127.0.0.1",
           local_port,
           String.to_charlist(target_host),
           target_port,
           timeout
         ) do
      {:ok, actual_port} ->
        Logger.debug("[HpcConnect] native tunnel: listening on localhost:#{actual_port}")
        {:ok, actual_port}

      {:error, reason} = err ->
        Logger.debug("[HpcConnect] native tunnel: failed: #{inspect(reason)}")
        err
    end
  end

  @doc """
  Like `open_native_tunnel/5` but raises on failure.
  """
  @spec open_native_tunnel!(term(), binary(), pos_integer(), non_neg_integer(), non_neg_integer()) ::
          pos_integer()
  def open_native_tunnel!(conn, target_host, target_port, local_port \\ 0, timeout \\ 10_000) do
    case open_native_tunnel(conn, target_host, target_port, local_port, timeout) do
      {:ok, actual_port} -> actual_port
      {:error, reason} -> raise RuntimeError, "native SSH tunnel failed: #{inspect(reason)}"
    end
  end

  @doc """
  Opens a **two-hop** native SSH port-forward tunnel for accessing a vLLM service
  running on a compute node.

  This implements the native equivalent of:
      ssh -J user@login_node user@compute_node -N -L local_port:localhost:remote_port

  The flow is:
  1. Ask `login_conn` (an existing SSH session to the login node) to open a TCP
     channel to `compute_node:22`.
  2. TCP-connect locally to the tunnel endpoint and perform an SSH handshake to
     the compute node.
  3. On the compute-node SSH connection, open `tcpip_tunnel_to_server` forwarding
     `localhost:local_port` → `localhost:remote_port` on the compute node.

  Returns `{compute_conn, actual_local_port}`. **`compute_conn` must be kept alive**
  (e.g. stored in the session or result map) — closing it will tear down the tunnel.
  """
  @spec open_native_compute_tunnel!(
          Session.t(),
          term(),
          binary(),
          pos_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) ::
          {term(), pos_integer()}
  def open_native_compute_tunnel!(
        session,
        login_conn,
        compute_node,
        remote_port,
        local_port \\ 0,
        timeout \\ 20_000
      ) do
    Logger.debug(
      "[HpcConnect] native compute tunnel: opening TCP channel via login_conn to #{compute_node}:22"
    )

    # Step 1: tunnel local TCP port → compute_node:22, through the login-node SSH connection
    ssh_tunnel_port =
      case :ssh.tcpip_tunnel_to_server(
             login_conn,
             ~c"127.0.0.1",
             0,
             String.to_charlist(compute_node),
             22,
             timeout
           ) do
        {:ok, p} ->
          Logger.debug(
            "[HpcConnect] native compute tunnel: TCP channel to #{compute_node}:22 on local port #{p}"
          )

          p

        {:error, reason} ->
          raise RuntimeError,
                "native compute tunnel: failed to open TCP channel to #{compute_node}:22 via login node: #{inspect(reason)}"
      end

    # Step 2: TCP-connect locally to that tunnel port
    tcp_sock =
      case :gen_tcp.connect(~c"127.0.0.1", ssh_tunnel_port, [:binary, active: false], timeout) do
        {:ok, s} ->
          s

        {:error, r} ->
          raise RuntimeError,
                "native compute tunnel: TCP connect to tunnel port #{ssh_tunnel_port} failed: #{inspect(r)}"
      end

    # Step 3: SSH handshake to the compute node through the TCP tunnel
    # Strip connect_timeout — the socket form :ssh.connect/3 does not accept it
    # and it causes the SSH FSM to stall.
    ssh_opts = session |> build_erlang_ssh_opts(timeout) |> Keyword.delete(:connect_timeout)

    Logger.debug("[HpcConnect] compute tunnel ssh_opts for handshake: #{inspect(ssh_opts)}")

    # Ensure socket is in the correct mode for Erlang's SSH FSM.
    :inet.setopts(tcp_sock, active: false, packet: :raw, mode: :binary)

    compute_conn =
      case :ssh.connect(tcp_sock, ssh_opts, timeout) do
        {:ok, c} ->
          Logger.debug(
            "[HpcConnect] native compute tunnel: SSH handshake to #{compute_node} succeeded"
          )

          c

        {:error, reason} ->
          :gen_tcp.close(tcp_sock)

          raise RuntimeError,
                "native compute tunnel: SSH handshake to #{compute_node} failed: #{inspect(reason)}"
      end

    # Step 4: port-forward on the compute-node connection: localhost:local_port → localhost:remote_port
    actual_port =
      case :ssh.tcpip_tunnel_to_server(
             compute_conn,
             ~c"127.0.0.1",
             local_port,
             ~c"127.0.0.1",
             remote_port,
             timeout
           ) do
        {:ok, p} ->
          Logger.debug(
            "[HpcConnect] native compute tunnel: port forward localhost:#{p} → #{compute_node}:localhost:#{remote_port}"
          )

          p

        {:error, reason} ->
          :ssh.close(compute_conn)

          raise RuntimeError,
                "native compute tunnel: port forward on compute node failed: #{inspect(reason)}"
      end

    {compute_conn, actual_port}
  end

  defp retryable_connect_error?(reason) do
    # Auth failures are not retryable — only transient network errors are.
    auth_failure? =
      is_list(reason) and
        :lists.member(:unable_to_connect_using_available_authentication_methods, reason)

    not auth_failure? and
      (reason in [:econnrefused, :timeout, :closed, :ehostunreach, :enetunreach] or
         (is_list(reason) and
            Enum.any?(reason, &(&1 in [:econnrefused, :timeout, :closed, :ehostunreach]))) or
         String.contains?(inspect(reason), ["econnrefused", "timeout", "closed"]))
  end

  # Polls until localhost:port is accepting TCP connections (tunnel is ready).
  defp wait_for_local_port(local_port, os_port, timeout_ms, waited \\ 0)

  defp wait_for_local_port(local_port, os_port, timeout_ms, waited)
       when waited >= timeout_ms do
    {output, exit_status} = collect_port_messages(os_port)

    raise RuntimeError,
          "proxy tunnel on port #{local_port} did not open within #{timeout_ms}ms" <>
            port_failure_details(exit_status, output)
  end

  defp wait_for_local_port(local_port, os_port, timeout_ms, waited) do
    case :gen_tcp.connect(~c"127.0.0.1", local_port, [:binary, active: false], 500) do
      {:ok, sock} ->
        :gen_tcp.close(sock)
        :ok

      {:error, _reason} ->
        if Port.info(os_port) == nil do
          {output, exit_status} = collect_port_messages(os_port)

          raise RuntimeError,
                "proxy tunnel process exited before port #{local_port} became reachable" <>
                  port_failure_details(exit_status, output)
        else
          Process.sleep(200)
          wait_for_local_port(local_port, os_port, timeout_ms, waited + 200)
        end
    end
  end

  defp collect_port_messages(port, acc \\ "", exit_status \\ nil)

  defp collect_port_messages(port, acc, exit_status) do
    case TunnelManager.port_info(port) do
      {:ok, %{output: output, exit_status: manager_exit_status}} ->
        merged_output = IO.iodata_to_binary([acc, output || ""])
        {merged_output, manager_exit_status || exit_status}

      _ ->
        collect_port_messages_from_mailbox(port, acc, exit_status)
    end
  end

  defp collect_port_messages_from_mailbox(port, acc, exit_status) do
    receive do
      {^port, {:data, data}} ->
        collect_port_messages_from_mailbox(port, [acc, data], exit_status)

      {^port, {:exit_status, status}} ->
        collect_port_messages_from_mailbox(port, acc, status)
    after
      0 ->
        {IO.iodata_to_binary(acc), exit_status}
    end
  end

  defp port_failure_details(exit_status, output) do
    status_part = if is_integer(exit_status), do: " (exit #{exit_status})", else: ""
    output_part = output |> to_string() |> String.trim()

    case output_part do
      "" ->
        status_part

      text ->
        "#{status_part}: #{text}"
    end
  end

  @doc """
  Executes a remote command over a **persistent** `:ssh` connection.

  The session must have been updated by `open_connection!/1`. Falls back to
  the OS OpenSSH (`ssh`) path when `session.ssh_conn` is `nil`.

  Returns `{output_binary, exit_status}` — same contract as `run/2`.
  """
  @spec exec(Session.t(), binary(), keyword()) :: {binary(), non_neg_integer()}
  def exec(%Session{ssh_conn: nil} = session, command, opts) do
    # `System.cmd/3` does not accept `:timeout`; keep it for native `:ssh`
    # branch only and drop it when we fall back to OS ssh/scp commands.
    os_opts = Keyword.drop(opts, [:timeout])
    session |> ssh_command(command, command) |> run(os_opts)
  end

  def exec(%Session{ssh_conn: conn}, command, opts) do
    timeout = Keyword.get(opts, :timeout, 120_000)
    full_command = String.to_charlist(command)

    {:ok, chan} = :ssh_connection.session_channel(conn, timeout)
    :success = :ssh_connection.exec(conn, chan, full_command, timeout)

    collect_exec_output(conn, chan, "", nil, timeout)
  end

  @doc """
  Like `exec/3` but raises on non-zero exit status.
  """
  @spec exec!(Session.t(), binary(), keyword()) :: binary()
  def exec!(%Session{} = session, command, opts \\ []) do
    case exec(session, command, opts) do
      {output, 0} ->
        output

      {output, status} ->
        raise RuntimeError, "command failed (status #{status}): #{String.trim(output)}"
    end
  end

  @doc """
  Uploads a local file or directory to the remote over the persistent SSH connection
  using SFTP. No external `scp` process needed.

  Falls back to OS `scp` when `session.ssh_conn` is `nil`.
  """
  @spec upload!(Session.t(), binary(), binary(), keyword()) :: :ok
  def upload!(%Session{ssh_conn: nil} = session, local_path, remote_path, opts) do
    recursive? = Keyword.get(opts, :recursive, false)
    retries = Keyword.get(opts, :retries, 3)
    delay_ms = Keyword.get(opts, :retry_delay_ms, 3_000)

    {upload_path, cleanup_dir} = prepare_upload_source(local_path, recursive?, opts)

    try do
      cmd =
        scp_to_command(session, upload_path, remote_path, "upload #{local_path}",
          recursive: recursive?
        )

      run_scp_with_retry!(cmd, retries, delay_ms)
    after
      cleanup_upload_source(cleanup_dir)
    end
  end

  def upload!(%Session{ssh_conn: conn}, local_path, remote_path, opts) do
    timeout = Keyword.get(opts, :timeout, 60_000)
    recursive? = Keyword.get(opts, :recursive, false)

    {:ok, sftp} = :ssh_sftp.start_channel(conn, timeout: timeout)

    try do
      if recursive? and File.dir?(local_path) do
        sftp_upload_dir(sftp, local_path, remote_path, timeout, opts)
      else
        sftp_upload_file(sftp, local_path, remote_path, timeout, opts)
      end
    after
      :ssh_sftp.stop_channel(sftp)
    end

    :ok
  end

  @transient_scp_errors [
    "Connection refused",
    "Connection closed",
    "Connection timed out",
    "timed out during banner exchange",
    "connect to host",
    "No route to host",
    "Network is unreachable",
    "kex_exchange_identification",
    "UNKNOWN port 65535",
    "Connection reset by peer",
    "Temporary failure in name resolution"
  ]

  defp run_scp_with_retry!(%Command{} = cmd, retries_left, delay_ms) do
    {output, status} = run(cmd, retries: 0)

    if status == 0 do
      :ok
    else
      message = String.trim(output)
      retryable? = scp_retryable_failure?(status, message)

      if retryable? and retries_left > 0 do
        Process.sleep(delay_ms)
        run_scp_with_retry!(cmd, retries_left - 1, delay_ms)
      else
        raise RuntimeError, "scp failed (status #{status}): #{message}"
      end
    end
  end

  defp scp_retryable_failure?(status, message) when is_integer(status) and is_binary(message) do
    status == 255 and Enum.any?(@transient_scp_errors, &String.contains?(message, &1))
  end

  # Recursively uploads a directory tree via SFTP.
  defp sftp_upload_dir(sftp, local_dir, remote_dir, timeout, opts) do
    sftp_mkdir_p(sftp, remote_dir, timeout)

    File.ls!(local_dir)
    |> Enum.each(fn name ->
      local_child = Path.join(local_dir, name)
      remote_child = remote_dir <> "/" <> name

      if File.dir?(local_child) do
        sftp_upload_dir(sftp, local_child, remote_child, timeout, opts)
      else
        sftp_upload_file(sftp, local_child, remote_child, timeout, opts)
      end
    end)
  end

  defp sftp_upload_file(sftp, local_path, remote_path, timeout, opts) do
    data =
      local_path
      |> File.read!()
      |> maybe_normalize_upload_data(local_path, opts)

    case :ssh_sftp.write_file(sftp, String.to_charlist(remote_path), data, timeout) do
      :ok ->
        :ok

      {:error, reason} ->
        raise RuntimeError,
              "SFTP upload failed for #{local_path} -> #{remote_path}: #{inspect(reason)}"
    end
  end

  # mkdir -p equivalent over SFTP.
  defp sftp_mkdir_p(sftp, path, timeout) do
    segments = String.split(path, "/", trim: true)

    Enum.reduce(segments, "", fn seg, acc ->
      dir = acc <> "/" <> seg
      :ssh_sftp.make_dir(sftp, String.to_charlist(dir), timeout)
      # ignore error — dir may already exist
      dir
    end)

    :ok
  end

  # Collects output messages from an :ssh_connection exec channel.
  defp collect_exec_output(conn, chan, acc, exit_status, timeout) do
    receive do
      {:ssh_cm, ^conn, {:data, ^chan, _type, data}} ->
        collect_exec_output(conn, chan, acc <> data, exit_status, timeout)

      {:ssh_cm, ^conn, {:exit_status, ^chan, status}} ->
        collect_exec_output(conn, chan, acc, status, timeout)

      {:ssh_cm, ^conn, {:closed, ^chan}} ->
        {acc, exit_status || 0}

      {:ssh_cm, ^conn, {:eof, ^chan}} ->
        collect_exec_output(conn, chan, acc, exit_status, timeout)
    after
      timeout ->
        {acc, exit_status || -1}
    end
  end

  # Build the proxy tunnel args by reusing the same style as the regular
  # start_proxy/open_proxy flow:
  #   ssh -N -L <local_port>:127.0.0.1:22 <target> [-J <proxy>]
  defp build_proxy_tunnel_args(session, local_port) do
    []
    |> maybe_append_option("-i", session.identity_file)
    |> maybe_append_option("-J", proxy_jump_target(session))
    |> Kernel.++(maybe_user_known_hosts_args(session))
    |> Kernel.++([
      "-N",
      "-L",
      "#{local_port}:127.0.0.1:22",
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
      "ConnectTimeout=30",
      "-o",
      "ExitOnForwardFailure=yes",
      "-o",
      "StrictHostKeyChecking=accept-new",
      "-o",
      "ServerAliveInterval=30",
      "-o",
      "ServerAliveCountMax=3",
      "-o",
      "LogLevel=ERROR"
    ])
    |> Kernel.++([direct_login_target(session)])
  end

  defp direct_login_target(%Session{} = session) do
    case session.username do
      username when is_binary(username) and username != "" ->
        "#{username}@#{session.cluster.host}"

      _ ->
        session.cluster.host
    end
  end

  defp maybe_user_known_hosts_args(%Session{known_hosts_file: path})
       when is_binary(path) and path != "" do
    ["-o", "UserKnownHostsFile=#{path}"]
  end

  defp maybe_user_known_hosts_args(_session), do: []

  @doc false
  @spec normalize_line_endings(binary(), :lf | nil) :: binary()
  def normalize_line_endings(data, :lf) when is_binary(data) do
    data
    |> String.replace("\r\n", "\n")
    |> String.replace("\r", "\n")
  end

  def normalize_line_endings(data, _mode), do: data

  defp prepare_upload_source(local_path, recursive?, opts) do
    if normalize_upload?(local_path, recursive?, opts) do
      temp_root =
        Path.join(
          System.tmp_dir!(),
          "hpc_connect_upload_#{System.system_time(:millisecond)}_#{System.unique_integer([:positive])}"
        )

      staged_path = Path.join(temp_root, Path.basename(local_path))
      File.mkdir_p!(temp_root)

      if recursive? and File.dir?(local_path) do
        stage_upload_dir(local_path, staged_path, opts)
      else
        stage_upload_file(local_path, staged_path, opts)
      end

      {staged_path, temp_root}
    else
      {local_path, nil}
    end
  end

  defp cleanup_upload_source(nil), do: :ok
  defp cleanup_upload_source(path), do: File.rm_rf(path)

  defp normalize_upload?(local_path, recursive?, opts) do
    case Keyword.get(opts, :normalize_line_endings) do
      :lf -> recursive? or upload_path_matches_extension?(local_path, opts)
      _ -> false
    end
  end

  defp stage_upload_dir(source_dir, target_dir, opts) do
    File.mkdir_p!(target_dir)

    File.ls!(source_dir)
    |> Enum.each(fn name ->
      source_child = Path.join(source_dir, name)
      target_child = Path.join(target_dir, name)

      if File.dir?(source_child) do
        stage_upload_dir(source_child, target_child, opts)
      else
        stage_upload_file(source_child, target_child, opts)
      end
    end)
  end

  defp stage_upload_file(source_path, target_path, opts) do
    File.mkdir_p!(Path.dirname(target_path))

    data =
      source_path
      |> File.read!()
      |> maybe_normalize_upload_data(source_path, opts)

    File.write!(target_path, data)
  end

  defp maybe_normalize_upload_data(data, local_path, opts) do
    if upload_path_matches_extension?(local_path, opts) do
      normalize_line_endings(data, Keyword.get(opts, :normalize_line_endings))
    else
      data
    end
  end

  defp upload_path_matches_extension?(path, opts) do
    extensions =
      opts
      |> Keyword.get(:normalize_extensions, [])
      |> Enum.map(&String.downcase/1)

    ext = path |> Path.extname() |> String.downcase()

    extensions == [] or ext in extensions
  end

  defp proxy_jump_identity(%Session{} = session) do
    jump_raw =
      session.proxy_jump || session.cluster.proxy_jump ||
        raise RuntimeError, "missing proxy_jump host for native jump tunnel"

    jump_primary =
      jump_raw
      |> String.split(",", parts: 2)
      |> hd()
      |> String.trim()

    case String.split(jump_primary, "@", parts: 2) do
      [user, host] when user != "" and host != "" ->
        {user, host}

      [host] when host != "" ->
        {session.username || "", host}

      _ ->
        {session.username || "", jump_primary}
    end
  end

  # Build Erlang :ssh option list from a session.
  defp build_erlang_ssh_opts(session, timeout, username_override \\ nil) do
    username = username_override || session.username || ""

    base = [
      user: String.to_charlist(username),
      silently_accept_hosts: true,
      connect_timeout: timeout,
      # Don't write to ~/.ssh/known_hosts during tests/IEx sessions
      user_interaction: false,
      # Route all messages to the calling process (needed for exec output)
      id_string: :random
    ]

    key_opts =
      if session.identity_file do
        [key_cb: {HpcConnect.SSHKeyCallback, [identity_file: session.identity_file]}]
      else
        []
      end

    base ++ key_opts
  end
end
