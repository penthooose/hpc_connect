defmodule HpcConnect.SSH do
  @moduledoc """
  Helpers for building and executing portable SSH / SCP commands.
  """

  alias HpcConnect.{Command, Session, Shell}

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
    cmd_opts = [stderr_to_stdout: true] ++ opts
    System.cmd(binary, args, cmd_opts)
  end

  @spec preview_arg(binary()) :: binary()
  def preview_arg(arg) do
    if String.contains?(arg, [" ", "\t", "\n", "\"", "'"]) do
      Shell.escape(arg)
    else
      arg
    end
  end

  @spec ssh_binary() :: binary()
  def ssh_binary, do: System.find_executable("ssh") || "ssh"

  @spec scp_binary() :: binary()
  def scp_binary, do: System.find_executable("scp") || "scp"

  @doc """
  Builds an SSH local port-forwarding command.

  The tunnel connects `local_port` on localhost to `remote_port` on the compute node,
  **through the cluster login host as a TCP gateway**.

  This avoids opening an SSH session to the compute node itself (which is blocked on
  some clusters or may require password auth even when login-node key auth works).

  The returned `%Command{}` can be inspected with `command_preview/1` or run as a
  background Port via `open_proxy!/1`.
  """
  @spec port_forward_command(Session.t(), binary(), pos_integer(), pos_integer()) :: Command.t()
  def port_forward_command(%Session{} = session, node, local_port, remote_port) do
    proxy_jump_target = proxy_jump_target(session)

    args =
      []
      |> maybe_append_option("-F", session.ssh_config_file)
      |> maybe_append_option("-i", session.identity_file)
      |> maybe_append_option("-J", proxy_jump_target)
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
        ["-N"],
        ["-L", "#{local_port}:#{node}:#{remote_port}"],
        [Session.target(session)]
      ])
      |> List.flatten()

    %Command{
      binary: ssh_binary(),
      args: args,
      summary:
        "Tunnel localhost:#{local_port} -> #{node}:#{remote_port} via login #{session.cluster.host}",
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
    base =
      []
      |> maybe_append_option("-F", session.ssh_config_file)
      |> maybe_append_option("-J", proxy_jump_target(session))
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
    base =
      []
      |> maybe_append_option("-F", session.ssh_config_file)
      |> maybe_append_option("-J", proxy_jump_target(session))
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

    args =
      []
      |> maybe_append_option("-i", session.identity_file)
      |> maybe_append_option("-F", session.ssh_config_file)
      |> maybe_append_option("-J", proxy_jump_target(session))
      |> Kernel.++([
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "PasswordAuthentication=no",
        "-o",
        "PreferredAuthentications=publickey",
        "-o",
        "NumberOfPasswordPrompts=0",
        "-M",
        "-N",
        "-S",
        socket_path,
        "-o",
        "ControlPersist=no",
        Session.target(session)
      ])

    port =
      Port.open({:spawn_executable, ssh_binary()}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        args: args
      ])

    :ok = wait_for_socket(socket_path, timeout)
    {%{session | master_socket: socket_path}, port}
  end

  @doc """
  Closes an SSH ControlMaster port opened by `open_master!/1`.
  """
  @spec close_master(port()) :: :ok
  def close_master(master_port) when is_port(master_port) do
    Port.close(master_port)
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

  ## Why this is better than per-command `ssh.exe`

  - One TCP handshake and key exchange for the whole session
  - No external processes per command → no CMD windows, works in Livebook
  - Connection-drop is detectable (`:ssh` sends `{:EXIT, conn, reason}`)
  - SCP upload goes over the same connection via SFTP — no `scp.exe`

  ## ProxyJump handling

  If the session has a `proxy_jump` host, one background OS process is opened:
  `ssh -N -L <free_port>:<target>:22 <proxy>`. This is a plain TCP tunnel —
  no interactive shell, no output, no visible window. Erlang `:ssh` then
  connects to `localhost:<free_port>`.

  Close with `close_connection/1`.

  Options: `:timeout` (ms, default 20_000).
  """
  @spec open_connection!(Session.t(), keyword()) :: {Session.t(), port() | nil}
  def open_connection!(%Session{} = session, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 20_000)
    attempts = Keyword.get(opts, :attempts, 3)

    # Suppress :ssh application logger output during connection attempts.
    # Erlang :ssh emits [notice]/[debug] messages on auth failures which are
    # confusing when a retry immediately succeeds. We restore the original
    # level (or remove the filter) after the final attempt, whether it
    # succeeds or raises.
    filter_id = :hpc_connect_ssh_silence
    :logger.add_primary_filter(filter_id, {&suppress_ssh_logs/2, []})

    try do
      do_open_connection!(session, timeout, attempts)
    after
      :logger.remove_primary_filter(filter_id)
    end
  end

  # Logger primary filter: drops all log events from the :ssh OTP application.
  # Matches on application: :ssh (covers all domains and log levels emitted by
  # Erlang SSH, including the disconnect notice and KEX debug messages).
  defp suppress_ssh_logs(%{meta: %{application: :ssh}} = _event, _extra), do: :stop
  defp suppress_ssh_logs(_event, _extra), do: :ignore

  defp do_open_connection!(session, timeout, attempts_left) do
    proxy_jump =
      session.proxy_jump || session.cluster.proxy_jump

    ssh_opts = build_erlang_ssh_opts(session, timeout)

    direct_host = String.to_charlist(session.cluster.host)

    direct_result = :ssh.connect(direct_host, 22, ssh_opts, timeout)

    {conn, tunnel_os_port} =
      case direct_result do
        {:ok, conn} ->
          {conn, nil}

        {:error, direct_reason} ->
          if proxy_jump do
            local_port = find_free_local_port()
            target_host = session.cluster.host
            tunnel_args = build_proxy_tunnel_args(session, proxy_jump, target_host, local_port)

            os_port =
              Port.open({:spawn_executable, ssh_binary()}, [
                :binary,
                :exit_status,
                :stderr_to_stdout,
                args: tunnel_args
              ])

            # Poll until the tunnel's local port is accepting connections.
            wait_for_local_port(local_port, 8_000)

            case :ssh.connect(~c"127.0.0.1", local_port, ssh_opts, timeout) do
              {:ok, conn} ->
                {conn, os_port}

              {:error, reason} ->
                safe_port_close(os_port)

                if attempts_left > 1 and retryable_connect_error?(reason) do
                  Process.sleep(1_000)
                  do_open_connection!(session, timeout, attempts_left - 1)
                else
                  raise RuntimeError,
                        "SSH connect failed (direct: #{inspect(direct_reason)}, proxy: #{inspect(reason)})"
                end
            end
          else
            if attempts_left > 1 and retryable_connect_error?(direct_reason) do
              Process.sleep(1_000)
              do_open_connection!(session, timeout, attempts_left - 1)
            else
              raise RuntimeError, "SSH connect failed: #{inspect(direct_reason)}"
            end
          end
      end

    updated =
      %{session | ssh_conn: conn, tunnel_port: tunnel_os_port}

    {updated, tunnel_os_port}
  end

  @doc """
  Closes a persistent SSH connection opened by `open_connection!/1`.

  Also closes the proxy tunnel OS port if present.
  Returns the session with `ssh_conn` and `tunnel_port` cleared.
  """
  @spec close_connection(Session.t()) :: Session.t()
  def close_connection(%Session{ssh_conn: conn, tunnel_port: tunnel} = session) do
    if conn, do: :ssh.close(conn)
    safe_port_close(tunnel)
    %{session | ssh_conn: nil, tunnel_port: nil}
  end

  defp safe_port_close(nil), do: :ok

  defp safe_port_close(port) when is_port(port) do
    if Port.info(port) != nil, do: Port.close(port)
    :ok
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
  defp wait_for_local_port(port, timeout_ms, waited \\ 0) do
    case :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 500) do
      {:ok, sock} ->
        :gen_tcp.close(sock)
        :ok

      {:error, _} when waited < timeout_ms ->
        Process.sleep(200)
        wait_for_local_port(port, timeout_ms, waited + 200)

      {:error, reason} ->
        raise RuntimeError, "proxy tunnel on port #{port} did not open: #{inspect(reason)}"
    end
  end

  @doc """
  Executes a remote command over a **persistent** `:ssh` connection.

  The session must have been updated by `open_connection!/1`. Falls back to
  the OS `ssh.exe` path when `session.ssh_conn` is `nil`.

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
  using SFTP. No `scp.exe` needed.

  Falls back to OS `scp.exe` when `session.ssh_conn` is `nil`.
  """
  @spec upload!(Session.t(), binary(), binary(), keyword()) :: :ok
  def upload!(%Session{ssh_conn: nil} = session, local_path, remote_path, opts) do
    recursive? = Keyword.get(opts, :recursive, false)

    cmd =
      scp_to_command(session, local_path, remote_path, "upload #{local_path}",
        recursive: recursive?
      )

    {output, status} = run(cmd)

    if status == 0 do
      :ok
    else
      raise RuntimeError, "scp failed (status #{status}): #{String.trim(output)}"
    end
  end

  def upload!(%Session{ssh_conn: conn}, local_path, remote_path, opts) do
    timeout = Keyword.get(opts, :timeout, 60_000)
    recursive? = Keyword.get(opts, :recursive, false)

    {:ok, sftp} = :ssh_sftp.start_channel(conn, timeout: timeout)

    try do
      if recursive? and File.dir?(local_path) do
        sftp_upload_dir(sftp, local_path, remote_path, timeout)
      else
        sftp_upload_file(sftp, local_path, remote_path, timeout)
      end
    after
      :ssh_sftp.stop_channel(sftp)
    end

    :ok
  end

  # Recursively uploads a directory tree via SFTP.
  defp sftp_upload_dir(sftp, local_dir, remote_dir, timeout) do
    sftp_mkdir_p(sftp, remote_dir, timeout)

    File.ls!(local_dir)
    |> Enum.each(fn name ->
      local_child = Path.join(local_dir, name)
      remote_child = remote_dir <> "/" <> name

      if File.dir?(local_child) do
        sftp_upload_dir(sftp, local_child, remote_child, timeout)
      else
        sftp_upload_file(sftp, local_child, remote_child, timeout)
      end
    end)
  end

  defp sftp_upload_file(sftp, local_path, remote_path, timeout) do
    data = File.read!(local_path)

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

  # Build the proxy tunnel args: ssh -N -L local_port:target:22 proxy_host
  defp build_proxy_tunnel_args(session, proxy_jump, target_host, local_port) do
    []
    |> maybe_append_option("-i", session.identity_file)
    |> maybe_append_option("-F", session.ssh_config_file)
    |> Kernel.++([
      "-N",
      "-L",
      "#{local_port}:#{target_host}:22",
      "-o",
      "IdentitiesOnly=yes",
      "-o",
      "PasswordAuthentication=no",
      "-o",
      "PreferredAuthentications=publickey",
      "-o",
      "NumberOfPasswordPrompts=0",
      "-o",
      "StrictHostKeyChecking=no",
      "-o",
      "UserKnownHostsFile=/dev/null"
    ])
    |> Kernel.++([
      if(session.username, do: "#{session.username}@#{proxy_jump}", else: proxy_jump)
    ])
  end

  # Build Erlang :ssh option list from a session.
  defp build_erlang_ssh_opts(session, timeout) do
    base = [
      user: String.to_charlist(session.username || ""),
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
