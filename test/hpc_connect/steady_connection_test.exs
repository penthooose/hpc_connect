defmodule HpcConnect.SteadyConnectionTest do
  use ExUnit.Case, async: true

  alias HpcConnect.{Session, SSH, SteadyConnection}

  defp session(opts \\ []) do
    HpcConnect.new_session(
      :fritz,
      Keyword.merge(
        [
          username: "hpcusr01",
          ssh_alias: "fritz",
          work_dir: "/scratch/hpc_connect",
          vault_dir: "/vault/models",
          steady_connection: true
        ],
        opts
      )
    )
  end

  describe "session parsing" do
    test "steady_connection defaults to false" do
      s = HpcConnect.new_session(:fritz, username: "hpcusr01", work_dir: "/w", vault_dir: "/v")
      refute s.steady_connection
      refute SteadyConnection.enabled?(s)
    end

    test "parses steady_connection from opts" do
      assert session(steady_connection: true).steady_connection
      refute session(steady_connection: false).steady_connection
    end

    test "parses steady_connection from .env via Session.local/1" do
      env_path =
        Path.join(System.tmp_dir!(), "hpc_connect_test_steady.env")
        |> tap(&File.write!(&1, "HPC_CONNECT_STEADY_CONNECTION=true\n"))

      on_exit(fn -> File.rm(env_path) end)

      s =
        Session.local(
          env_file: env_path,
          username: "hpcusr01",
          identity_file: "~/.ssh/id_hpc",
          work_dir: "/w",
          vault_dir: "/v"
        )

      assert s.steady_connection == true
      assert SteadyConnection.enabled?(s)
    end
  end

  describe "session_key/1" do
    test "is stable for the same session and distinct across sessions" do
      a = session()
      _b = session()

      assert SteadyConnection.session_key(a) == SteadyConnection.session_key(a)
      assert is_binary(SteadyConnection.session_key(a))
      assert String.starts_with?(SteadyConnection.session_key(a), "steady|")

      other = session(username: "otheruser", identity_file: "~/.ssh/other")
      refute SteadyConnection.session_key(a) == SteadyConnection.session_key(other)
    end
  end

  describe "ssh command building" do
    test "ssh_command carries session_key and keepalive options when enabled" do
      cmd = SSH.ssh_command(session(), "echo ok", "probe")
      assert is_binary(cmd.session_key)

      preview = HpcConnect.command_preview(cmd)
      assert preview =~ "ServerAliveInterval=30"
      assert preview =~ "ServerAliveCountMax=3"
    end

    test "ssh_command has no session_key/keepalive when disabled" do
      cmd = SSH.ssh_command(session(steady_connection: false), "echo ok", "probe")
      assert cmd.session_key == nil

      preview = HpcConnect.command_preview(cmd)
      refute preview =~ "ServerAliveInterval"
    end

    test "scp_to_command stays one-shot (no session_key) but gets keepalive" do
      cmd =
        SSH.scp_to_command(session(), "/local/file", "/remote/file", "upload", recursive: true)

      assert cmd.session_key == nil

      preview = HpcConnect.command_preview(cmd)
      assert preview =~ "ServerAliveInterval=30"
    end
  end

  describe "lifecycle without a live connection" do
    test "connected?/lookup_server are false/nil before connection" do
      s = session(steady_connection: false)
      refute SteadyConnection.connected?(s)
      assert SteadyConnection.lookup_server(s) == nil
    end

    test "ensure_connected!/open_steady_connection! are no-ops when disabled" do
      s = session(steady_connection: false)
      assert HpcConnect.open_steady_connection!(s) == s
      assert SteadyConnection.ensure_connected!(s) == s
    end

    test "disconnect is a no-op when disabled" do
      assert HpcConnect.close_steady_connection(session(steady_connection: false)) == :ok
    end
  end

  describe "public API" do
    test "steady_connection?/1" do
      assert HpcConnect.steady_connection?(session())
      refute HpcConnect.steady_connection?(session(steady_connection: false))
    end
  end

  describe "remote env prefix" do
    test "does not export steady config keys" do
      s =
        session()
        |> Session.put_env("STEADY_SSH_CONNECTION", "true")
        |> Session.put_env("HPC_CONNECT_STEADY_CONNECTION", "true")
        |> Session.put_env("REAL_VAR", "1")

      prefix = Session.remote_env_prefix(s)
      refute prefix =~ "STEADY_SSH"
      refute prefix =~ "HPC_CONNECT_STEADY"
      assert prefix =~ "REAL_VAR"
    end
  end
end
