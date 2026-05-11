defmodule HpcConnectTest do
  use ExUnit.Case
  doctest HpcConnect

  setup do
    on_exit(fn ->
      System.tmp_dir!()
      |> Path.join("hpc_connect")
      |> File.rm_rf()
    end)

    :ok
  end

  test "builds a plan using ssh alias configuration" do
    session =
      HpcConnect.new_session(:fritz,
        username: "hpcusr01",
        ssh_alias: "fritz",
        vault_dir: "/vault/models",
        work_dir: "/scratch/hpc_connect"
      )

    model = HpcConnect.new_model("meta-llama/Llama-3.1-8B-Instruct")
    job = HpcConnect.new_job(session, conda_env: "llm", port_range: {8100, 8200})

    plan = HpcConnect.plan(session, model, job)

    assert length(plan.install_scripts) == 2
    assert Path.basename(plan.download_model.binary) in ["ssh", "ssh.exe"]
    assert Enum.any?(plan.download_model.args, &(&1 == "fritz"))
    assert plan.reserve_port.remote_command =~ "find_free_port.sh"
    assert plan.start_vllm.remote_command =~ "start_vllm.sh"
  end

  test "falls back to explicit host and ssh options when no alias is set" do
    session =
      HpcConnect.new_session(:alex,
        username: "hpcusr01",
        ssh_alias: nil,
        identity_file: "~/.ssh/id_hpc",
        proxy_jump: "csnhr.nhr.fau.de"
      )

    command = HpcConnect.SSH.ssh_command(session, "echo ok", "probe")
    preview = HpcConnect.command_preview(command)

    assert preview =~ "-J"
    assert preview =~ "csnhr.nhr.fau.de"
    assert preview =~ "-i"
    assert preview =~ "hpcusr01@alex.nhr.fau.de"
  end

  test "renders model cache exports into the vllm command" do
    session =
      HpcConnect.new_session(:helma,
        username: "hpcusr01",
        ssh_alias: "helma",
        vault_dir: "/vault/models"
      )

    model =
      HpcConnect.new_model("Qwen/Qwen2.5-7B-Instruct",
        extra_env: %{"VLLM_WORKER_MULTIPROC_METHOD" => "spawn"}
      )

    job = HpcConnect.new_job(session, conda_env: "llm", port: 8123)
    command = HpcConnect.Scripts.start_vllm_command(session, model, job)

    assert command.remote_command =~ "HF_HOME=/vault/models/Qwen--Qwen2.5-7B-Instruct"
    assert command.remote_command =~ "VLLM_WORKER_MULTIPROC_METHOD=spawn"
    assert command.remote_command =~ "--port 8123"
  end

  test "resolves every documented server alias" do
    assert HpcConnect.cluster!(:csnhr).host == "csnhr.nhr.fau.de"
    assert HpcConnect.cluster!("csnhr").host == "csnhr.nhr.fau.de"
    assert HpcConnect.cluster!("csnhr.nhr.fau.de").host == "csnhr.nhr.fau.de"
    assert HpcConnect.cluster!("fritz").host == "fritz.nhr.fau.de"
    assert HpcConnect.cluster!("fritz.nhr.fau.de").host == "fritz.nhr.fau.de"
    assert HpcConnect.cluster!("alex").host == "alex.nhr.fau.de"
    assert HpcConnect.cluster!("alex.nhr.fau.de").host == "alex.nhr.fau.de"
    assert HpcConnect.cluster!("helma").host == "helma.nhr.fau.de"
    assert HpcConnect.cluster!("helma.nhr.fau.de").host == "helma.nhr.fau.de"
    assert HpcConnect.cluster!("tinyx").host == "tinyx.nhr.fau.de"
    assert HpcConnect.cluster!("tinyx.nhr.fau.de").host == "tinyx.nhr.fau.de"
    assert HpcConnect.cluster!("woody").host == "woody.nhr.fau.de"
    assert HpcConnect.cluster!("woody.nhr.fau.de").host == "woody.nhr.fau.de"
    assert HpcConnect.cluster!("meggie").host == "meggie.rrze.uni-erlangen.de"
    assert HpcConnect.cluster!("meggie.rrze.fau.de").host == "meggie.rrze.uni-erlangen.de"

    assert HpcConnect.cluster!("meggie.rrze.uni-erlangen.de").host ==
             "meggie.rrze.uni-erlangen.de"
  end

  test "builds a livebook session from an uploaded key path" do
    tmp_dir =
      Path.join([
        System.tmp_dir!(),
        "livebook",
        "test_session",
        "registered_files"
      ])

    File.mkdir_p!(tmp_dir)

    uploaded_key_path = Path.join(tmp_dir, "id_team")
    File.write!(uploaded_key_path, "FAKE_PRIVATE_KEY")

    session = HpcConnect.new_livebook_session(:fritz, "hpcusr01", uploaded_key_path)
    command = HpcConnect.connect_command(session, "hostname")
    preview = HpcConnect.command_preview(command)

    assert File.exists?(session.identity_file)
    assert File.exists?(session.ssh_config_file)
    assert File.exists?(session.known_hosts_file)
    assert session.credential_dir != nil
    assert preview =~ "-F"
    assert preview =~ session.ssh_config_file
    assert preview =~ "fritz"

    config = File.read!(session.ssh_config_file)
    assert config =~ "Host fritz.nhr.fau.de fritz"
    assert config =~ "Host meggie.rrze.fau.de meggie.rrze.uni-erlangen.de meggie"
    assert config =~ "User hpcusr01"

    assert session.uploaded_key_path == uploaded_key_path

    assert :ok == HpcConnect.cleanup_session(session)
    refute File.exists?(session.credential_dir)
    assert File.exists?(uploaded_key_path)

    assert :ok == HpcConnect.cleanup_session(session, delete_uploaded: true)
    refute File.exists?(uploaded_key_path)
  end

  test "runs one-call livebook connect workflow" do
    tmp_dir =
      Path.join([
        System.tmp_dir!(),
        "livebook",
        "test_session",
        "registered_files"
      ])

    File.mkdir_p!(tmp_dir)

    uploaded_key_path = Path.join(tmp_dir, "id_team_workflow")
    File.write!(uploaded_key_path, "FAKE_PRIVATE_KEY")

    result =
      HpcConnect.livebook_connect(:fritz, "hpcusr01", uploaded_key_path,
        remote_command: "echo READY",
        uploaded_filename: "id_team",
        connect_fun: fn _session, _command, _opts -> "READY\n" end,
        work_dir: "/scratch/hpc_connect",
        vault_dir: "/vault/models"
      )

    assert is_map(result)
    assert is_binary(result.probe)
    assert result.probe =~ "READY"
    assert result.command_preview =~ "echo"
    assert result.details.cluster == :fritz
    assert result.details.uploaded_filename == "id_team"
    assert File.exists?(result.session.identity_file)

    assert :ok == HpcConnect.cleanup_session(result.session, delete_uploaded: true)
    refute File.exists?(uploaded_key_path)
  end

  test "connection_setup supports local mode without kino" do
    tmp_dir =
      Path.join([
        System.tmp_dir!(),
        "livebook",
        "test_session",
        "registered_files"
      ])

    File.mkdir_p!(tmp_dir)

    key_path = Path.join(tmp_dir, "id_team_local_mode")
    File.write!(key_path, "FAKE_PRIVATE_KEY")

    result =
      HpcConnect.connection_setup(
        mode: :local,
        cluster: :alex,
        username: "hpcusr01",
        key_path: key_path,
        remote_command: "echo LOCAL",
        connect_fun: fn _session, _command, _opts -> "LOCAL\n" end
      )

    assert result.mode == :local
    assert result.ui_rendered? == false
    assert result.probe =~ "LOCAL"
    assert result.details.cluster == :alex

    # In local mode no temp credentials are created; cleanup is a no-op and
    # the original key file is intentionally NOT deleted.
    assert :ok == HpcConnect.connection_cleanup(result, delete_uploaded: true)
    assert File.exists?(key_path)
  end

  test "connection_setup local keeps original key when native_ssh is disabled" do
    tmp_dir = Path.join(System.tmp_dir!(), "hpc_connect_local_key_mode")
    File.mkdir_p!(tmp_dir)

    key_path = Path.join(tmp_dir, "id_hpc_original")
    fallback_path = key_path <> "_hpc_connect_pem"

    File.write!(key_path, "-----BEGIN OPENSSH PRIVATE KEY-----\nFAKE\n")
    File.write!(fallback_path, "-----BEGIN RSA PRIVATE KEY-----\nFAKE\n")

    result =
      HpcConnect.connection_setup(
        mode: :local,
        cluster: :alex,
        username: "hpcusr01",
        key_path: key_path,
        native_ssh: false,
        connect_fun: fn _session, _command, _opts -> "LOCAL\n" end
      )

    assert result.session.identity_file == Path.expand(key_path)
    refute result.session.identity_file == Path.expand(fallback_path)
  end

  test "bootstrap loads env file into session and returns startup summary" do
    tmp_dir = Path.join(System.tmp_dir!(), "hpc_connect_bootstrap_test")
    File.mkdir_p!(tmp_dir)

    env_file = Path.join(tmp_dir, ".env")
    File.write!(env_file, "HUGGINGFACE_HUB_TOKEN=test-token\nEXTRA_FLAG=1\n")

    key_path = Path.join(tmp_dir, "id_bootstrap")
    File.write!(key_path, "FAKE_PRIVATE_KEY")

    result =
      HpcConnect.bootstrap(
        mode: :local,
        cluster: :alex,
        username: "hpcusr01",
        key_path: key_path,
        env_file: env_file,
        remote_command: "echo BOOT",
        connect_fun: fn _session, _command, _opts -> "BOOT\n" end
      )

    assert result.probe =~ "BOOT"
    assert result.session.env["HUGGINGFACE_HUB_TOKEN"] == "test-token"
    assert Map.has_key?(result, :startup)
    assert Map.has_key?(result.startup, :available_gpus)
    assert Map.has_key?(result.startup, :downloaded_models)

    assert :ok == HpcConnect.connection_cleanup(result, delete_uploaded: true)
  end

  test "connection_setup supports livebook mode with pre-provided values" do
    tmp_dir =
      Path.join([
        System.tmp_dir!(),
        "livebook",
        "test_session",
        "registered_files"
      ])

    File.mkdir_p!(tmp_dir)

    uploaded_key_path = Path.join(tmp_dir, "id_team_livebook_direct")
    File.write!(uploaded_key_path, "FAKE_PRIVATE_KEY")

    result =
      HpcConnect.connection_setup(
        mode: :livebook,
        cluster: "fritz",
        username: "hpcusr01",
        uploaded_key_path: uploaded_key_path,
        uploaded_filename: "id_team",
        remote_command: "echo LIVEBOOK",
        connect_fun: fn _session, _command, _opts -> "LIVEBOOK\n" end
      )

    assert result.mode == :livebook
    assert result.ui_rendered? == false
    assert result.probe =~ "LIVEBOOK"
    assert result.details.uploaded_filename == "id_team"

    assert :ok == HpcConnect.connection_cleanup(result, delete_uploaded: true)
    refute File.exists?(uploaded_key_path)
  end

  test "cleanup_livebook_orphans removes artifacts from interrupted sessions" do
    tmp_dir =
      Path.join([
        System.tmp_dir!(),
        "livebook",
        "test_session",
        "registered_files"
      ])

    File.mkdir_p!(tmp_dir)

    uploaded_key_path = Path.join(tmp_dir, "id_team_orphan")
    File.write!(uploaded_key_path, "FAKE_PRIVATE_KEY")

    session = HpcConnect.new_livebook_session(:alex, "hpcusr01", uploaded_key_path)

    assert File.exists?(session.credential_dir)
    assert File.exists?(uploaded_key_path)

    # Simulate interrupted flow where cleanup_session/2 was not called.
    assert :ok == HpcConnect.cleanup_livebook_orphans(delete_uploaded: true)

    refute File.exists?(session.credential_dir)
    refute File.exists?(uploaded_key_path)
  end

  test "parses remote model listing output" do
    output = "Qwen--Qwen3-0.6B\nmeta-llama--Llama-3.2-1B-Instruct\n"

    assert HpcConnect.Model.parse_remote_listing(output) == [
             %{name: "Qwen--Qwen3-0.6B", repo_hint: "Qwen/Qwen3-0.6B"},
             %{
               name: "meta-llama--Llama-3.2-1B-Instruct",
               repo_hint: "meta-llama/Llama-3.2-1B-Instruct"
             }
           ]
  end

  test "download_model raises when token is required but missing" do
    session =
      HpcConnect.new_session(:alex,
        username: "hpcusr01",
        ssh_alias: "alex",
        identity_file: "~/.ssh/id_hpc",
        vault_dir: "/vault/hpc_connect"
      )

    model = HpcConnect.new_model("meta-llama/Llama-3.2-1B-Instruct")

    assert_raise ArgumentError, ~r/missing required HUGGINGFACE_HUB_TOKEN/, fn ->
      HpcConnect.download_model(session, model)
    end
  end

  test "derives absolute work_dir and vault_dir from username hpcusr12" do
    session =
      HpcConnect.new_session(:alex,
        username: "hpcusr12",
        ssh_alias: "alex"
      )

    assert session.work_dir == "/home/hpc/hpcusr/hpcusr12/.cache/hpc_connect"
    assert session.vault_dir == "/home/vault/hpcusr/hpcusr12"
    refute String.starts_with?(session.work_dir, "$HOME")
    refute String.starts_with?(session.vault_dir, "$HOME")
  end

  test "derives group barz for username barz123h" do
    session =
      HpcConnect.new_session(:alex,
        username: "barz123h",
        ssh_alias: "alex"
      )

    assert session.work_dir == "/home/hpc/barz/barz123h/.cache/hpc_connect"
    assert session.vault_dir == "/home/vault/barz/barz123h"
  end

  test "start_app defers vllm access setup until after submission and falls back to managed proxy" do
    parent = self()

    session =
      HpcConnect.new_session(:alex,
        username: "hpcusr01",
        ssh_alias: "alex",
        identity_file: "/tmp/id_hpc_test",
        # Simulate a cluster that is directly TCP-reachable (no ProxyJump required).
        # For clusters with proxy_jump set, native SSH is skipped entirely because
        # Erlang :ssh cannot replicate the OS SSH transparent-proxy topology.
        proxy_jump: nil
      )

    submit_fun = fn _session, _slurm_opts ->
      send(parent, :submitted)

      %{
        job_id: "12345",
        node: "a0128",
        partition: "a40",
        gpus: 1,
        walltime: "02:00:00",
        port: 50_200,
        sif_path: "/remote/vllm.sif",
        logs_dir: "/remote/logs"
      }
    end

    open_connection_fun = fn _session, _opts ->
      send(parent, :native_connect_attempted)
      raise RuntimeError, "native bridge timeout"
    end

    open_proxy_fun = fn _session, node, proxy_opts ->
      send(parent, {:proxy_opened, node, proxy_opts[:remote_port], proxy_opts[:local_port]})

      {
        %{
          base_url: "http://localhost:50500",
          local_port: 50_500,
          remote_port: proxy_opts[:remote_port],
          node: node
        },
        make_ref()
      }
    end

    result =
      HpcConnect.start_app(session,
        app: "vllm",
        native_ssh: true,
        args: [partition: "a40", gpus: 1, walltime: "02:00:00", port: 50_200],
        local_port: 50_500,
        submit_fun: submit_fun,
        open_connection_fun: open_connection_fun,
        open_proxy_fun: open_proxy_fun,
        wait_for_node: false,
        wait_for_app: false
      )

    assert_receive :submitted
    assert_receive :native_connect_attempted
    assert_receive {:proxy_opened, "a0128", 50_200, 50_500}

    assert result.node == "a0128"
    assert result.access_mode == :openssh_proxy
    assert result.base_url == "http://localhost:50500"
    assert result.native_error =~ "native bridge timeout"
  end

  test "start_app uses managed OS proxy by default when native_ssh is not enabled" do
    parent = self()

    session =
      HpcConnect.new_session(:alex,
        username: "hpcusr01",
        ssh_alias: "alex",
        identity_file: "/tmp/id_hpc_test"
      )
      |> Map.put(:ssh_conn, make_ref())

    submit_fun = fn _session, _slurm_opts ->
      %{
        job_id: "12346",
        node: "a0999",
        partition: "a40",
        gpus: 1,
        walltime: "02:00:00",
        port: 50_200,
        sif_path: "/remote/vllm.sif",
        logs_dir: "/remote/logs"
      }
    end

    open_connection_fun = fn _session, _opts ->
      send(parent, :native_connect_attempted)
      raise RuntimeError, "should-not-run"
    end

    open_proxy_fun = fn _session, node, proxy_opts ->
      send(parent, {:proxy_opened_default, node, proxy_opts[:remote_port]})

      {
        %{
          base_url: "http://localhost:50501",
          local_port: 50_501,
          remote_port: proxy_opts[:remote_port],
          node: node,
          os_pid: 42,
          pid: 42
        },
        make_ref()
      }
    end

    open_native_tunnel_fun = fn _session,
                                _native_conn,
                                _node,
                                _remote_port,
                                _local_port,
                                _timeout ->
      send(parent, :native_tunnel_attempted)
      {make_ref(), 50_599}
    end

    result =
      HpcConnect.start_app(session,
        app: "vllm",
        args: [partition: "a40", gpus: 1, walltime: "02:00:00", port: 50_200],
        submit_fun: submit_fun,
        open_connection_fun: open_connection_fun,
        open_native_tunnel_fun: open_native_tunnel_fun,
        open_proxy_fun: open_proxy_fun,
        wait_for_node: false,
        wait_for_app: false
      )

    refute_receive :native_connect_attempted
    refute_receive :native_tunnel_attempted
    assert_receive {:proxy_opened_default, "a0999", 50_200}

    assert result.access_mode == :openssh_proxy
    assert result.base_url == "http://localhost:50501"
    assert result.proxy.os_pid == 42
  end

  test "start_app attempts hybrid native ProxyJump first, then falls back to managed proxy" do
    parent = self()

    session =
      HpcConnect.new_session(:alex,
        username: "hpcusr01",
        ssh_alias: "alex",
        identity_file: "/tmp/id_hpc_test"
      )

    submit_fun = fn _session, _slurm_opts ->
      %{
        job_id: "12347",
        node: "a0902",
        partition: "a100",
        gpus: 1,
        walltime: "02:00:00",
        port: 50_200,
        sif_path: "/remote/vllm.sif",
        logs_dir: "/remote/logs"
      }
    end

    open_connection_fun = fn _session, open_opts ->
      send(
        parent,
        {:native_connect_attempted, open_opts[:proxy_jump_via_native],
         open_opts[:proxy_jump_via_os]}
      )

      raise RuntimeError, "native bridge timeout"
    end

    open_proxy_fun = fn _session, node, proxy_opts ->
      send(parent, {:proxy_opened_proxy_jump, node, proxy_opts[:remote_port]})

      {
        %{
          base_url: "http://localhost:50502",
          local_port: 50_502,
          remote_port: proxy_opts[:remote_port],
          node: node
        },
        make_ref()
      }
    end

    result =
      HpcConnect.start_app(session,
        app: "vllm",
        native_ssh: true,
        args: [partition: "a100", gpus: 1, walltime: "02:00:00", port: 50_200],
        submit_fun: submit_fun,
        open_connection_fun: open_connection_fun,
        open_proxy_fun: open_proxy_fun,
        wait_for_node: false,
        wait_for_app: false
      )

    assert_receive {:native_connect_attempted, true, true}
    assert_receive {:proxy_opened_proxy_jump, "a0902", 50_200}

    assert result.access_mode == :openssh_proxy
    assert result.base_url == "http://localhost:50502"
    assert result.native_error =~ "native bridge timeout"
  end

  test "kill_proxy closes tracked proxy by session and node" do
    session =
      HpcConnect.new_session(:alex,
        username: "hpcusr01",
        ssh_alias: "alex",
        identity_file: "/tmp/id_hpc_test"
      )

    {bin, args} =
      case :os.type() do
        {:win32, _} ->
          {System.find_executable("cmd") || "cmd", ["/c", "ping", "-n", "20", "127.0.0.1"]}

        _ ->
          {System.find_executable("sh") || "sh", ["-c", "sleep 20"]}
      end

    port =
      HpcConnect.open_proxy!(%{
        session: session,
        node: "a0128",
        command: %HpcConnect.Command{
          binary: bin,
          args: args,
          summary: "test proxy",
          remote_command: nil
        }
      })

    assert Port.info(port) != nil

    info = HpcConnect.proxy_info(session, "a0128")
    assert is_list(info)
    assert length(info) >= 1

    assert :ok == HpcConnect.kill_proxy(session, "a0128")
    assert Port.info(port) == nil
    assert HpcConnect.proxy_info(session, "a0128") == []
  end
end
