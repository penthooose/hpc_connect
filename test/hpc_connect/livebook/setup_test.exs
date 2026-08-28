defmodule HpcConnect.Livebook.SetupTest do
  use ExUnit.Case, async: false

  alias HpcConnect.Livebook.Setup

  describe "inputs/0" do
    test "covers the key env vars HpcConnect reads" do
      env_keys = Setup.inputs() |> Enum.map(& &1.env)

      assert "HPC_CONNECT_CLUSTER" in env_keys
      assert "HPC_CONNECT_USERNAME" in env_keys
      assert "HPC_CONNECT_IDENTITY_FILE" in env_keys
      assert "HPC_CONNECT_WORK_DIR" in env_keys
      assert "HPC_CONNECT_STEADY_CONNECTION" in env_keys
      assert "HPC_CONNECT_RETRY_FOREVER" in env_keys
      assert "HPC_CONNECT_REMOTE_COMMAND" in env_keys
    end

    test "marks HUGGINGFACE_HUB_TOKEN as a secret (never persisted)" do
      hf = Enum.find(Setup.inputs(), &(&1.env == "HUGGINGFACE_HUB_TOKEN"))
      assert hf.type == :secret
    end
  end

  describe "resolve_defaults/4" do
    test "persisted value wins over env file value" do
      persisted = %{"HPC_CONNECT_CLUSTER" => "alex"}
      env_map = %{"HPC_CONNECT_CLUSTER" => "fritz"}
      defaults = Setup.resolve_defaults(Setup.inputs(), persisted, env_map, "/tmp")

      assert defaults["HPC_CONNECT_CLUSTER"] == "alex"
    end

    test "env file value fills when persisted is blank" do
      defaults =
        Setup.resolve_defaults(Setup.inputs(), %{}, %{"HPC_CONNECT_USERNAME" => "hpcusr01"}, "/tmp")

      assert defaults["HPC_CONNECT_USERNAME"] == "hpcusr01"
    end

    test "non-path fields fall back to their static default" do
      defaults = Setup.resolve_defaults(Setup.inputs(), %{}, %{}, "/tmp")

      assert defaults["HPC_CONNECT_CLUSTER"] == "fritz"
      assert defaults["HPC_CONNECT_WORK_DIR"] == "$HOME/.cache/hpc_connect"
      assert defaults["HPC_CONNECT_RETRY_FOREVER"] == "true"
      assert defaults["HPC_CONNECT_REMOTE_COMMAND"] == "hostname && whoami"
    end
  end

  describe "non_blank/1 and render_env_content/1" do
    test "select options are converted to {value, label} tuples (Kino ≥ 0.19)" do
      assert Setup.select_options(["true", "false"]) ==
               [{"true", "true"}, {"false", "false"}]
    end

    test "filters nil and blank values" do
      assert Setup.non_blank(%{"A" => "x", "B" => "", "C" => nil, "D" => "  "}) == %{
               "A" => "x"
             }
    end

    test "renders sorted KEY=VALUE lines" do
      content =
        Setup.render_env_content(%{
          "HPC_CONNECT_CLUSTER" => "alex",
          "HPC_CONNECT_USERNAME" => "hpcusr01"
        })

      assert content == "HPC_CONNECT_CLUSTER=alex\nHPC_CONNECT_USERNAME=hpcusr01"
    end

    test "write_env_file returns a normalized (Path.expand'd) path" do
      tmp =
        Path.join(System.tmp_dir!(), "hpc_livebook_setup_test_#{System.unique_integer([:positive])}")

      File.mkdir_p!(tmp)
      on_exit(fn -> File.rm_rf!(tmp) end)

      raw = Path.join(tmp, "x.env")
      path = Setup.write_env_file(%{"A" => "1"}, env_output_path: raw)

      assert path == Path.expand(raw)
      assert File.exists?(path)
      assert File.read!(path) =~ "A=1"
    end
  end

  describe "prepare/1 in :local mode" do
    setup do
      tmp =
        Path.join(System.tmp_dir!(), "hpc_livebook_setup_test_#{System.unique_integer([:positive])}")

      env_file = Path.join(tmp, "input.env")
      File.mkdir_p!(tmp)
      File.write!(env_file, "HPC_CONNECT_CLUSTER=alex\nHPC_CONNECT_USERNAME=hpcusr01\n")

      env_keys = Setup.inputs() |> Enum.map(& &1.env)
      before_env = Map.new(env_keys, &{&1, System.get_env(&1)})

      on_exit(fn ->
        Enum.each(before_env, fn {k, v} ->
          if v, do: System.put_env(k, v), else: System.delete_env(k)
        end)

        File.rm_rf!(tmp)
      end)

      %{tmp: tmp, env_file: env_file}
    end

    test "applies env and writes a temp env file for bootstrap", %{tmp: tmp, env_file: env_file} do
      result =
        Setup.prepare(
          mode: :local,
          env_file: env_file,
          persist_path: Path.join(tmp, "setup.json"),
          env_output_path: Path.join(tmp, "out.env"),
          tmp_base: Path.join(tmp, "sess")
        )

      assert result.env_map["HPC_CONNECT_CLUSTER"] == "alex"
      assert result.env_map["HPC_CONNECT_USERNAME"] == "hpcusr01"
      assert result.persisted_path == Path.join(tmp, "setup.json")

      assert File.exists?(result.env_file)
      assert File.read!(result.env_file) =~ "HPC_CONNECT_CLUSTER=alex"
      assert File.read!(result.env_file) =~ "HPC_CONNECT_USERNAME=hpcusr01"

      assert System.get_env("HPC_CONNECT_CLUSTER") == "alex"
    end

    test "exposes the effective SSH key in the result map", %{tmp: tmp} do
      result =
        Setup.prepare(
          mode: :local,
          env_file: nil,
          persist_path: Path.join(tmp, "setup.json"),
          env_output_path: Path.join(tmp, "out.env"),
          tmp_base: Path.join(tmp, "sess")
        )

      assert Map.has_key?(result, :ssh_key_path)
      assert Map.has_key?(result, :ssh_key_temporary?)
      # No upload in :local mode, so the key is never a temp staged copy.
      assert result.ssh_key_temporary? == false
    end

    test "keeps a persisted SSH identity (auto-detect never clobbers it)", %{tmp: tmp} do
      persist_path = Path.join(tmp, "setup.json")
      File.write!(persist_path, Jason.encode!(%{"HPC_CONNECT_IDENTITY_FILE" => "~/.ssh/id_test"}))

      result =
        Setup.prepare(
          mode: :local,
          env_file: nil,
          persist_path: persist_path,
          env_output_path: Path.join(tmp, "out.env"),
          tmp_base: Path.join(tmp, "sess")
        )

      assert result.values["HPC_CONNECT_IDENTITY_FILE"] == "~/.ssh/id_test"
    end
  end

  describe "cleanup/1" do
    test "prints a friendly notice when no temp key is recorded" do
      tmp =
        Path.join(System.tmp_dir!(), "hpc_livebook_setup_test_#{System.unique_integer([:positive])}")

      File.mkdir_p!(tmp)
      on_exit(fn -> File.rm_rf!(tmp) end)

      output =
        ExUnit.CaptureIO.capture_io(fn ->
          assert Setup.cleanup(registry_path: Path.join(tmp, "registry.json")) == :ok
        end)

      assert output =~ "No temporary SSH key found"
    end
  end
end
