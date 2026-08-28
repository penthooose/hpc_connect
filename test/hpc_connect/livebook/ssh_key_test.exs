defmodule HpcConnect.Livebook.SshKeyTest do
  use ExUnit.Case, async: false

  alias HpcConnect.Livebook.SshKey

  setup do
    tmp = Path.join(System.tmp_dir!(), "ssh_key_test_#{System.unique_integer([:positive])}")
    File.mkdir_p!(tmp)
    on_exit(fn -> File.rm_rf!(tmp) end)
    %{tmp: tmp}
  end

  defp write_key(tmp, name, content \\ "PRIVATE KEY MATERIAL\n") do
    path = Path.join(tmp, name)
    File.write!(path, content)
    path
  end

  defp registry_path(tmp), do: Path.join(tmp, "registry.json")

  describe "resolve/2" do
    test "uses a configured path that exists", %{tmp: tmp} do
      key = write_key(tmp, "id_a") |> Path.expand()
      assert SshKey.resolve([identity_file: key], nil) == {:configured, key}
    end

    test "an upload wins over a configured path", %{tmp: tmp} do
      key = write_key(tmp, "id_a") |> Path.expand()
      assert SshKey.resolve([identity_file: key], "/some/upload") == {:uploaded, "/some/upload"}
    end

    test "falls back to the upload when the configured path is missing or blank", %{tmp: tmp} do
      missing = Path.join(tmp, "missing_key")
      assert SshKey.resolve([identity_file: missing], "/upload/key") == {:uploaded, "/upload/key"}
      assert SshKey.resolve([], "/upload/key") == {:uploaded, "/upload/key"}
    end

    test "returns :missing when nothing is configured and no upload" do
      assert SshKey.resolve([], nil) == :missing
      assert SshKey.resolve([identity_file: "/does/not/exist"], nil) == :missing
    end
  end

  describe "configured_path/1" do
    test "returns the existing path", %{tmp: tmp} do
      key = write_key(tmp, "id") |> Path.expand()
      assert SshKey.configured_path(identity_file: key) == key
    end

    test "unifies separators before checking (never returns a mixed path)", %{tmp: tmp} do
      key = write_key(tmp, "id_norm")
      resolved = SshKey.configured_path(identity_file: key)

      assert resolved != nil
      refute resolved =~ "\\"
      assert Path.basename(resolved) == "id_norm"
    end

    test "returns nil for blank or missing paths", %{tmp: tmp} do
      assert SshKey.configured_path(identity_file: "") == nil
      assert SshKey.configured_path(identity_file: Path.join(tmp, "missing")) == nil
      assert SshKey.configured_path([]) == nil
    end
  end

  describe "normalize_path/1" do
    test "returns nil for blank input" do
      assert SshKey.normalize_path(nil) == nil
      assert SshKey.normalize_path("") == nil
      assert SshKey.normalize_path("   ") == nil
    end

    test "expands ~ and unifies separators to forward slashes" do
      expanded = SshKey.normalize_path("~/.ssh/some_key")
      assert expanded == Path.expand("~/.ssh/some_key")
      refute expanded =~ "\\"
      assert expanded =~ ".ssh/some_key"
    end

    test "unifies backslash paths to forward slashes (Windows)", %{tmp: tmp} do
      if match?({:win32, _}, :os.type()) do
        expanded = SshKey.normalize_path(Path.join(tmp, "a\\b\\key"))
        refute expanded =~ "\\"
        assert expanded =~ "a/b/key"
      else
        :ok
      end
    end
  end

  describe "expand_path/1 (tilde normalization)" do
    test "expands ~ to a platform-canonical path without mixed separators" do
      expanded = SshKey.expand_path("~/.ssh/some_key")

      assert expanded == Path.expand("~/.ssh/some_key")
      refute expanded =~ "\\/"
      refute expanded =~ "/\\"
      assert expanded =~ ".ssh/some_key"
    end

    test "leaves a literal ~ when no home can be resolved" do
      # Can't force System.user_home/0 to return nil portably; the fallback is
      # only asserted for the tilde-only edge shape.
      assert SshKey.expand_path("/abs/path/key") == Path.expand("/abs/path/key")
    end
  end

  describe "default_identity_path/1" do
    test "returns nil when the home has no .ssh directory", %{tmp: tmp} do
      assert SshKey.default_identity_path(tmp) == nil
    end

    test "returns nil when .ssh has no private key", %{tmp: tmp} do
      ssh_dir = Path.join(tmp, ".ssh")
      File.mkdir_p!(ssh_dir)
      File.touch!(Path.join(ssh_dir, "known_hosts"))
      File.touch!(Path.join(ssh_dir, "authorized_keys"))

      assert SshKey.default_identity_path(tmp) == nil
    end

    test "prefers id_ed25519 over id_rsa over id_ecdsa", %{tmp: tmp} do
      ssh_dir = Path.join(tmp, ".ssh")
      File.mkdir_p!(ssh_dir)
      write_key(ssh_dir, "id_rsa")
      write_key(ssh_dir, "id_ed25519")

      assert SshKey.default_identity_path(tmp) == Path.expand(Path.join(ssh_dir, "id_ed25519"))
    end

    test "falls back to the first id_* file when preferred names are absent", %{tmp: tmp} do
      ssh_dir = Path.join(tmp, ".ssh")
      File.mkdir_p!(ssh_dir)
      write_key(ssh_dir, "id_custom")
      write_key(ssh_dir, "id_custom.pub")

      assert SshKey.default_identity_path(tmp) == Path.expand(Path.join(ssh_dir, "id_custom"))
    end
  end

  describe "stage_upload/2 + cleanup/0" do
    test "stages an upload; cleanup removes temp keys but never a persistent key", %{tmp: tmp} do
      upload = write_key(tmp, "uploaded_key") |> Path.expand()
      persistent = write_key(tmp, "persistent_key")

      staged =
        SshKey.stage_upload(upload,
          stage_dir: Path.join(tmp, "keys"),
          registry_path: registry_path(tmp)
        )

      assert File.exists?(staged)
      assert staged != upload

      deleted = SshKey.cleanup(registry_path: registry_path(tmp))

      assert staged in deleted
      assert upload in deleted
      assert File.exists?(persistent)
      refute File.exists?(upload)
      refute File.exists?(staged)
    end

    test "cleanup with no registry is a no-op returning []", %{tmp: tmp} do
      assert SshKey.cleanup(registry_path: registry_path(tmp)) == []
    end

    test "staged path is forward-slash normalized (never mixed separators)", %{tmp: tmp} do
      upload = write_key(tmp, "uploaded_key") |> Path.expand()

      staged =
        SshKey.stage_upload(upload,
          stage_dir: Path.join(tmp, "keys"),
          registry_path: registry_path(tmp)
        )

      refute staged =~ "\\"
      assert Path.basename(staged) =~ "id_livebook_"
      assert File.exists?(staged)
      SshKey.cleanup(registry_path: registry_path(tmp))
    end

    test "default stage dir (no option) is forward-slash normalized", %{tmp: tmp} do
      upload = write_key(tmp, "uploaded_key") |> Path.expand()
      staged = SshKey.stage_upload(upload, registry_path: registry_path(tmp))

      refute staged =~ "\\"
      assert staged =~ "livebook_keys/id_livebook_"
      assert File.exists?(staged)
      SshKey.cleanup(registry_path: registry_path(tmp))
    end

    test "cleanup_notice prints a friendly message when nothing to delete", %{tmp: tmp} do
      output =
        ExUnit.CaptureIO.capture_io(fn ->
          assert SshKey.cleanup_notice(registry_path: registry_path(tmp)) == :ok
        end)

      assert output =~ "No temporary SSH key found"
    end
  end
end
