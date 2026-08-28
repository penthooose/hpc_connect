defmodule HpcConnect.Livebook.SshKey do
  @moduledoc """
  SSH key resolution + temp-key cleanup shared by the Livebook setup overlays
  (hpc_connect and atp_benchmark_runner).

  Resolution order for the effective SSH identity file:

    1. a **configured** path (`:identity_file` opt or `HPC_CONNECT_IDENTITY_FILE`)
       that already exists on disk -> `{:configured, path}` (treated as persistent,
       cleanup never touches it);
    2. an **uploaded** key (Kino file ref or path) -> `{:uploaded, path}`;
    3. otherwise -> `:missing` (callers show the interactive upload panel).

  Uploaded keys are **staged** into a stable temp location (with safe 600
  permissions on Unix) and both the staged copy and the original upload are
  recorded in a small registry file under the temp dir, so a notebook-end
  cleanup (`cleanup/0`) can remove exactly those temp artifacts and never a
  persistent `~/.ssh` key. `cleanup/0` never raises and prints a short notice
  even when nothing is left to delete.
  """

  @registry_file "livebook_ssh_keys.json"
  @preferred_keys ~w(id_ed25519 id_rsa id_ecdsa)

  @type resolution :: {:configured, binary()} | {:uploaded, binary()} | :missing

  @doc """
  Resolves the effective SSH identity file. An **uploaded** key wins when
  provided (the explicit user choice); otherwise a **configured** path
  (`:identity_file` opt or `HPC_CONNECT_IDENTITY_FILE`) that already exists on
  disk is used (treated as persistent, cleanup never touches it); otherwise
  `:missing` (callers show an error / keep the setup cell waiting).
  """
  @spec resolve(keyword(), term()) :: resolution()
  def resolve(opts, upload) do
    case resolve_upload(upload) do
      {:uploaded, path} ->
        {:uploaded, path}

      :missing ->
        case configured_path(opts) do
          nil -> :missing
          path -> {:configured, path}
        end
    end
  end

  @doc """
  Returns the configured identity path if it is set and exists on disk,
  otherwise `nil`. The candidate is canonicalized first (`normalize_path/1`:
  `~` expanded, forward-slash separators), then checked on disk, so detection
  never sees mixed `\\`/`/` separators.
  """
  @spec configured_path(keyword()) :: binary() | nil
  def configured_path(opts) do
    candidate =
      Keyword.get(opts, :identity_file) ||
        System.get_env("HPC_CONNECT_IDENTITY_FILE") ||
        ""

    case normalize_path(candidate) do
      nil -> nil
      path -> if File.exists?(path), do: path, else: nil
    end
  end

  @doc """
  Canonicalizes a path for cross-platform handling: trims whitespace, expands
  `~`, and returns an absolute path with forward-slash separators (valid on
  both Windows and Linux; `Path.expand` normalizes `\` to `/` on Windows).

  Returns `nil` for blank input. Run this **before** existence checks so
  detection always sees the unified form of the path.
  """
  @spec normalize_path(binary() | nil) :: binary() | nil
  def normalize_path(nil), do: nil

  def normalize_path(path) when is_binary(path) do
    trimmed = String.trim(path)

    cond do
      trimmed == "" ->
        nil

      # `Path.expand` cannot expand `~` without a resolvable home; keep the
      # literal form rather than a drive-relative artifact.
      String.starts_with?(trimmed, "~") and is_nil(System.user_home()) ->
        trimmed

      true ->
        Path.expand(trimmed)
    end
  end

  @doc """
  Auto-detects a private key in the user's `.ssh` directory (cross-platform:
  `~/.ssh` on Unix, `C:\\Users\\<user>\\.ssh` on Windows) so setup overlays can
  pre-fill the SSH identity field with an existing key.

  Prefers `id_ed25519`, then `id_rsa`, then `id_ecdsa`; falls back to the first
  `id_*` file (ignoring `*.pub`) when none of those exist. Returns `nil` when
  `.ssh` has no private key; callers then show the upload panel. Pass `home`
  to override the home directory (mainly for tests).
  """
  @spec default_identity_path(binary() | nil) :: binary() | nil
  def default_identity_path(home \\ System.user_home()) do
    ssh_dir = Path.join(normalize_path(home) || "", ".ssh")

    if File.dir?(ssh_dir) do
      case detect_key(ssh_dir) do
        nil -> nil
        path -> Path.expand(path)
      end
    end
  end

  @doc """
  Resolves an uploaded key value to its path, or `:missing` when absent.
  """
  @spec resolve_upload(term()) :: {:uploaded, binary()} | :missing
  def resolve_upload(nil), do: :missing
  def resolve_upload(""), do: :missing
  def resolve_upload(%{file_ref: file_ref}), do: {:uploaded, kino_file_path(file_ref)}
  def resolve_upload(%{__struct__: _} = ref), do: {:uploaded, kino_file_path(ref)}
  def resolve_upload(path) when is_binary(path), do: {:uploaded, path}

  @doc """
  Copies an uploaded key into a stable temp location (safe 600 perms on Unix),
  records both the staged copy and the original upload for cleanup, and returns
  the staged path to use as the identity file.
  """
  @spec stage_upload(binary(), keyword()) :: binary()
  def stage_upload(uploaded_path, opts \\ []) do
    stage_dir =
      opts
      |> Keyword.get(:stage_dir, default_stage_dir())
      |> normalize_path()

    File.mkdir_p!(stage_dir)
    staged = Path.join(stage_dir, "id_livebook_#{System.unique_integer([:positive])}")
    File.cp!(uploaded_path, staged)
    maybe_restrict_permissions(staged)

    record(%{"kind" => "staged", "path" => staged}, opts)
    record(%{"kind" => "upload", "path" => Path.expand(uploaded_path)}, opts)
    staged
  end

  @doc """
  Deletes recorded temp SSH keys (staged copies and original uploads) that still
  exist, clears the registry, and returns the list of deleted paths.
  Never raises and never touches a persistent configured key.
  """
  @spec cleanup(keyword()) :: [binary()]
  def cleanup(opts \\ []) do
    entries = read_registry(opts)

    deleted =
      entries
      |> Enum.map(fn entry -> Map.get(entry, "path") end)
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()
      |> Enum.filter(&File.exists?/1)
      |> Enum.filter(&temp_path?/1)
      |> Enum.map(fn path ->
        File.rm(path)
        path
      end)

    write_registry([], opts)
    deleted
  end

  @doc """
  Runs `cleanup/1` and prints a short human notice. Prints a "nothing to
  delete / already gone" line when no temp key remains, without raising.
  """
  @spec cleanup_notice(keyword()) :: :ok
  def cleanup_notice(opts \\ []) do
    case cleanup(opts) do
      [] ->
        IO.puts("No temporary SSH key found; nothing to delete (key already gone).")

      paths ->
        IO.puts("Deleted temporary SSH key(s):")

        Enum.each(paths, fn path ->
          IO.puts("  - #{path}")
        end)
    end

    :ok
  end

  # helpers

  defp detect_key(ssh_dir) do
    preferred =
      Enum.find(@preferred_keys, fn name ->
        File.regular?(Path.join(ssh_dir, name))
      end)

    if preferred do
      Path.join(ssh_dir, preferred)
    else
      fallback_detect(ssh_dir)
    end
  end

  defp fallback_detect(ssh_dir) do
    case File.ls(ssh_dir) do
      {:ok, files} ->
        files
        |> Enum.filter(fn name ->
          String.starts_with?(name, "id_") and not String.ends_with?(name, ".pub")
        end)
        |> Enum.sort()
        |> List.first()
        |> case do
          nil -> nil
          name -> Path.join(ssh_dir, name)
        end

      _ ->
        nil
    end
  end

  @doc false
  # Canonicalizes a path via `normalize_path/1`, falling back to the original
  # string when it is blank. Thin alias for callers that expect a string back.
  @spec expand_path(binary()) :: binary()
  def expand_path(path), do: normalize_path(path) || path

  @doc false
  @spec temp_path?(binary()) :: boolean()
  def temp_path?(path) do
    expanded = Path.expand(path)
    tmp = Path.expand(System.tmp_dir!())

    String.starts_with?(expanded, tmp)
  end

  defp maybe_restrict_permissions(path) do
    case :os.type() do
      {:win32, _} -> :ok
      _ -> File.chmod(path, 0o600)
    end
  end

  defp kino_file_path(file_ref) do
    apply(Module.concat([Kino, Input]), :file_path, [file_ref])
  end

  defp record(entry, opts) do
    entries = read_registry(opts) ++ [entry]
    write_registry(entries, opts)
  end

  # Canonical (forward-slash) temp dir for staged uploaded keys.
  defp default_stage_dir do
    normalize_path(Path.join([System.tmp_dir!(), "hpc_connect", "livebook_keys"]))
  end

  defp registry_path(opts) do
    Keyword.get(
      opts,
      :registry_path,
      normalize_path(Path.join([System.tmp_dir!(), "hpc_connect", @registry_file]))
    )
  end

  defp read_registry(opts) do
    with {:ok, json} <- File.read(registry_path(opts)),
         {:ok, entries} <- Jason.decode(json),
         true <- is_list(entries) do
      entries
    else
      _ -> []
    end
  end

  defp write_registry(entries, opts) do
    path = registry_path(opts)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, Jason.encode!(entries, pretty: true))
    :ok
  end
end
