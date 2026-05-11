defmodule HpcConnect.Credentials do
  @moduledoc """
  Creates and cleans up temporary SSH credential material for shared-runtime use
  cases such as Livebook.
  """

  alias HpcConnect.Cluster

  @type t :: %{
          credential_dir: binary(),
          identity_file: binary(),
          ssh_config_file: binary(),
          known_hosts_file: binary(),
          cleanup_registry_file: binary()
        }

  @spec create(binary(), binary(), keyword()) :: t()
  def create(username, uploaded_key_path, opts \\ []) do
    clusters = Keyword.get(opts, :clusters, Cluster.defaults())
    prefix = Keyword.get(opts, :prefix, "livebook")

    credential_dir =
      System.tmp_dir!()
      |> Path.join("hpc_connect")
      |> Path.join(
        "#{prefix}_#{System.system_time(:millisecond)}_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(credential_dir)

    key_basename = Path.basename(uploaded_key_path)
    identity_file = Path.join(credential_dir, key_basename)
    known_hosts_file = Path.join(credential_dir, "known_hosts")
    ssh_config_file = Path.join(credential_dir, "config")

    File.cp!(uploaded_key_path, identity_file)
    File.write!(known_hosts_file, "")

    File.write!(
      ssh_config_file,
      render_config(clusters, username, identity_file, known_hosts_file)
    )

    maybe_restrict_permissions(identity_file)
    maybe_restrict_permissions(known_hosts_file)
    maybe_restrict_permissions(ssh_config_file)

    cleanup_registry_file =
      register_cleanup_paths(credential_dir, [
        %{type: :credential_dir, path: credential_dir},
        %{type: :uploaded, path: uploaded_key_path}
      ])

    %{
      credential_dir: credential_dir,
      identity_file: identity_file,
      ssh_config_file: ssh_config_file,
      known_hosts_file: known_hosts_file,
      cleanup_registry_file: cleanup_registry_file
    }
  end

  @spec cleanup(binary() | map() | nil) :: :ok
  def cleanup(nil), do: :ok
  def cleanup(%{credential_dir: credential_dir}), do: cleanup(credential_dir)

  def cleanup(credential_dir) when is_binary(credential_dir) do
    _ = File.rm_rf(credential_dir)
    :ok
  end

  @doc """
  Cleans artifacts registered for a specific credential directory.

  This supports recovery from interrupted sessions, because paths are tracked on
  disk under a cleanup registry.
  """
  @spec cleanup_registered(binary() | nil, keyword()) :: :ok
  def cleanup_registered(credential_dir, opts \\ [])
  def cleanup_registered(nil, _opts), do: :ok

  def cleanup_registered(credential_dir, opts) when is_binary(credential_dir) do
    delete_uploaded? = Keyword.get(opts, :delete_uploaded, false)
    force_uploaded_delete? = Keyword.get(opts, :force_uploaded_delete, false)
    registry_file = registry_file_for(credential_dir)

    entries = read_registry_entries(registry_file)
    remaining_entries = apply_cleanup_entries(entries, delete_uploaded?, force_uploaded_delete?)
    persist_registry_entries(registry_file, remaining_entries)
    :ok
  end

  @doc """
  Sweeps all known cleanup registry entries.

  Use this to remove orphaned temp files from interrupted/paused sessions.
  """
  @spec sweep_cleanup_registry(keyword()) :: :ok
  def sweep_cleanup_registry(opts \\ []) do
    delete_uploaded? = Keyword.get(opts, :delete_uploaded, false)
    force_uploaded_delete? = Keyword.get(opts, :force_uploaded_delete, false)

    registry_dir()
    |> File.ls()
    |> case do
      {:ok, files} ->
        Enum.each(files, fn filename ->
          registry_file = Path.join(registry_dir(), filename)
          entries = read_registry_entries(registry_file)

          remaining_entries =
            apply_cleanup_entries(entries, delete_uploaded?, force_uploaded_delete?)

          persist_registry_entries(registry_file, remaining_entries)
        end)

        :ok

      {:error, _reason} ->
        :ok
    end
  end

  @doc """
  Removes the original Livebook uploaded file when the path looks like a
  Livebook registered file under the system temp directory.

  Use `force: true` only when you are certain the path is safe to remove.
  """
  @spec cleanup_uploaded_file(binary() | nil, keyword()) :: :ok
  def cleanup_uploaded_file(path, opts \\ [])
  def cleanup_uploaded_file(nil, _opts), do: :ok

  def cleanup_uploaded_file(path, opts) when is_binary(path) do
    force? = Keyword.get(opts, :force, false)

    if force? or livebook_upload_path?(path) do
      _ = File.rm(path)
    end

    :ok
  end

  @spec livebook_upload_path?(binary() | nil) :: boolean()
  def livebook_upload_path?(nil), do: false

  def livebook_upload_path?(path) when is_binary(path) do
    expanded = path |> Path.expand() |> String.downcase()
    tmp_root = System.tmp_dir!() |> Path.expand() |> String.downcase()

    String.starts_with?(expanded, tmp_root) and
      String.contains?(expanded, "livebook") and
      String.contains?(expanded, "registered_files")
  end

  defp register_cleanup_paths(credential_dir, entries) do
    _ = File.mkdir_p(registry_dir())
    registry_file = registry_file_for(credential_dir)

    payload = %{
      created_at_unix: System.system_time(:second),
      entries: entries
    }

    _ = File.write(registry_file, :erlang.term_to_binary(payload))
    registry_file
  end

  defp read_registry_entries(registry_file) do
    with {:ok, binary} <- File.read(registry_file),
         {:ok, payload} <- safe_binary_to_term(binary),
         entries when is_list(entries) <- Map.get(payload, :entries) do
      entries
    else
      _ -> []
    end
  end

  defp safe_binary_to_term(binary) do
    {:ok, :erlang.binary_to_term(binary, [:safe])}
  rescue
    _ -> {:error, :invalid_term}
  end

  defp registry_dir do
    Path.join([System.tmp_dir!(), "hpc_connect", "cleanup_registry"])
  end

  defp registry_file_for(credential_dir) do
    basename = Path.basename(credential_dir)
    Path.join(registry_dir(), "#{basename}.term")
  end

  defp apply_cleanup_entries(entries, delete_uploaded?, force_uploaded_delete?) do
    Enum.reduce(entries, [], fn
      %{type: :credential_dir, path: path}, acc ->
        _ = File.rm_rf(path)
        acc

      %{type: :uploaded, path: path} = entry, acc ->
        cond do
          not delete_uploaded? ->
            [entry | acc]

          true ->
            _ = cleanup_uploaded_file(path, force: force_uploaded_delete?)

            if File.exists?(path) do
              [entry | acc]
            else
              acc
            end
        end

      entry, acc ->
        [entry | acc]
    end)
    |> Enum.reverse()
  end

  defp persist_registry_entries(registry_file, []), do: File.rm(registry_file)

  defp persist_registry_entries(registry_file, entries) do
    payload = %{
      created_at_unix: System.system_time(:second),
      entries: entries
    }

    File.write(registry_file, :erlang.term_to_binary(payload))
  end

  @spec render_config([Cluster.t()], binary(), binary(), binary()) :: binary()
  def render_config(clusters, username, identity_file, known_hosts_file) do
    Enum.map_join(clusters, "\n\n", fn cluster ->
      aliases =
        Enum.uniq(
          (cluster.aliases ++ [cluster.host, cluster.ssh_alias])
          |> Enum.reject(&is_nil/1)
        )

      [
        "Host #{Enum.join(aliases, " ")}",
        "  HostName #{cluster.host}",
        "  User #{username}",
        "  IdentityFile #{render_local_config_path(identity_file)}",
        "  IdentitiesOnly yes",
        "  PasswordAuthentication no",
        "  PreferredAuthentications publickey",
        "  ForwardX11 no",
        "  ForwardX11Trusted no",
        "  UserKnownHostsFile #{render_local_config_path(known_hosts_file)}",
        "  StrictHostKeyChecking accept-new"
      ]
      |> maybe_append_proxy_jump(cluster.proxy_jump, username)
      |> Enum.join("\n")
    end)
  end

  defp maybe_append_proxy_jump(lines, nil, _username), do: lines
  defp maybe_append_proxy_jump(lines, "", _username), do: lines

  defp maybe_append_proxy_jump(lines, proxy_jump, username) do
    lines ++ ["  ProxyJump #{render_jump(proxy_jump, username)}"]
  end

  defp render_jump(proxy_jump, username) do
    cluster = Cluster.fetch!(proxy_jump)
    alias_name = cluster.ssh_alias || cluster.host

    if String.contains?(proxy_jump, "@") do
      proxy_jump
    else
      "#{username}@#{alias_name}"
    end
  rescue
    ArgumentError ->
      if String.contains?(proxy_jump, "@") do
        proxy_jump
      else
        "#{username}@#{proxy_jump}"
      end
  end

  defp maybe_restrict_permissions(path) do
    case :os.type() do
      {:win32, _} -> :ok
      _ -> File.chmod(path, 0o600)
    end
  end

  defp render_local_config_path(path) when is_binary(path) do
    rendered =
      path
      |> String.replace("\\", "/")
      |> String.replace("\"", "\\\"")

    if String.contains?(rendered, [" ", "\t"]) do
      ~s("#{rendered}")
    else
      rendered
    end
  end
end
