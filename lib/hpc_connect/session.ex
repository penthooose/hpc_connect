defmodule HpcConnect.Session do
  @moduledoc """
  Runtime connection settings for a specific HPC cluster session.
  """

  alias HpcConnect.{Cluster, EnvFile, Shell}

  @enforce_keys [:cluster]
  defstruct [
    :cluster,
    :username,
    :ssh_alias,
    :uploaded_key_path,
    :identity_file,
    :ssh_config_file,
    :known_hosts_file,
    :credential_dir,
    :proxy_jump,
    :work_dir,
    :vault_dir,
    :port_range,
    # Path to an SSH ControlMaster socket (set by SSH.open_master!/1)
    :master_socket,
    # Erlang :ssh persistent connection reference (set by SSH.open_connection!/1)
    :ssh_conn,
    # Background OS Port holding the proxy jump tunnel (optional OS fallback)
    :tunnel_port,
    # Native Erlang :ssh connection to ProxyJump host (when native jump tunnel is used)
    :jump_ssh_conn,
    # Local listener port opened by native jump tunnel (:ssh.tcpip_tunnel_to_server)
    :jump_tunnel_port,
    # Native Erlang :ssh connection to compute node (second hop of vLLM 2-hop native tunnel)
    :compute_ssh_conn,
    env: %{}
  ]

  @type t :: %__MODULE__{
          cluster: Cluster.t(),
          username: binary() | nil,
          ssh_alias: binary() | nil,
          uploaded_key_path: binary() | nil,
          identity_file: binary() | nil,
          ssh_config_file: binary() | nil,
          known_hosts_file: binary() | nil,
          credential_dir: binary() | nil,
          proxy_jump: binary() | nil,
          work_dir: binary(),
          vault_dir: binary(),
          port_range: {pos_integer(), pos_integer()},
          master_socket: binary() | nil,
          ssh_conn: pid() | reference() | nil,
          tunnel_port: port() | nil,
          jump_ssh_conn: pid() | reference() | nil,
          jump_tunnel_port: pos_integer() | nil,
          compute_ssh_conn: pid() | reference() | nil,
          env: map()
        }

  @spec new(atom() | binary() | Cluster.t(), keyword()) :: t()
  def new(%Cluster{} = cluster, opts), do: build(cluster, opts)
  def new(cluster_name, opts), do: cluster_name |> Cluster.fetch!() |> build(opts)

  @spec target(t()) :: binary()
  def target(%__MODULE__{ssh_alias: alias}) when is_binary(alias) and alias != "", do: alias

  def target(%__MODULE__{username: username, cluster: cluster}) when is_binary(username) do
    "#{username}@#{cluster.host}"
  end

  def target(%__MODULE__{cluster: cluster}), do: cluster.host

  @spec put_env(t(), binary(), binary()) :: t()
  def put_env(%__MODULE__{} = session, key, value) when is_binary(key) and is_binary(value) do
    %{session | env: Map.put(session.env, key, value)}
  end

  @spec merge_env(t(), map()) :: t()
  def merge_env(%__MODULE__{} = session, env_map) when is_map(env_map) do
    %{session | env: Map.merge(session.env, env_map)}
  end

  @spec merge_env_file(t(), binary()) :: t()
  def merge_env_file(%__MODULE__{} = session, path) when is_binary(path) do
    merge_env(session, EnvFile.load(path))
  end

  @spec fetch_env(t(), binary()) :: binary() | nil
  def fetch_env(%__MODULE__{} = session, key) when is_binary(key) do
    Map.get(session.env, key) || System.get_env(key)
  end

  @spec remote_env_prefix(t()) :: binary()
  def remote_env_prefix(%__MODULE__{} = session) do
    session.env
    |> Enum.reject(fn {key, _value} -> String.starts_with?(key, "HPC_CONNECT_") end)
    |> Enum.sort_by(fn {key, _value} -> key end)
    |> Enum.map_join(" && ", fn {key, value} -> "export #{key}=#{Shell.escape(value)}" end)
  end

  # Derive the HPC group from a username.
  #
  # Heuristics:
  # - prefer leading alpha prefix when it has at least 4 chars ("barz123h" -> "barz")
  # - otherwise strip trailing digits and use remaining stem when it has at least 4 chars
  #   ("hpcusr12" -> "hpcusr")
  @spec derive_group(binary() | nil) :: binary() | nil
  defp derive_group(nil), do: nil

  defp derive_group(username) do
    with [leading] <- Regex.run(~r/^([A-Za-z]+)/, username, capture: :all_but_first),
         true <- String.length(leading) >= 4 do
      leading
    else
      _ ->
        stem = String.replace(username, ~r/\d+$/, "")

        if String.length(stem) >= 4 do
          stem
        else
          nil
        end
    end
  end

  defp derive_home_dir(nil), do: nil

  defp derive_home_dir(username) do
    case derive_group(username) do
      nil -> nil
      group -> "/home/hpc/#{group}/#{username}"
    end
  end

  defp derive_work_dir(nil), do: nil

  defp derive_work_dir(username) do
    case derive_group(username) do
      nil -> nil
      group -> "/home/hpc/#{group}/#{username}/.cache/hpc_connect"
    end
  end

  defp derive_vault_dir(nil), do: nil

  defp derive_vault_dir(username) do
    case derive_group(username) do
      nil -> nil
      group -> "/home/vault/#{group}/#{username}"
    end
  end

  defp build(cluster, opts) do
    username = Keyword.get(opts, :username, env("HPC_CONNECT_USERNAME"))
    ssh_alias = Keyword.get(opts, :ssh_alias, cluster.ssh_alias || env("HPC_CONNECT_SSH_ALIAS"))
    uploaded_key_path = normalize_local_path(Keyword.get(opts, :uploaded_key_path))

    identity_file =
      normalize_local_path(Keyword.get(opts, :identity_file, env("HPC_CONNECT_IDENTITY_FILE")))

    ssh_config_file = normalize_local_path(Keyword.get(opts, :ssh_config_file))
    known_hosts_file = normalize_local_path(Keyword.get(opts, :known_hosts_file))
    credential_dir = normalize_local_path(Keyword.get(opts, :credential_dir))

    proxy_jump =
      Keyword.get(opts, :proxy_jump, cluster.proxy_jump || env("HPC_CONNECT_PROXY_JUMP"))

    work_dir =
      opts
      |> Keyword.get(
        :work_dir,
        env("HPC_CONNECT_WORK_DIR") ||
          derive_work_dir(username) ||
          cluster.default_work_dir ||
          raise(
            ArgumentError,
            "could not derive work_dir from username #{inspect(username)}; set :work_dir or HPC_CONNECT_WORK_DIR"
          )
      )
      |> normalize_remote_runtime_dir(:work_dir, username, cluster)

    vault_dir =
      opts
      |> Keyword.get(
        :vault_dir,
        env("HPC_CONNECT_VAULT_DIR") ||
          derive_vault_dir(username) ||
          cluster.vault_dir ||
          raise(
            ArgumentError,
            "could not derive vault_dir from username #{inspect(username)}; set :vault_dir or HPC_CONNECT_VAULT_DIR"
          )
      )
      |> normalize_remote_runtime_dir(:vault_dir, username, cluster)

    port_range =
      Keyword.get(
        opts,
        :port_range,
        parse_port_range(env("HPC_CONNECT_PORT_RANGE")) || {8000, 8999}
      )

    env_overrides = Keyword.get(opts, :env, %{})

    env_overrides =
      case Keyword.get(opts, :env_file) do
        value when is_binary(value) and value != "" ->
          Map.merge(EnvFile.load(value), env_overrides)

        _ ->
          env_overrides
      end

    %__MODULE__{
      cluster: cluster,
      username: username,
      ssh_alias: ssh_alias,
      uploaded_key_path: uploaded_key_path,
      identity_file: identity_file,
      ssh_config_file: ssh_config_file,
      known_hosts_file: known_hosts_file,
      credential_dir: credential_dir,
      proxy_jump: proxy_jump,
      work_dir: work_dir,
      vault_dir: vault_dir,
      port_range: port_range,
      env: env_overrides
    }
  end

  defp env(name), do: System.get_env(name)

  defp normalize_local_path(path) when is_binary(path) and path != "" do
    Path.expand(path)
  end

  defp normalize_local_path(_), do: nil

  defp normalize_remote_runtime_dir(path, kind, username, cluster)
       when is_binary(path) and path != "" do
    trimmed = String.trim(path)
    home_dir = derive_home_dir(username)
    derived_work_dir = derive_work_dir(username)
    derived_vault_dir = derive_vault_dir(username)

    case {kind, trimmed} do
      {:work_dir, value}
      when value in ["$HOME/.cache/hpc_connect", "~/.cache/hpc_connect", cluster.default_work_dir] and
             is_binary(derived_work_dir) ->
        derived_work_dir

      {:vault_dir, value}
      when value in ["$HOME/vault/hpc_connect", "~/vault/hpc_connect", cluster.vault_dir] and
             is_binary(derived_vault_dir) ->
        derived_vault_dir

      {:work_dir, "$HOME/" <> suffix} when is_binary(home_dir) ->
        Path.join(home_dir, suffix)

      {:work_dir, "~/" <> suffix} when is_binary(home_dir) ->
        Path.join(home_dir, suffix)

      {:vault_dir, "$HOME/vault/hpc_connect/" <> suffix} when is_binary(derived_vault_dir) ->
        Path.join(derived_vault_dir, suffix)

      {:vault_dir, "~/vault/hpc_connect/" <> suffix} when is_binary(derived_vault_dir) ->
        Path.join(derived_vault_dir, suffix)

      _ ->
        trimmed
    end
  end

  defp normalize_remote_runtime_dir(path, _kind, _username, _cluster), do: path

  defp parse_port_range(nil), do: nil

  defp parse_port_range(value) do
    case String.split(value, ":", parts: 2) do
      [min_port, max_port] -> {String.to_integer(min_port), String.to_integer(max_port)}
      _ -> nil
    end
  rescue
    ArgumentError -> nil
  end
end
