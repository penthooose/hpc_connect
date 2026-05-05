defmodule HpcConnect.Model do
  @moduledoc """
  Model metadata and derived runtime environment for remote inference.
  """

  alias HpcConnect.{Session, Shell, SSH}

  @enforce_keys [:repo_id]
  defstruct [:repo_id, :revision, :remote_dir_name, :hf_token_env, extra_env: %{}]

  @type t :: %__MODULE__{
          repo_id: binary(),
          revision: binary() | nil,
          remote_dir_name: binary() | nil,
          hf_token_env: binary(),
          extra_env: map()
        }

  @spec new(binary(), keyword()) :: t()
  def new(repo_id, opts \\ []) do
    %__MODULE__{
      repo_id: repo_id,
      revision: Keyword.get(opts, :revision),
      remote_dir_name: Keyword.get(opts, :remote_dir_name),
      hf_token_env: Keyword.get(opts, :hf_token_env, "HUGGINGFACE_HUB_TOKEN"),
      extra_env: Keyword.get(opts, :extra_env, %{})
    }
  end

  @spec remote_dir(Session.t(), t()) :: binary()
  def remote_dir(%Session{} = session, %__MODULE__{} = model) do
    Path.join(models_root(session), model.remote_dir_name || sanitize_repo_id(model.repo_id))
  end

  @spec models_root(Session.t()) :: binary()
  def models_root(%Session{} = session) do
    Path.join([session.vault_dir, ".cache", "hpc_connect", "models"])
  end

  @spec env_map(Session.t(), t()) :: map()
  def env_map(%Session{} = session, %__MODULE__{} = model) do
    cache_dir = remote_dir(session, model)

    %{
      "HF_HOME" => cache_dir,
      "HUGGINGFACE_HUB_CACHE" => cache_dir,
      "TRANSFORMERS_CACHE" => cache_dir
    }
    |> Map.merge(model.extra_env)
  end

  @spec env_exports(Session.t(), t()) :: binary()
  def env_exports(%Session{} = session, %__MODULE__{} = model) do
    session
    |> env_map(model)
    |> Enum.sort_by(fn {key, _value} -> key end)
    |> Enum.map_join(" ", fn {key, value} -> "#{key}=#{value}" end)
  end

  defp sanitize_repo_id(repo_id) do
    String.replace(repo_id, "/", "--")
  end

  @spec list_remote_command(Session.t()) :: HpcConnect.Command.t()
  def list_remote_command(%Session{} = session) do
    root = models_root(session)

    remote_command =
      "mkdir -p #{Shell.escape(root)} && find #{Shell.escape(root)} -mindepth 1 -maxdepth 1 -type d -printf '%f\\n' | sort"

    SSH.ssh_command(session, remote_command, "List downloaded model directories")
  end

  @spec parse_remote_listing(binary()) :: [map()]
  def parse_remote_listing(output) when is_binary(output) do
    output
    |> String.split("\n", trim: true)
    |> Enum.map(&strip_ansi/1)
    |> Enum.map(&String.trim/1)
    |> Enum.filter(&valid_remote_listing_line?/1)
    |> Enum.map(fn name ->
      %{
        name: name,
        repo_hint: String.replace(name, "--", "/")
      }
    end)
  end

  defp valid_remote_listing_line?(""), do: false

  defp valid_remote_listing_line?(line) do
    not String.contains?(line, ["WARNING", "Warning", "Path", "!!!", "quota"]) and
      String.match?(line, ~r/^[A-Za-z0-9._-]+$/)
  end

  defp strip_ansi(value) do
    Regex.replace(~r/\e\[[0-9;]*m/, value, "")
  end
end
