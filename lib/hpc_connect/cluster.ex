defmodule HpcConnect.Cluster do
  @moduledoc """
  Built-in cluster metadata for FAU / NHR style SSH entry points.
  """

  @enforce_keys [:name, :host]
  defstruct [
    :name,
    :host,
    :ssh_alias,
    :proxy_jump,
    :vault_dir,
    :default_work_dir,
    :default_partition,
    :gpu_type,
    :require_gres,
    :http_proxy,
    :notes,
    aliases: []
  ]

  @type t :: %__MODULE__{
          name: atom(),
          host: binary(),
          aliases: [binary()],
          ssh_alias: binary() | nil,
          proxy_jump: binary() | nil,
          vault_dir: binary() | nil,
          default_work_dir: binary() | nil,
          default_partition: binary() | nil,
          gpu_type: binary() | nil,
          require_gres: binary() | nil,
          http_proxy: binary() | nil,
          notes: binary() | nil
        }

  @spec defaults() :: [t()]
  def defaults do
    [
      %__MODULE__{
        name: :csnhr,
        host: "csnhr.nhr.fau.de",
        aliases: ["csnhr.nhr.fau.de", "csnhr"],
        ssh_alias: "csnhr",
        vault_dir: "$HOME/vault/hpc_connect",
        default_work_dir: "$HOME/.cache/hpc_connect",
        notes: "Gateway / jump host"
      },
      %__MODULE__{
        name: :fritz,
        host: "fritz.nhr.fau.de",
        aliases: ["fritz.nhr.fau.de", "fritz"],
        ssh_alias: "fritz",
        proxy_jump: "csnhr.nhr.fau.de",
        vault_dir: "$HOME/vault/hpc_connect",
        default_work_dir: "$HOME/.cache/hpc_connect",
        http_proxy: "http://proxy.nhr.fau.de:80"
      },
      %__MODULE__{
        name: :alex,
        host: "alex.nhr.fau.de",
        aliases: ["alex.nhr.fau.de", "alex"],
        ssh_alias: "alex",
        proxy_jump: "csnhr.nhr.fau.de",
        vault_dir: "$HOME/vault/hpc_connect",
        default_work_dir: "$HOME/.cache/hpc_connect",
        http_proxy: "http://proxy.nhr.fau.de:80"
      },
      %__MODULE__{
        name: :helma,
        host: "helma.nhr.fau.de",
        aliases: ["helma.nhr.fau.de", "helma"],
        ssh_alias: "helma",
        proxy_jump: "csnhr.nhr.fau.de",
        vault_dir: "$HOME/vault/hpc_connect",
        default_work_dir: "$HOME/.cache/hpc_connect",
        http_proxy: "http://proxy.nhr.fau.de:80"
      },
      %__MODULE__{
        name: :tinyx,
        host: "tinyx.nhr.fau.de",
        aliases: ["tinyx.nhr.fau.de", "tinyx"],
        ssh_alias: "tinyx",
        proxy_jump: "csnhr.nhr.fau.de",
        vault_dir: "$HOME/vault/hpc_connect",
        default_work_dir: "$HOME/.cache/hpc_connect",
        gpu_type: "a100",
        default_partition: "a100",
        require_gres: "gpu:1",
        http_proxy: "http://proxy.nhr.fau.de:80",
        notes: "Tier3-Grundversorgung"
      },
      %__MODULE__{
        name: :woody,
        host: "woody.nhr.fau.de",
        aliases: ["woody.nhr.fau.de", "woody"],
        ssh_alias: "woody",
        proxy_jump: "csnhr.nhr.fau.de",
        vault_dir: "$HOME/vault/hpc_connect",
        default_work_dir: "$HOME/.cache/hpc_connect",
        http_proxy: "http://proxy.nhr.fau.de:80",
        notes: "Tier3-Grundversorgung"
      },
      %__MODULE__{
        name: :meggie,
        host: "meggie.rrze.uni-erlangen.de",
        aliases: ["meggie.rrze.fau.de", "meggie.rrze.uni-erlangen.de", "meggie"],
        ssh_alias: "meggie",
        proxy_jump: "csnhr.nhr.fau.de",
        vault_dir: "$HOME/vault/hpc_connect",
        default_work_dir: "$HOME/.cache/hpc_connect",
        notes: "Tier3-Grundversorgung"
      }
    ]
  end

  @spec fetch!(atom() | binary()) :: t()
  def fetch!(name) when is_binary(name) do
    Enum.find(defaults(), fn cluster ->
      Atom.to_string(cluster.name) == name or cluster.host == name or name in cluster.aliases
    end) || raise ArgumentError, "unknown cluster #{inspect(name)}"
  end

  def fetch!(name) when is_atom(name) do
    Enum.find(defaults(), &(&1.name == name)) ||
      raise ArgumentError, "unknown cluster #{inspect(name)}"
  end
end
