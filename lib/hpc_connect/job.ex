defmodule HpcConnect.Job do
  @moduledoc """
  Job-level options for starting a remote inference service.
  """

  alias HpcConnect.Session

  defstruct [
    :partition,
    :gpus,
    :walltime,
    :port,
    :port_range,
    :conda_env,
    modules: [],
    python_bin: "python",
    vllm_host: "0.0.0.0",
    extra_args: []
  ]

  @type t :: %__MODULE__{
          partition: binary() | nil,
          gpus: pos_integer() | nil,
          walltime: binary() | nil,
          port: pos_integer() | nil,
          port_range: {pos_integer(), pos_integer()},
          conda_env: binary() | nil,
          modules: [binary()],
          python_bin: binary(),
          vllm_host: binary(),
          extra_args: [binary()]
        }

  @spec new(Session.t(), keyword()) :: t()
  def new(%Session{} = session, opts \\ []) do
    %__MODULE__{
      partition: Keyword.get(opts, :partition, session.cluster.default_partition),
      gpus: Keyword.get(opts, :gpus),
      walltime: Keyword.get(opts, :walltime),
      port: Keyword.get(opts, :port),
      port_range: Keyword.get(opts, :port_range, session.port_range),
      conda_env: Keyword.get(opts, :conda_env),
      modules: Keyword.get(opts, :modules, []),
      python_bin: Keyword.get(opts, :python_bin, "python"),
      vllm_host: Keyword.get(opts, :vllm_host, "0.0.0.0"),
      extra_args: Keyword.get(opts, :extra_args, [])
    }
  end
end
