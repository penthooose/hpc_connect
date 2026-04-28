defmodule HpcConnect.Command do
  @moduledoc """
  A portable shell command representation.
  """

  @enforce_keys [:binary, :args, :summary]
  defstruct [:binary, :args, :summary, :remote_command]

  @type t :: %__MODULE__{
          binary: binary(),
          args: [binary()],
          summary: binary(),
          remote_command: binary() | nil
        }
end
