defmodule HpcConnect.Command do
  @moduledoc """
  A portable shell command representation.
  """

  @enforce_keys [:binary, :args, :summary]
  defstruct [:binary, :args, :summary, :remote_command, :session_key, extended_debug: false]

  @type t :: %__MODULE__{
          binary: binary(),
          args: [binary()],
          summary: binary(),
          remote_command: binary() | nil,
          # Steady-connection registry key; when set, run/2 routes the command
          # through the persistent OS SSH shell instead of a one-shot process.
          session_key: binary() | nil,
          # When true, SSH.run prints a timestamped trace line before executing.
          extended_debug: boolean()
        }
end
