defmodule HpcConnect.Shell do
  @moduledoc false

  @spec escape(binary() | atom() | number()) :: binary()
  def escape(value) do
    value
    |> to_string()
    |> String.replace("'", "'\"'\"'")
    |> then(&"'#{&1}'")
  end
end
