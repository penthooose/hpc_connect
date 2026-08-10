defmodule HpcConnect.Backoff do
  @moduledoc """
  Exponential-backoff scheduling for transient SSH/SCP failures.

  Shared by `HpcConnect.SSH`, `HpcConnect` retry helpers and the steady
  connection module so retries always use a consistent, capped, optionally
  jittered schedule instead of fixed sleeps.

  ## Options

    * `:base_ms` – initial delay (default `1_000`)
    * `:factor` – multiplier per attempt (default `2.0`)
    * `:max_ms` – upper bound for the delay (default `30_000`)
    * `:jitter?` – add up to 20% random jitter (default `false`)
  """

  @defaults [base_ms: 1_000, factor: 2.0, max_ms: 30_000, jitter?: false]

  @spec delay(non_neg_integer(), keyword()) :: non_neg_integer()
  def delay(attempt, opts \\ []) when is_integer(attempt) and attempt >= 0 do
    base_ms = Keyword.get(opts, :base_ms, @defaults[:base_ms])
    factor = Keyword.get(opts, :factor, @defaults[:factor])
    max_ms = Keyword.get(opts, :max_ms, @defaults[:max_ms])
    jitter? = Keyword.get(opts, :jitter?, @defaults[:jitter?])

    capped = min(trunc(base_ms * :math.pow(factor, attempt)), max_ms)

    if jitter? and capped > 0 do
      capped + :rand.uniform(trunc(capped * 0.2) + 1)
    else
      capped
    end
  end

  @doc """
  Extracts the `retry_backoff_*` keys from a caller's option keyword list.
  """
  @spec options(keyword()) :: keyword()
  def options(opts) do
    Keyword.take(opts, [
      :retry_backoff_base_ms,
      :retry_backoff_factor,
      :retry_backoff_max_ms,
      :retry_backoff_jitter
    ])
  end
end
