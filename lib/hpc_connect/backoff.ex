defmodule HpcConnect.Backoff do
  @moduledoc """
  Backoff scheduling for transient SSH/SCP failures.

  Shared by `HpcConnect.SSH`, `HpcConnect` retry helpers and the steady
  connection module so retries always use a consistent, capped, optionally
  jittered schedule instead of fixed sleeps.

  Supports exponential (default) and linear schedules.

  ## Options

  `delay/2` accepts both the internal keys (`:base_ms`, `:factor`, `:max_ms`,
  `:mode`, `:jitter?`) and the `retry_backoff_*`-prefixed variants used by
  callers (see `options/1`), so tuning actually takes effect.

    * `:base_ms` / `:retry_backoff_base_ms` – initial delay / linear step (default `1_000`)
    * `:factor` / `:retry_backoff_factor` – exponential multiplier (default `2.0`)
    * `:max_ms` / `:retry_backoff_max_ms` – upper bound (default `30_000`)
    * `:mode` / `:retry_backoff_mode` – `:exponential` (default) | `:linear`
    * `:jitter?` / `:retry_backoff_jitter` – add up to 20% jitter (default `false`)
  """

  @defaults [mode: :exponential, base_ms: 1_000, factor: 2.0, max_ms: 30_000, jitter?: false]

  # Retry-forever schedule used to wait out the csnhr jump-gateway throttle
  # (it refuses new connections for up to ~1 minute after too many requests):
  # a linear 10 s, 20 s, 30 s, ... progression capped at 60 s.
  @forever_defaults [mode: :linear, base_ms: 10_000, max_ms: 60_000]

  @spec delay(non_neg_integer(), keyword()) :: non_neg_integer()
  def delay(attempt, opts \\ []) when is_integer(attempt) and attempt >= 0 do
    mode = resolve_opt(opts, :mode, :retry_backoff_mode, @defaults[:mode])
    base_ms = resolve_opt(opts, :base_ms, :retry_backoff_base_ms, @defaults[:base_ms])
    factor = resolve_opt(opts, :factor, :retry_backoff_factor, @defaults[:factor])
    max_ms = resolve_opt(opts, :max_ms, :retry_backoff_max_ms, @defaults[:max_ms])
    jitter? = resolve_opt(opts, :jitter?, :retry_backoff_jitter, @defaults[:jitter?])

    capped =
      case mode do
        # Linear: step * (attempt + 1) — attempt 0 → 1 step, attempt 1 → 2 steps, ...
        :linear -> min(trunc(base_ms * (attempt + 1)), max_ms)
        _ -> min(trunc(base_ms * :math.pow(factor, attempt)), max_ms)
      end

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
      :retry_backoff_mode,
      :retry_backoff_jitter
    ])
  end

  @doc """
  Backoff schedule for retry-forever: a linear 10 s → 60 s progression so the
  jump-gateway's ~1-minute throttle window can clear between attempts. Caller
  `retry_backoff_*` overrides are respected (`put_new`), so explicit tuning wins.
  """
  @spec forever_options(keyword()) :: keyword()
  def forever_options(opts) do
    options(opts)
    |> Keyword.put_new(:retry_backoff_mode, @forever_defaults[:mode])
    |> Keyword.put_new(:retry_backoff_base_ms, @forever_defaults[:base_ms])
    |> Keyword.put_new(:retry_backoff_max_ms, @forever_defaults[:max_ms])
  end

  # Reads the unprefixed key first, then the retry_backoff_* variant, then default.
  defp resolve_opt(opts, key, prefixed, default) do
    case Keyword.fetch(opts, key) do
      {:ok, value} -> value
      :error -> Keyword.get(opts, prefixed, default)
    end
  end
end
