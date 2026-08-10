defmodule HpcConnect.BackoffTest do
  use ExUnit.Case, async: true

  alias HpcConnect.Backoff

  describe "delay/2" do
    test "grows exponentially from the base" do
      assert Backoff.delay(0) == 1_000
      assert Backoff.delay(1) == 2_000
      assert Backoff.delay(2) == 4_000
      assert Backoff.delay(3) == 8_000
    end

    test "respects base_ms and factor" do
      opts = [base_ms: 100, factor: 3]
      assert Backoff.delay(0, opts) == 100
      assert Backoff.delay(1, opts) == 300
      assert Backoff.delay(2, opts) == 900
    end

    test "caps at max_ms" do
      opts = [base_ms: 100, factor: 2, max_ms: 1_000]
      assert Backoff.delay(0, opts) == 100
      assert Backoff.delay(3, opts) == 800
      assert Backoff.delay(4, opts) == 1_000
      assert Backoff.delay(20, opts) == 1_000
    end

    test "adds jitter within bounds when enabled" do
      opts = [base_ms: 1_000, factor: 2, max_ms: 100_000, jitter?: true]

      for attempt <- 0..10 do
        value = Backoff.delay(attempt, opts)
        floor = min(trunc(1_000 * :math.pow(2, attempt)), 100_000)
        assert value >= floor
        assert value <= trunc(floor * 1.2) + 1
      end
    end

    test "extracts backoff options" do
      assert Backoff.options(retry_backoff_base_ms: 500, retry_backoff_max_ms: 9_000, foo: 1) ==
               [retry_backoff_base_ms: 500, retry_backoff_max_ms: 9_000]
    end
  end
end
