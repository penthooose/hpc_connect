defmodule HpcConnect.BatchTest do
  use ExUnit.Case, async: true

  alias HpcConnect.Batch

  defp session do
    HpcConnect.new_session(:fritz,
      username: "hpcusr01",
      ssh_alias: "fritz",
      work_dir: "/scratch/hpc_connect",
      vault_dir: "/vault/models"
    )
  end

  # Builds a synthetic combined output as the remote `{ ...; }` script would
  # produce for the given per-command `{output, status}` pairs using the suffix.
  defp combined_output(pairs, suffix) do
    pairs
    |> Enum.with_index(1)
    |> Enum.map_join("", fn {{output, status}, i} ->
      out = if output == "", do: "", else: output <> "\n"
      "#{out}__HPC_BATCH_EXIT_#{i}_#{suffix}__#{status}\n__HPC_BATCH_END_#{i}_#{suffix}__\n"
    end)
  end

  describe "run/3 with a stubbed run_fun" do
    test "splits each command's output and exit status" do
      suffix = "abc123"

      commands = [
        {:first, "echo hello"},
        {:second, "false"},
        {:third, "printf 'world\\n2'"}
      ]

      canned =
        combined_output(
          [{"hello", 0}, {"", 1}, {"world\n2", 0}],
          suffix
        )

      results =
        Batch.run(session(), commands,
          suffix: suffix,
          run_fun: fn _script -> {canned, 0} end
        )

      assert [
               %{label: :first, output: "hello", status: 0},
               %{label: :second, output: "", status: 1}
             ] =
               Enum.take(results, 2)

      assert %{label: :third, output: "world\n2", status: 0} = Enum.at(results, 2)
      assert length(results) == 3
    end

    test "preserves order and handles multi-line output with blank lines" do
      suffix = "xyz"
      commands = [{:a, "echo a"}, {:b, "echo b1; echo; echo b2"}]

      canned =
        combined_output(
          [{"a", 0}, {"b1\n\nb2", 0}],
          suffix
        )

      results = Batch.run(session(), commands, suffix: suffix, run_fun: fn _ -> {canned, 0} end)

      assert %{label: :a, output: "a", status: 0} = Enum.at(results, 0)
      assert %{label: :b, output: "b1\n\nb2", status: 0} = Enum.at(results, 1)
    end

    test "propagates a non-zero segment status without crashing" do
      suffix = "zzz"
      commands = [{:boom, "exit 7"}]

      canned = combined_output([{"", 7}], suffix)
      results = Batch.run(session(), commands, suffix: suffix, run_fun: fn _ -> {canned, 7} end)

      assert [%{label: :boom, output: "", status: 7}] = results
    end
  end
end
