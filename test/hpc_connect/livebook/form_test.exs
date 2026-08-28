defmodule HpcConnect.Livebook.FormTest do
  use ExUnit.Case, async: false

  alias HpcConnect.Livebook.Form

  defp unique_name, do: {:test, System.unique_integer([:positive])}

  setup do
    # hpc_connect does not depend on Kino; stub the subscribe step so the owner
    # treats the form opaquely (it only needs the tag for event matching).
    :persistent_term.put({HpcConnect.Livebook.Form, :subscribe_fun}, fn _control, _tag -> :ok end)
    :persistent_term.put({HpcConnect.Livebook.Form, :error_content_fun}, fn msg -> {:err, msg} end)

    on_exit(fn ->
      :persistent_term.erase({HpcConnect.Livebook.Form, :subscribe_fun})
      :persistent_term.erase({HpcConnect.Livebook.Form, :error_content_fun})
    end)

    :ok
  end

  defp start_owner do
    name = unique_name()
    owner = Form.ensure(name)

    on_exit(fn ->
      :global.unregister_name({HpcConnect.Livebook.Form, name})
    end)

    owner
  end

  defp test_form, do: :fake_form

  describe "persistent form owner (re-submittable without re-running the cell)" do
    test "await returns the handler result on the first submit" do
      owner = start_owner()

      Form.attach(owner, test_form(), nil, fn data, _event ->
        {String.upcase(data[:x]), nil}
      end)

      task = Task.async(fn -> Form.await(owner, 5_000) end)
      Process.sleep(50)
      Form.submit(owner, %{x: "hello"})

      assert Task.await(task) == "HELLO"
    end

    test "the same owner can be re-submitted and re-awaited multiple times" do
      owner = start_owner()

      Form.attach(owner, test_form(), nil, fn data, _event -> {data[:x], nil} end)

      # First submit: a cell is awaiting.
      task = Task.async(fn -> Form.await(owner, 5_000) end)
      Process.sleep(50)
      Form.submit(owner, %{x: "a"})
      assert Task.await(task) == "a"

      # Re-submit with no cell waiting (user edits + presses Setup again): the
      # handler still runs and must not crash the owner.
      Form.submit(owner, %{x: "b"})

      # A later await gets the next submit.
      task = Task.async(fn -> Form.await(owner, 5_000) end)
      Process.sleep(50)
      Form.submit(owner, %{x: "c"})
      assert Task.await(task) == "c"
    end

    test "a second attach switches to a new form and handler" do
      owner = start_owner()

      Form.attach(owner, test_form(), nil, fn data, _event -> {"first:" <> data[:x], nil} end)

      task = Task.async(fn -> Form.await(owner, 5_000) end)
      Process.sleep(50)
      Form.submit(owner, %{x: "a"})
      assert Task.await(task) == "first:a"

      # Re-running the setup cell attaches a fresh form + handler.
      Form.attach(owner, test_form(), nil, fn data, _event -> {"second:" <> data[:x], nil} end)

      task = Task.async(fn -> Form.await(owner, 5_000) end)
      Process.sleep(50)
      Form.submit(owner, %{x: "z"})

      assert Task.await(task) == "second:z"
    end

    test "a :retry handler result keeps the cell waiting until a valid submit" do
      owner = start_owner()

      Form.attach(owner, test_form(), nil, fn data, _event ->
        if data[:x] == "ok" do
          {:valid_result, nil}
        else
          {:retry, nil}
        end
      end)

      task = Task.async(fn -> Form.await(owner, 5_000) end)
      Process.sleep(50)

      # Invalid submit: the owner re-renders the frame but keeps the waiter.
      Form.submit(owner, %{x: "bad"})
      Process.sleep(100)
      assert Task.yield(task, 200) == nil

      # A later valid submit replies to the still-pending waiter.
      Form.submit(owner, %{x: "ok"})
      assert Task.await(task) == :valid_result
    end

    test "a raised handler keeps the cell waiting (does not return an error)" do
      owner = start_owner()

      Form.attach(owner, test_form(), nil, fn data, _event ->
        if data[:x] == "ok" do
          {:valid_result, nil}
        else
          raise "invalid: #{data[:x]}"
        end
      end)

      task = Task.async(fn -> Form.await(owner, 5_000) end)
      Process.sleep(50)

      Form.submit(owner, %{x: "bad"})
      Process.sleep(100)
      assert Task.yield(task, 200) == nil

      Form.submit(owner, %{x: "ok"})
      assert Task.await(task) == :valid_result
    end
  end
end
