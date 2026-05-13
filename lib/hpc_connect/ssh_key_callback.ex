defmodule HpcConnect.SSHKeyCallback do
  @moduledoc false
  # Load a private key from the explicit `identity_file` path passed in `:key_cb_private`.

  @behaviour :ssh_client_key_api

  @impl true
  def is_host_key(_key, _host, _alg, _opts), do: true

  @impl true
  def user_key(_alg, opts) do
    cb_opts = Keyword.get(opts, :key_cb_private, opts)

    identity_file =
      Keyword.get(cb_opts, :identity_file) ||
        Keyword.get(opts, :identity_file)

    if is_nil(identity_file) or not File.exists?(identity_file) do
      {:error, "identity_file #{inspect(identity_file)} not found"}
    else
      pem = File.read!(identity_file)

      if String.starts_with?(pem, "-----BEGIN OPENSSH PRIVATE KEY-----") do
        {:error,
         "unsupported OPENSSH PRIVATE KEY format for native Erlang :ssh key callback (use PEM key)"}
      else
        # Use the first supported PEM entry in the file.
        result =
          pem
          |> :public_key.pem_decode()
          |> Enum.find_value(fn entry ->
            try do
              {:ok, :public_key.pem_entry_decode(entry)}
            rescue
              _ -> nil
            catch
              _, _ -> nil
            end
          end)

        result || {:error, "no supported private key found in #{identity_file}"}
      end
    end
  end
end
