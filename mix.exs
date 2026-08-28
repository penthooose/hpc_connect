defmodule HpcConnect.MixProject do
  use Mix.Project

  def project do
    [
      app: :hpc_connect,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :ssh, :crypto, :public_key]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:jason, "~> 1.4"},
      # Regular (not optional) so `Mix.install([{:hpc_connect, ...}])` in a
      # Livebook notebook fetches Kino automatically — no need to add it to the
      # notebook's own Mix.install. Only used for the interactive setup forms.
      {:kino, "~> 0.19"}
    ]
  end
end
