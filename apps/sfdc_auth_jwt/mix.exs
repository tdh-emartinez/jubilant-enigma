defmodule SfdcAuthJwt.MixProject do
  use Mix.Project

  def project do
    [
      app: :sfdc_auth_jwt,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.9.1",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:poison, "~> 4.0"},
      {:joken, "~> 2.1"},
      {:httpoison, "~> 1.5"}
      # {:sibling_app_in_umbrella, in_umbrella: true}
    ]
  end
end
