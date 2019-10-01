defmodule ElxSfdcApi.MixProject do
  use Mix.Project

  def project do
    [
      app: :elx_sfdc_api,
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
      {:xml_builder, "~> 2.1"},
      {:elixir_xml_to_map, "~> 0.1.2"},
      {:sfdc_auth_jwt, in_umbrella: true}
    ]
  end
end
