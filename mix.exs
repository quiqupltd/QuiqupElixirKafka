defmodule QuiqupElixirKafka.Mixfile do
  use Mix.Project

  def project do
    [
      app: :quiqup_elixir_kafka,
      version: "0.1.0",
      elixir: "~> 1.4",
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
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, g4it: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"},
      {:kafka_ex, "~> 0.8.3"}
    ]
  end
end
