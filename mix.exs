defmodule BigDataBaller.MixProject do
  use Mix.Project

  def project do
    [
      app: :big_data_baller,
      version: "0.1.0",
      elixir: "~> 1.7",
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
      {:nba, "~> 0.7"},
      {:jason, "~> 1.1"},
      {:ex_aws, "~> 2.1"},
      {:ex_aws_s3, "~> 2.0"},
      {:ex_aws_dynamo, "~> 2.0"},
      {:sweet_xml, "~> 0.6"},
      {:hackney, "~> 1.15"},
      {:timex, "~> 3.6"},
      {:csv, "~> 2.3"}
    ]
  end
end
