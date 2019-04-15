defmodule BigDataBaller.TeamStats do
  alias BigDataBaller.Category
  alias BigDataBaller.Util

  @measure_types %{
    four_factors: "Four Factors",
    advanced: "Advanced",
    scoring: "Scoring",
    opponent: "Opponent",
    defense: "Defense"
  }

  defp new_category(start_year, end_year) do
    %Category{
      endpoint: :team_stats,
      current_datetime: start_year,
      end_datetime: end_year,
      interval: :year,
      s3_bucket: AwsService.s3_bucket(),
      s3_root_dir: "team"
    }
  end
end