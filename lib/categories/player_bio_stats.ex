defmodule BigDataBaller.PlayerBioStats do
  alias BigDataBaller.Category
  alias BigDataBaller.AwsService
  alias BigDataBaller.TimeService
  alias BigDataBaller.Util

  def run(year), do: run(year, year)

  def run(start_year, end_year) do
    with {:ok, _} <- AwsService.creds?(),
         category <- new_category(start_year, end_year) do
      step_through_years(category)
    end
  end

  def new_category(start_year, end_year) do
    %Category{
      endpoint: :player_bio_stats,
      current_datetime: start_year,
      end_datetime: end_year,
      interval: :year,
      s3_bucket: AwsService.s3_bucket(),
      s3_root_dir: "player/player_bio_stats"
    }
  end

  def step_through_years(category) do
    if TimeService.complete?(category) do
      IO.puts("Done fetching box scores for the specified time range")
    else
      fetch_and_persist(category)
      step_through_years(TimeService.step(category))
    end
  end

  defp fetch_and_persist(%{endpoint: endpoint, current_datetime: dt, s3_root_dir: s3_root_dir}) do
    with season <- Util.querystring_season(dt),
         s3_path <- "#{s3_root_dir}/#{dt}.json",
         {:ok, bio_stats} <- apply(Nba.Stats, endpoint, [[Season: season]]) do
      BigDataBaller.json_library().encode!(bio_stats)
      |> AwsService.write_s3(s3_path)
    else
      _ -> IO.puts("Error fetching bio_stats for year #{dt}")
    end
  end
end
