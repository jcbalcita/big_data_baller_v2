defmodule BigDataBaller.BoxScore do
  alias BigDataBaller.Category
  alias BigDataBaller.AwsService
  alias BigDataBaller.TimeService
  alias BigDataBaller.Util

  @game_date_format "{M}/{D}/{YYYY}"
  @s3_directory_format "{YYYY}/{0M}/{0D}"

  defp time_library, do: TimeService.time_library()

  def run do
    yesterday_dt = TimeService.yesterday(time_library().now())
    yesterday_tuple =
      time_library().format!(yesterday_dt, "{YYYY}/{M}/{D}")
      |> String.split("/")
      |> Enum.map(&String.to_integer/1)
      |> List.to_tuple()

    run(yesterday_tuple)
  end

  def run(date), do: run(date, date)

  def run(start_date, end_date) do
    with {:ok, _} <- AwsService.creds?(),
         {:ok, start_datetime} <- TimeService.tuple_to_datetime(start_date),
         {:ok, end_datetime} <- TimeService.tuple_to_datetime(end_date),
         category <- new_category(start_datetime, end_datetime) do
      step_through_days(category)
    else
      {:error, message} -> IO.puts(message)
      _ -> IO.puts("#{__MODULE__}: Something went wrong")
    end
  end

  def new_category(start_datetime, end_datetime) do
    %Category{
      endpoint: :box_score,
      current_datetime: start_datetime,
      end_datetime: end_datetime,
      interval: :day,
      s3_bucket: AwsService.s3_bucket(),
      s3_root_dir: "box_score"
    }
  end

  def step_through_days(category) do
    if TimeService.complete?(category) do
      IO.puts("Done fetching box scores for the specified time range")
    else
      fetch_and_persist(category)
      step_through_days(TimeService.step(category))
    end
  end

  defp fetch_and_persist(category) do
    with {:ok, date_str} <- time_library().format(category.current_datetime, @game_date_format),
         {:ok, response} <- Nba.Stats.scoreboard(gameDate: date_str),
         game_headers <- Map.get(response, "GameHeader") do
      if game_headers,
        do: Enum.each(game_headers, &process_game(&1, category)),
        else: IO.puts("Error fetching scoreboard for #{date_str}")
    else
      {:error, message} -> IO.puts(message)
    end
  end

  defp process_game(game_info, %{endpoint: endpoint, current_datetime: dt, s3_root_dir: s3_root_dir}) do
    with season_start_year <- game_info["SEASON"],
         qs_season <- Util.querystring_season(season_start_year),
         gid <- game_info["GAME_ID"],
         [_, game_code] <- String.split(game_info["GAMECODE"], "/"),
         {:ok, year_month_day} <- time_library().format(dt, @s3_directory_format),
         s3_path <- "#{s3_root_dir}/#{season_start_year}/#{year_month_day}/#{gid}-#{game_code}.json" do

      Process.sleep(900)

      case apply(Nba.Stats, endpoint, [[GameID: gid, Season: qs_season]]) do
        {:ok, response} ->
          BigDataBaller.json_library().encode!(response)
          |> AwsService.write_s3(s3_path)

        {:error, message} ->
          IO.puts("Error fetching box score for #{gid}-#{game_code}... #{message}")
      end
    else
      _ ->
        if (game_info["GAME_ID"]),
          do: IO.puts("Error processing GID: #{game_info["GAME_ID"]}"),
          else: IO.puts("Scoreboard result object is messed up.")
    end
  end
end
