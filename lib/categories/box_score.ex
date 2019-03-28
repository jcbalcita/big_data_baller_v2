defmodule BigDataBaller.BoxScore do
  alias BigDataBaller.Category
  alias BigDataBaller.AwsService
  alias BigDataBaller.Time

  @game_date_format "{YYYY}/{0M}/{0D}"

  def run(date), do: run(date, date)

  def run(start_date, end_date) do
    with {:ok, _} <- AwsService.creds?(),
         {:ok, start_datetime} <- Time.tuple_to_datetime(start_date),
         {:ok, end_datetime} <- Time.tuple_to_datetime(end_date),
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
      s3_root_dir: "box_score_trad"
    }
  end

  def step_through_days(category) do
    if Time.complete?(category) do
      IO.puts("Done fetching box scores for the specified time range")
    else
      IO.puts(category.current_datetime)
      fetch_and_persist(category)
      step_through_days(Time.step(category))
    end
  end

  defp fetch_and_persist(category) do
    with {:ok, date_str} <- Timex.format(category.current_datetime, @game_date_format),
         {:ok, response} <- Nba.Stats.scoreboard(%{"gameDate" => date_str}),
         game_headers <- Map.get(response, "GameHeader") do
      if game_headers,
        do: Enum.each(game_headers, &process_game(&1, category)),
        else: IO.puts("Error fetching scoreboard for #{date_str}")
    else
      {:error, message} -> IO.puts(message)
    end
  end

  defp process_game(game_header, category) do
    IO.puts(category)
  end
end
