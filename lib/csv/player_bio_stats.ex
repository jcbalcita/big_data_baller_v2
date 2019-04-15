defmodule BigDataBaller.Csv.PlayerBioStats do
  alias BigDataBaller.Csv.PlayerBioStats

  @dir_path "spark/csv/player/player_bio_stats"

  defstruct [:team_stats, :player_stats, :home_team, :away_team]

  defp file_library, do: Application.get_env(:big_data_baller, :file_library, File)
  defp io, do: Application.get_env(:big_data_baller, :io, IO)


  def run(start_year \\ 1996, end_year \\ 2018) do
    start_year..end_year |> Enum.each(&run_year/1)
  end

  def run_year(year) do
    case open_csv_file(year) do
      {:ok, file} ->
        file_to_read = "syncS3/player/player_bio_stats/#{year}.json"
        convert_to_matrix(file_to_read)
        |> CSV.encode()
        |> Enum.each(fn contents -> io().write(file, contents) end)

      {:error, error} ->
        raise %RuntimeError{message: error}

      _ ->
        raise %RuntimeError{message: "Unable to create csv file for the year #{year}"}
    end
  end

  defp convert_to_matrix(filepath) do
    with {:ok, contents} <- file_library().read(filepath),
         {:ok, json_season} <- BigDataBaller.json_library().decode(contents),
         players <- Map.get(json_season, "LeagueDashPlayerBioStats") do
      Enum.filter(players, fn player -> player["PLAYER_NAME"] != nil && player["PLAYER_NAME"] != "" end)
      Enum.sort_by(players, fn player -> player["PLAYER_ID"] end)
      |> Enum.map(&get_player_bio_stats/1)
    else
      _ -> IO.puts("Error reading #{filepath}")
    end
  end

  defp open_csv_file(year) do
    with {:ok, filepath} <- maybe_create_dir(year),
         {:ok, file} <- get_new_csv_file(filepath) do
      {:ok, file}
    else
      {:error, error} ->
        raise %RuntimeError{message: error}
    end
  end

  defp maybe_create_dir(year) do
    filepath = "#{@dir_path}/#{year}.csv"

    if file_library().dir?(@dir_path) do
      {:ok, filepath}
    else
      io().puts "Creating directory - #{@dir_path}"
      case file_library().mkdir(@dir_path) do
        :ok -> {:ok, filepath}
        _ -> {:error, "Could not create directory"}
      end
    end
  end

  defp get_new_csv_file(filepath) do
    case file_library().stat(filepath) do
      {:ok, _} ->
        file_library().rm!(filepath)
        io().puts("Deleted existing file #{filepath}")
        create_and_open_csv_file(filepath)

      {:error, :enoent} ->
        create_and_open_csv_file(filepath)
    end
  end

  defp create_and_open_csv_file(filepath) do
    with :ok <- file_library().touch(filepath) do
      io().puts("Touched new file #{filepath}")
      file_library().open(filepath, [:write, :utf8])
    else
      _ -> {:error, "Could not create file #{filepath}"}
    end
  end

  defp get_player_bio_stats(player) do
    [
      player["PLAYER_ID"],
      player["PLAYER_NAME"],
      player["PLAYER_HEIGHT"],
      player["PLAYER_HEIGHT_INCHES"],
      player["PLAYER_WEIGHT"],
      player["AGE"],
      player["COLLEGE"],
      player["COUNTRY"],
      player["DRAFT_NUMBER"],
      player["DRAFT_ROUND"],
      player["DRAFT_YEAR"],
      player["GP"],
      player["PTS"],
      player["REB"],
      player["AST"],
      player["USG_PCT"],
      player["TS_PCT"],
      player["DREB_PCT"],
      player["OREB_PCT"],
      player["AST_PCT"],
      player["NET_RATING"],
      player["TEAM_ABBREVIATION"],
      player["TEAM_ID"]
    ]
  end
end