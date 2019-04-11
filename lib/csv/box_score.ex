defmodule BigDataBaller.Csv.BoxScore do
  alias BigDataBaller.Csv.BoxScore

  defstruct [:team_stats, :player_stats, :home_team, :away_team]

  defp file_library, do: Application.get_env(:big_data_baller, :file_library, File)
  defp io, do: Application.get_env(:big_data_baller, :io, IO)

  defp dir_path, do: "spark/csv/box_score"

  def run(start_year \\ 1996, end_year \\ 2018) do
    start_year..end_year |> Enum.each(&run_year/1)
  end

  def run_year(year) do
    case open_file(year) do
      {:ok, file} ->
        Path.wildcard("syncS3/box_score/#{year}/**/*.json")
        |> convert_to_matrix()
        |> CSV.encode()
        |> Enum.each(fn contents -> io().write(file, contents) end)

        file_library().close(file)

      {:error, error} ->
        raise %RuntimeError{message: error}

      _ ->
        raise %RuntimeError{message: "Unable to create csv file for the year #{year}"}
    end
  end

  defp open_file(year) do
    with {:ok, filepath} <- maybe_create_dir(year),
         {:ok, file} <- get_new_csv_file(filepath) do
      {:ok, file}
    else
      {:error, error} ->
        raise %RuntimeError{message: error}
    end
  end

  defp maybe_create_dir(year) do
    filepath = "#{dir_path()}/#{year}.csv"

    if file_library().dir?(dir_path()) do
      {:ok, filepath}
    else
      io().puts "Creating directory - #{dir_path()}"
      case file_library().mkdir(dir_path()) do
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

  defp convert_to_matrix(filepaths) do
    filepaths
    |> Enum.reduce([], fn filepath, acc ->
      player_rows = create_player_rows(filepath)
      case player_rows do
        nil -> acc
        _ -> Enum.concat(acc, create_player_rows(filepath))
      end
    end)
  end

  defp create_player_rows(filepath) do
    with {:ok, body} <- file_library().read(filepath),
         {:ok, game_map} <- Jason.decode(body),
         box_score <- game_map_to_box_score_struct(game_map, filepath) do
      if !all_star_game?(box_score), do: create_rows(box_score)
    else
      {:error, error} -> io().puts(error)
      _ -> io().puts("[ERROR] Unable to ro read #{filepath}")
    end
  end

  defp game_map_to_box_score_struct(game, filepath) do
    {home_team, away_team} = home_and_away_teams_from_filepath(filepath)
   %BoxScore{
      team_stats: Map.get(game, "TeamStats"),
      player_stats: Map.get(game, "PlayerStats"),
      home_team: home_team,
      away_team: away_team
    }
  end

  def create_rows(%{player_stats: player_stats, home_team: home_team}) do
    Enum.map(player_stats, &get_player_stats(&1, home_team))
  end

  defp get_player_stats(player_map, home_team) do
    [
      player_map["PLAYER_ID"],
      minutes(player_map["MIN"]),
      player_map["PTS"],
      player_map["OREB"],
      player_map["DREB"],
      player_map["REB"],
      player_map["AST"],
      player_map["STL"],
      player_map["BLK"],
      player_map["TO"],
      player_map["FTM"],
      player_map["FTA"],
      player_map["FGM"],
      player_map["FGA"],
      player_map["FG3M"],
      player_map["FG3A"],
      player_map["PLAYER_NAME"],
      player_map["TEAM_ID"],
      player_map["TEAM_ABBREVIATION"],
      player_map["START_POSITION"],
      player_map["PLUS_MINUS"],
      player_map["GAME_ID"],
      player_map["TEAM_ABBREVIATION"] == home_team
    ]
  end

  defp minutes(nil), do: nil

  defp minutes(min_str),
    do: String.split(min_str, ":") |> List.first() |> Integer.parse() |> elem(0)

  defp home_and_away_teams_from_filepath(filepath) do
    String.split(filepath, "/")
    |> List.last()
    |> String.split("-")
    |> List.last()
    |> String.split(".")
    |> List.first()
    |> String.split_at(3)
  end

  defp all_star_game?(%{team_stats: [team | _], home_team: home_team}) do
    is_new_style_asg = team["TEAM_CITY"] == "Team"
    is_old_style_asg = home_team == "WST" || home_team == "EST"

    is_new_style_asg || is_old_style_asg
  end
end