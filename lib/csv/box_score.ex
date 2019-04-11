defmodule BigDataBaller.Csv.BoxScore do

  def run(start_year \\ 1996, end_year \\ 2018) do
    start_year..end_year |> Enum.each(&run_year/1)
  end

  def run_year(year) do
    File.mkdir("spark/csv/box_score")
    File.touch("spark/csv/box_score/#{year}.csv")
    {:ok, file} = File.open("spark/csv/box_score/#{year}.csv", [:write, :utf8])

    Path.wildcard("syncS3/box_score/#{year}/**/*.json")
    |> convert_to_matrix()
    |> CSV.encode()
    |> Enum.each(&IO.write(file, &1))

    File.close(file)
  end

  defp convert_to_matrix(filepaths) do
    filepaths
    |> Enum.reduce([], fn filepath, acc ->
      if String.contains?(filepath, "WSTEST") || String.contains?(filepath, "ESTWST") do 
        acc
      else 
        player_rows = create_player_rows(filepath) 
        case player_rows do
          nil -> acc
          _ -> Enum.concat(acc, create_player_rows(filepath))
        end
      end
    end)
  end

  defp create_player_rows(filepath) do
    with {:ok, body} <- File.read(filepath),
         {:ok, json} <- Jason.decode(body),
         home_away_names <- home_and_away_teams_from_filepath(filepath) do
      
      if not_an_all_star_game(json), do: create_rows(json, home_away_names)
    else
      _ -> IO.puts("[ERROR] Unable to ro read #{filepath}")
    end
  end

  def create_rows(box_score, home_away_names) do
    player_stats = Map.get(box_score, "PlayerStats")
    team_stats = Map.get(box_score, "TeamStats")
    [{_, home_team} | _ ] = get_team_home_away_status(team_stats, home_away_names)

    Enum.map(player_stats, &get_player_stats(&1, home_team))
  end

  defp get_player_stats(player_map, home_team_name) do
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
      player_map["TEAM_ABBREVIATION"] == home_team_name
    ]
  end

  defp minutes(nil), do: nil

  defp minutes(min_str),
    do: String.split(min_str, ":") |> List.first() |> Integer.parse() |> elem(0)

  defp get_team_home_away_status(team_stats, {home_name, away_name}) do
    home_id =
      team_stats
      |> Enum.find(fn team -> team["TEAM_ABBREVIATION"] == home_name end)
      |> Map.get("TEAM_ID")

    away_id =
      team_stats
      |> Enum.find(fn team -> team["TEAM_ABBREVIATION"] != home_name end)
      |> Map.get("TEAM_ID")

    [{home_id, home_name}, {away_id, away_name}]
  end

  defp home_and_away_teams_from_filepath(filepath) do
    String.split(filepath, "/")
    |> List.last()
    |> String.split("-")
    |> List.last()
    |> String.split(".")
    |> List.first()
    |> String.split_at(3)
  end

  defp not_an_all_star_game(game) do
    team_city = Map.get(game, "TeamStats")
      |> List.first()
      |> Map.get("TEAM_CITY")
    
    team_city != "Team"
  end
end