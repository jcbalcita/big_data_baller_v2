defmodule BigDataBaller.Util do
  def season_year_suffix(season_start_year) when is_integer(season_start_year) do
    (season_start_year + 1)
    |> Integer.to_string()
    |> String.slice(-2..-1)
  end

  def season_year_suffix(season_start_year) when is_binary(season_start_year) do
    (elem(Integer.parse(season_start_year), 0))
    |> season_year_suffix()
  end

  def querystring_season(start_year) do
    "#{start_year}" <> "-" <> season_year_suffix(start_year)
  end
end