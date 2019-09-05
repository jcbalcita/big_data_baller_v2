defmodule BigDataBaller.Util do
  def season_year_suffix(season_start_year) when is_integer(season_start_year) do
    (season_start_year + 1)
    |> Integer.to_string()
    |> String.slice(-2..-1)
  end

  def season_year_suffix(season_start_year) when is_binary(season_start_year) do
    maybe_parsed = Integer.parse(season_start_year)

    case maybe_parsed do
      {num, _} -> season_year_suffix(num)
      :error -> raise %RuntimeError{message: "Unable to parse #{season_start_year} as an integer"}
    end
  end

  def querystring_season(start_year) do
    "#{start_year}" <> "-" <> season_year_suffix(start_year)
  end
end
