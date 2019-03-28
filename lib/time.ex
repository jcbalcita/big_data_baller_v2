defmodule BigDataBaller.Time do
  alias BigDataBaller.Category

  def tuple_to_datetime(date_tuple) do
    case Timex.to_datetime(date_tuple) do
      {:error, message} -> {:error, message}
      {:ok, datetime} -> {:ok, datetime}
    end
  end

  def step(%{interval: :year, current_datetime: datetime} = category) do
    %Category{category | current_datetime: datetime + 1}
  end

  def step(%{interval: :day, current_datetime: datetime} = category) do
    plus_one_day = Timex.add(datetime, Timex.Duration.from_days(1))
    %Category{category | current_datetime: plus_one_day}
  end

  def complete?(%{interval: :year, current_datetime: current_year, end_datetime: end_year}) do
    current_year > end_year
  end

  def complete?(%{interval: :day, current_datetime: current_datetime, end_datetime: end_datetime}) do
    Timex.after?(current_datetime, end_datetime)
  end

  def get_year(%{interval: :day, current_datetime: dt}), do: Timex.format(dt, "YYYY")
end
