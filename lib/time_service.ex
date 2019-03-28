defmodule BigDataBaller.TimeService do
  alias BigDataBaller.Category

  def time_library, do: Application.get_env(:big_data_baller, :time_library, Timex)
  defp duration, do: Application.get_env(:big_data_baller, :duration, Timex.Duration)

  def tuple_to_datetime(date_tuple) do
    case time_library().to_datetime(date_tuple) do
      {:error, message} -> {:error, message}
      datetime -> {:ok, datetime}
    end
  end

  def step(%{interval: :year, current_datetime: datetime} = category) do
    %Category{category | current_datetime: datetime + 1}
  end

  def step(%{interval: :day, current_datetime: datetime} = category) do
    %Category{category | current_datetime: tomorrow(datetime)}
  end

  def complete?(%{interval: :year, current_datetime: current_year, end_datetime: end_year}) do
    current_year > end_year
  end

  def complete?(%{interval: :day, current_datetime: current_datetime, end_datetime: end_datetime}) do
    time_library().after?(current_datetime, end_datetime)
  end

  def get_year(%{interval: :day, current_datetime: dt}), do: time_library().format(dt, "{YYYY}")

  def yesterday(dt) do
    one_day = duration().from_days(1)
    time_library().subtract(dt, one_day)
  end

  def tomorrow(dt) do
    time_library().add(dt, duration().from_days(1))
  end
end
