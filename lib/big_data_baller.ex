defmodule BigDataBaller do
  def json_library, do: Application.get_env(:big_data_baller, :json_library, Jason)
end
