defmodule BigDataBaller.Persist do
  alias BigDataBaller.AwsService

  @local_bio_stats "spark/parquet/player/player_bio_stats/**/part*"
  @local_box_scores "spark/parquet/**/part*"

  defp file_library, do: Application.get_env(:big_data_baller, :file_library, File)

  def box_score_parquet do
    Path.wildcard(@local_box_scores)
    |> Enum.each(&read_and_upload/1)
  end

  def player_bio_stats do
    Path.wildcard(@local_bio_stats)
    |> Enum.each(&read_and_upload/1)
  end

  defp read_and_upload(filepath) do
    with {:ok, contents} <- file_library().read(filepath),
         s3_path <- get_s3_path(filepath) do
      AwsService.write_s3(contents, s3_path)
    else
      {:error, error} -> {:error, error}
    end
  end

  defp get_s3_path(local_filepath) do
    [_ | s3_dirs] = String.split(local_filepath, "/")
    Enum.join(s3_dirs, "/")
  end
end
