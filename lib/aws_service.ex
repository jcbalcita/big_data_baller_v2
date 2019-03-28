defmodule BigDataBaller.AwsService do
  @date_directory_format "{YYYY}/{0M}/{0D}"

  defp aws_library, do: Application.get_env(:big_data_baller, :aws_library, ExAws)
  defp s3_library, do: Application.get_env(:big_data_baller, :s3_library, ExAws.S3)

  def creds? do
    if Application.get_env(:ex_aws, :access_key_id) &&
         Application.get_env(:ex_aws, :secret_access_key) do
      {:ok, "Creds in environment"}
    else
      {:error, "No AWS creds in enviroment!"}
    end
  end

  def write_s3(data, path), do: write_s3(data, s3_bucket(), path)

  def write_s3(data, bucket_name, path) do
    s3_library().put_object(bucket_name, path, data)
    |> aws_library().request()
  end

  def s3_bucket, do: "nba-box-scores-s3"
end
