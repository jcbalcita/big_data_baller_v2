defmodule BigDataBaller.AwsService do
  @date_directory_format "{YYYY}/{0M}/{0D}"
  @aws Application.get_env(:big_data_baller, :aws_library, ExAws)

  def creds? do
    if Application.get_env(:ex_aws, :access_key_id) &&
        Application.get_env(:ex_aws, :secret_access_key) do
      {:ok, "Creds in environment"}
    else
      {:error, "No AWS creds in enviroment!"}
    end
  end

  def write_s3(bucket_name, path, data) do
    @aws.S3.put_object(bucket_name, path, data)
    |> @aws.request()
  end

  def s3_bucket, do: "nba-box-scores-s3"
end