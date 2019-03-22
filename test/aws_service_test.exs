defmodule BigDataBaller.AwsServiceTest do
  use ExUnit.Case
  alias BigDataBaller.AwsService
  doctest AwsService

  test "creds? returns false if aws_creds are not in env" do
    # given
    Application.delete_env(:ex_aws, :access_key_id)
    Application.delete_env(:ex_aws, :secret_access_key)
    expected = :aws_error
    # when
    result = AwsService.creds?
    # then
    assert result == expected
  end

  test "creds? returns true if aws_creds are in env" do
    # given
    Application.put_env(:ex_aws, :access_key_id, "something")
    Application.put_env(:ex_aws, :secret_access_key, "something")
    expected = :ok
    # when
    result = AwsService.creds?
    # then
    assert result == expected
  end
end
