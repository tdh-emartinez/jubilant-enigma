defmodule SfdcAuthJwt.LoginResult do
  @derive [Poison.Encoder]
  defstruct [:access_token, :scope, :instance_url, :id, :token_type]
end
