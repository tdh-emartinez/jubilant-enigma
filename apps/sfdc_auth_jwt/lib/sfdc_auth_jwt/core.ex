defmodule SfdcAuthJwt.Core do
  @moduledoc false

  def establish_config(config_path, instance_name) do
    full_path = config_path
                |> Path.expand(__DIR__)

    full_path <> "/config/#{instance_name}_config.json"
    |> File.read!
    |> Poison.decode!
    |> Map.put("full_path", full_path)
    |> get_sfdc_access
  end
  defp get_sfdc_access (instance_config) do
    config_name = instance_config["name"]
    tmp_exp = DateTime.utc_now
              |> DateTime.to_unix

    aud = case instance_config["production"] do
      false -> "https://test.salesforce.com"
      true -> "https://login.salesforce.com"
    end

    exp = "#{tmp_exp + 3600}"
    token_config = %{}
                   |> Joken.Config.add_claim("aud", fn -> aud end, nil)
                   |> Joken.Config.add_claim("iss", fn -> instance_config["iss"] end, nil)
                   |> Joken.Config.add_claim("sub", fn -> instance_config["sub"] end, nil)
                   |> Joken.Config.add_claim("exp", fn -> exp end, nil)

    rsa_pem = File.read!(instance_config["full_path"] <> "/config/sfdc_server.key")
    signed_token = Joken.generate_and_sign!(token_config, nil, Joken.Signer.create("RS256", %{"pem" => rsa_pem}))

    # switch maps to use atoms
    enc_params = URI.encode_query(
      %{grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer", assertion: signed_token}
    )
    uri = "#{aud}/services/oauth2/token?#{enc_params}"

    {_, res} = HTTPoison.post(uri, [])
    #IO.inspect(res)
    login_result = Poison.decode(res.body, as: %SfdcAuthJwt.LoginResult{})

    case login_result do
      {:ok, token_info} ->
        %{
          access_info: token_info,
          instance_name: config_name,
          refresh_after: exp,
          full_path: instance_config["full_path"]
        }
      _ ->
        {:error, login_result}
    end
  end
end
