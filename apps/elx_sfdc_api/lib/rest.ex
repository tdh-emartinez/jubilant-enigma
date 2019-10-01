defmodule ElxSfdcApi.Rest do
  def fetch_sfdc_objects(metadata_params) do
    data = fetch_from_rest_api metadata_params, "/services/data/v43.0/sobjects"
    data["sobjects"]
  end
  def list_query_fields(metadata_params, object_name) do
    sobject_fields = describe_sfdc_object(metadata_params, object_name)["fields"]

    query_fields = sobject_fields
                   |> Enum.reject(&(&1["type"] == "address"))
                   |> Stream.filter(&(&1["calculated"] == false))
                   |> Enum.reduce(
                        [],
                        fn field, acc ->
                          case field["type"] do
                            "reference" ->
                              if "Account" in field["referenceTo"] and field["polymorphicForeignKey"] == false do
                                [(field["relationshipName"] <> ".GUID__c"), field["relationshipName"] <> ".Id"] ++ acc
                              else
                                if field["relationshipName"] == nil do
                                  [field["name"] | acc]
                                else
                                  [(field["relationshipName"] <> ".Id") | acc]
                                end
                              end
                            _ ->
                              [field["name"] | acc]
                          end
                        end
                      )

    soql_statement = query_fields
                     |> Enum.reverse()
                     |> Enum.join(", ")

    ref_fields = query_fields
                 |> Enum.filter(&String.contains?(&1, ".Id"))
                 |> Enum.reduce(
                      [],
                      fn field, acc ->
                        [field | acc]
                      end
                    )
    {soql_statement, ref_fields}
  end
  def describe_sfdc_object(metadata_params, object_name) do
    fetch_from_rest_api(metadata_params, "/services/data/v43.0/sobjects/#{object_name}/describe")
  end
  defp fetch_from_rest_api(metadata_params, uri) do
    access_info = metadata_params.access_info
    headers = ["Authorization": "Bearer #{access_info.access_token}", "Accept": "Application/json; Charset=utf-8"]
    {:ok, res} = HTTPoison.get("#{access_info.instance_url}#{uri}", headers, [timeout: :infinity, recv_timeout: :infinity])
    Poison.decode!(res.body)
  end
end
