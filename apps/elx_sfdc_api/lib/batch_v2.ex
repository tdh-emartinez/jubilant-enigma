defmodule ElxSfdcApi.BatchV2 do
  @wait_time  5000

  def submit_upsert_file_job(instance_name, operation, object_name, external_id, csv_file_name) do
    instance_config = GenServer.call(String.to_atom(instance_name), :config)
    access_info = instance_config.access_info
    dst_data_prefix = "#{Application.get_env(:sfdc_api, :base_dir)}/#{instance_name}/acecsv/"
                      |> Path.expand(__DIR__)

    create_result = create_upsert_job(access_info, operation, object_name, external_id)

    job_id = create_result["id"]
    content_url = create_result["contentUrl"]
    upload_csv_from_file(access_info, content_url, dst_data_prefix <> csv_file_name)
    close_job(access_info, job_id)

    final_result = wait_for_job(access_info, job_id, false)
    unless final_result["numberRecordsFailed"] == "0" do
      download_failed_job_rows(access_info, job_id, dst_data_prefix <> "/failed_#{job_id}.csv.zip")
    end
    unless final_result["numberRecordsProcessed"] == "0" do
      download_valid_job_rows(access_info, job_id, dst_data_prefix <> "/valid_#{job_id}.csv.zip")
    end
    IO.inspect final_result
  end
  defp upload_csv_from_file(access_info, content_url, csv_file_name) do
    {:ok, csv_data} = File.read(csv_file_name)

    IO.puts "csv upload url: #{content_url}"
    {_, response} = HTTPoison.request(
      :put,
      "#{access_info.instance_url}/#{content_url}",
      csv_data,
      generate_headers(access_info, "text/csv")
    )
    response.status_code
  end
  def submit_upsert_job(instance_name, operation, object_name, external_id, csv_data) do

    instance_config = GenServer.call(String.to_atom(instance_name), :config)
    access_info = instance_config.access_info
    dst_data_prefix = "#{Application.get_env(:sfdc_api, :base_dir)}/#{instance_name}/acecsv/"
                      |> Path.expand(__DIR__)

    create_result = create_upsert_job(access_info, operation, object_name, external_id)

    job_id = create_result["id"]
    upload_csv(access_info, job_id, csv_data)
    close_job(access_info, job_id)

    final_result = wait_for_job(access_info, job_id, false)
    unless final_result["numberRecordsFailed"] == "0" do
      download_failed_job_rows(access_info, job_id, dst_data_prefix <> "/failed_#{job_id}.csv.zip")
    end
    unless final_result["numberRecordsProcessed"] == "0" do
      download_valid_job_rows(access_info, job_id, dst_data_prefix <> "/valid_#{job_id}.csv.zip")
    end
    IO.inspect final_result
  end

  def submit_job(instance_name, operation, object_name, csv_data) do
    instance_config = GenServer.call(String.to_atom(instance_name), :config)
    access_info = instance_config.access_info
    create_result = create_job(access_info, operation, object_name)
    dst_data_prefix = "#{Application.get_env(:sfdc_api, :base_dir)}/#{instance_name}/acecsv/"
                      |> Path.expand(__DIR__)
    job_id = create_result["id"]
    upload_csv(access_info, job_id, csv_data)
    close_job(access_info, job_id)

    final_result = wait_for_job(access_info, job_id, false)
    unless final_result["numberRecordsFailed"] == "0" do
      download_failed_job_rows(access_info, job_id, dst_data_prefix <> "/failed_#{job_id}.csv.zip")
    end
    unless final_result["numberRecordsProcessed"] == "0" do
      download_valid_job_rows(access_info, job_id, dst_data_prefix <> "/valid_#{job_id}.csv.zip")
    end
    IO.inspect final_result
  end

  defp download_valid_job_rows(access_info, job_id, file_name) do
    IO.puts "content file_name: #{file_name}"

    is_zip = String.ends_with?(file_name, ".zip")
    fetch_header = ["Authorization": "Bearer #{access_info.access_token}"]
    fetch_header = if is_zip do
      fetch_header ++ ["Accept-Encoding": "gzip"]
    else
      fetch_header ++ ["Content-Type": "text/csv; charset=UTF-8"]
    end

    %HTTPoison.Response{body: body} = HTTPoison.get!(
      "#{generate_uri(access_info)}/ingest/#{job_id}/successfulResults/",
      fetch_header,
      [timeout: :infinity, recv_timeout: :infinity]
    )

    File.write!(file_name, body)
  end
  defp download_failed_job_rows(access_info, job_id, file_name) do
    IO.puts "content file_name: #{file_name}"

    is_zip = String.ends_with?(file_name, ".zip")
    fetch_header = ["Authorization": "Bearer #{access_info.access_token}"]
    fetch_header = if is_zip do
      fetch_header ++ ["Accept-Encoding": "gzip"]
    else
      fetch_header ++ ["Content-Type": "text/csv; charset=UTF-8"]
    end

    %HTTPoison.Response{body: body} = HTTPoison.get!(
      "#{generate_uri(access_info)}/ingest/#{job_id}/failedResults/",
      fetch_header,
      [timeout: :infinity, recv_timeout: :infinity]
    )

    File.write!(file_name, body)
  end
  defp check_job_status(access_info, job_id) do
    {_, res} = HTTPoison.get("#{generate_uri(access_info)}/ingest/#{job_id}", generate_headers(access_info))
    Poison.decode!(res.body)
  end
  defp wait_for_job(access_info, job_id, finished) when finished == true do
    {_, res} = HTTPoison.get(
      "#{generate_uri(access_info)}/ingest/#{job_id}",
      generate_headers(access_info)
    )
    Poison.decode!(res.body)
  end

  defp wait_for_job(access_info, job_id, _finished) do
    job_status = check_job_status(access_info, job_id)
    case job_status["state"] do
      "JobComplete" -> wait_for_job(access_info, job_id, true)
      "Failed" -> wait_for_job(access_info, job_id, true)
      _ ->
        :timer.sleep(@wait_time)
        wait_for_job(access_info, job_id, false)
    end
  end

  defp upload_csv(access_info, job_info, csv_data) do
    {_, response} = HTTPoison.put(
      generate_uri(access_info) <> "/ingest/#{job_info}/batches",
      csv_data,
      generate_headers(access_info, "text/csv")
    )
    response.status_code
  end

  defp create_upsert_job(access_info, operation, object_name, external_id) do
    manifest = %{
      "operation" => operation,
      "object" => object_name,
      "externalIdFieldName" => external_id,
      "lineEnding" => "LF",
      "contentType" => "CSV"
    }

    {_, result} = HTTPoison.post(
      "#{generate_uri(access_info)}/ingest/",
      Poison.encode!(manifest),
      generate_headers(access_info)
    )
    Poison.decode! result.body
  end
  defp create_job(access_info, operation, object_name) do
    manifest = %{
      "operation" => operation,
      "object" => object_name,
      "lineEnding" => "LF",
      "contentType" => "CSV"
    }

    {_, result} = HTTPoison.post(
      "#{generate_uri(access_info)}/ingest/",
      Poison.encode!(manifest),
      generate_headers(access_info)
    )
    Poison.decode! result.body
  end

  def close_job(access_info, job_id, state \\ "UploadComplete") do
    manifest = %{
      "state" => state
    }

    {_, result} = HTTPoison.patch(
      "#{generate_uri(access_info)}/ingest/#{job_id}",
      Poison.encode!(manifest),
      generate_headers(access_info)
    )
    Poison.decode! result.body
  end
  defp generate_uri(access_info) do
    "#{access_info.instance_url}/services/data/v43.0/jobs"
  end
  defp generate_headers(access_info, mime_type \\ "application/json; charset=UTF-8") do
    ["Authorization": "Bearer #{access_info.access_token}", "Content-Type": "#{mime_type}"]
  end
end
