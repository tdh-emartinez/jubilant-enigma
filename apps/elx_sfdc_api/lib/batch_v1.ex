defmodule ElxSfdcApi.BatchV1 do
  @wait_time  5000

  def create_batch_job(batch_params) do
    #instance_config = GenServer.call(String.to_atom(instance_name), :config)
    access_info = batch_params.access_info
    file_name = "#{batch_params.full_path}/#{batch_params.instance_name}/acecsv/#{batch_params[:sobject]}#{
      batch_params[:zip_annotation]
    }.csv.zip"

    job_info = post_bulk_job(access_info, batch_params[:sobject])
    add_batch_to_job(
      access_info,
      job_info["id"],
      batch_params[:soql]
    )
    close_bulk_job(access_info, job_info["id"])

    job_results = wait_for_job(access_info, job_info["id"], false)
    if job_results["state"] == "Completed" do
      batch_results = if is_map(job_results) do
        [%{batch_id: job_results["id"], job_id: job_results["jobId"]}]
      else
        Enum.reduce(job_results, [], fn row, acc -> [%{batch_id: row["id"], job_id: row["jobId"]} | acc] end)
      end
      content_info = Enum.map(
        batch_results,
        &(
          Map.put_new(
            &1,
            :result_id,
            fetch_batch_result(access_info, &1.job_id, &1.batch_id)
            |> Map.fetch!("result")
          ))
      )
      fetch_result_content(
        content_info,
        access_info,
        file_name
        |> Path.expand(__DIR__)
      )
    else
      IO.puts "Batch failed"
      IO.inspect job_results
    end

  end

  defp wait_for_job(access_info, job_id, finished) when finished == true do
    {_, res} = HTTPoison.get(
      "#{generate_uri(access_info)}/#{job_id}/batch",
      generate_headers(access_info)
    )
    XmlToMap.naive_map(res.body)
    |> Map.fetch!("batchInfoList")
    |> Map.fetch!("batchInfo")
  end
  defp wait_for_job(access_info, job_id, _finished) do
    job_status = check_job_status(access_info, job_id)
    if(
      String.to_integer(job_status["numberBatchesCompleted"]) + String.to_integer(
        job_status["numberBatchesFailed"]
      ) == String.to_integer(job_status["numberBatchesTotal"])
    ) do
      wait_for_job(access_info, job_id, true)
    else
      :timer.sleep(@wait_time)
      wait_for_job(access_info, job_id, false)
    end
  end
  defp fetch_result_content(content_info, access_info, file_name) do
    IO.puts "content file_name: #{file_name}"
    is_zip = String.ends_with?(file_name, ".zip")
    fetch_header = ["X-SFDC-Session": access_info.access_token]
    fetch_header = if is_zip do
      fetch_header ++ ["Accept-Encoding": "gzip"]
    else
      fetch_header ++ ["Content-Type": "text/csv; charset=UTF-8"]
    end

    Enum.each(
      content_info,
      fn content_item ->
        %HTTPoison.Response{body: body} = HTTPoison.get!(
          "#{generate_uri(access_info)}/#{content_item.job_id}/batch/#{content_item.batch_id}/result/#{
            content_item.result_id
          }",
          fetch_header,
          [timeout: :infinity, recv_timeout: :infinity]
        )

        File.write!(file_name, body)
      end
    )
    :ok
  end

  defp fetch_batch_result(access_info, job_id, batch_id) do
    {_, res} = HTTPoison.get(
      "#{generate_uri(access_info)}/#{job_id}/batch/#{batch_id}/result",
      generate_headers(access_info),
      [timeout: :infinity, recv_timeout: :infinity]
    )
    XmlToMap.naive_map(res.body)
    |> Map.fetch!("result-list")
  end
  #	defp check_batch_status(access_info, job_id, batch_id) do
  #		{_, res} = HTTPoison.get(
  #			"#{generate_uri(access_info)}/#{job_id}/batch/#{batch_id}",
  #			generate_headers(access_info)
  #		)
  #		XmlToMap.naive_map(res.body)
  #		|> Map.fetch!("batchInfo")
  #	end
  defp check_job_status(access_info, job_id) do
    {_, res} = HTTPoison.get("#{generate_uri(access_info)}/#{job_id}",
      generate_headers(access_info),
      [timeout: :infinity, recv_timeout: :infinity])
    XmlToMap.naive_map(res.body)
    |> Map.fetch!("jobInfo")
  end
  defp add_batch_to_job(access_info, job_id, query) do
    {_, res} = HTTPoison.post(
      "#{generate_uri(access_info)}/#{job_id}/batch",
      query,
      generate_headers(access_info, "text/csv")
    )
    XmlToMap.naive_map(res.body)
    |> Map.fetch!("batchInfo")
  end
  defp close_bulk_job(access_info, job_id) do
    {_, res} = HTTPoison.post(
      "#{generate_uri(access_info)}/#{job_id}",
      generate_job_close(),
      generate_headers(access_info)
    )
    XmlToMap.naive_map(res.body)
    |> Map.fetch!("jobInfo")
  end

  defp post_bulk_job(access_info, object_name) do
    {_, res} = HTTPoison.post(
      "#{generate_uri(access_info)}",
      generate_job_manifest(object_name),
      generate_headers(access_info)
    )
    XmlToMap.naive_map(res.body)
    |> Map.fetch!("jobInfo")
  end

  defp generate_job_manifest(object_name, concurrency_mode \\ "Parallel", content_type \\ "CSV") do
    XmlBuilder.document(
      "jobInfo",
      %{xmlns: "http://www.force.com/2009/06/asyncapi/dataload"},
      [
        {"operation", nil, "query"},
        {"object", nil, object_name},
        {"concurrencyMode", nil, concurrency_mode},
        {"contentType", nil, content_type}
      ]
    )
    |> XmlBuilder.generate(format: :none)
  end
  defp generate_job_close do
    XmlBuilder.document(
      "jobInfo",
      %{xmlns: "http://www.force.com/2009/06/asyncapi/dataload"},
      [
        {"state", nil, "Closed"},
      ]
    )
    |> XmlBuilder.generate(format: :none)
  end
  defp generate_uri(access_info) do
    "#{access_info.instance_url}/services/async/43.0/job"
  end
  defp generate_headers(access_info, mime_type \\ "application/xml") do
    ["X-SFDC-Session": access_info.access_token, "Content-Type": "#{mime_type}; charset=UTF-8"]
  end
end
