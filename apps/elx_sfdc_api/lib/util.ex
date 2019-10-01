defmodule ElxSfdcApi.Util do
  @moduledoc false
  def stream_zip_from(file_name) do
    file_name
    |> Path.expand(__DIR__)
    |> File.open!([:read, :compressed])
    |> IO.binstream(:line)

  end

  def fix_header_on_file(file_name) do
    dst_file_name = file_name
                    |> Path.expand(__DIR__)
    src_file_name = String.replace(file_name, ".csv", "_raw.csv")
                    |> Path.expand(__DIR__)

    File.cp!(dst_file_name, src_file_name)
    dst_pid = File.open!(dst_file_name, [:write, :utf8])

    File.stream!(src_file_name)
    |> Stream.take(1)
    |> Stream.each(&IO.write(dst_pid, String.replace(&1,".","::")))
    |> Stream.run()

    File.stream!(src_file_name)
    |> Stream.drop(1)
    |> Stream.each(&IO.write(dst_pid, &1))
    |> Stream.run()

    File.close(dst_pid)

    File.rm!(src_file_name)
  end
  def fetch_key_prefixes(instance_info) do
    instance_name = instance_info[:instance_name]
    prefix_csv_path = "#{instance_info[:full_path]}/#{instance_name}/acecsv/KeyPrefix.csv"

    prefix_map = unless File.exists?(prefix_csv_path) do
      sfdc_object_map = ElxSfdcApi.Rest.fetch_sfdc_objects(instance_info)
                        |> Enum.reduce(
                             Map.new(),
                             fn object_data, acc ->
                               unless object_data["keyPrefix"] == "" do
                                 Map.put(acc, object_data["keyPrefix"], object_data["name"])
                               else
                                 acc
                               end
                             end
                           )
      {:ok, prefix_csv_file} = File.open(prefix_csv_path, [:write, :utf8])

      IO.write(prefix_csv_file, "keyPrefix,name\n")
      Enum.each(
        Map.keys(sfdc_object_map),
        fn key ->
          IO.write(prefix_csv_file, "#{key},#{sfdc_object_map[key]}\n")
        end
      )

      File.close(prefix_csv_file)
      sfdc_object_map
    else
      prefix_csv_path
      |> File.stream!
      |> CSV.decode!([headers: true, strip_fields: true])
      |> Enum.reduce(
           Map.new(),
           fn row, acc ->
             Map.put(acc, row["keyPrefix"], row["name"])
           end
         )
    end
    prefix_map
  end

  def gather_row_ids(row_data, ref_fields) do
    Enum.reduce(
      ref_fields,
      MapSet.new(),
      fn field, acc ->
        unless row_data[field] == "" do
          MapSet.put(acc, row_data[field])
        else
          acc
        end
      end
    )
  end

  def fetch_raw_sobject_data_from(sobject_name, instance_info, soql_statement \\ "") when soql_statement != "" do
    instance_name = instance_info[:instance_name]
    query_hash = Base.encode16(:erlang.md5(String.downcase(soql_statement)), case: :lower)
    batch_params = %{
      sobject: sobject_name,
      soql: soql_statement,
      zip_annotation: "_#{query_hash}"
    }
    file_name = "#{instance_info[:full_path]}/#{instance_name}/acecsv/#{batch_params[:sobject]}#{
      batch_params[:zip_annotation]
    }.csv.zip"

    unless File.exists?(file_name) do
      IO.puts "fetching fresh data for #{file_name}"
      ElxSfdcApi.BatchV1.create_batch_job(Map.merge(instance_info, batch_params))
    else
      IO.puts "using cached data for #{file_name}"
    end
    query_hash
  end

  def fetch_sobject_data_from(sobject_name, instance_info, criteria \\ "")  do
    instance_name = instance_info[:instance_name]
    {soql_statement, ref_fields} = ElxSfdcApi.Rest.list_query_fields(instance_info, sobject_name)
    batch_params = %{
      sobject: sobject_name,
      soql: "SELECT " <> soql_statement <> " FROM #{
        sobject_name
      } " <> criteria,
      zip_annotation: "_#{instance_name}"
    }
    file_name = "#{instance_info[:full_path]}/#{instance_name}/acecsv/#{batch_params[:sobject]}#{
      batch_params[:zip_annotation]
    }.csv.zip"

    unless File.exists?(file_name) do
      IO.puts "fetching fresh data for #{file_name}"
      ElxSfdcApi.BatchV1.create_batch_job(Map.merge(instance_info,batch_params))
      File.write!(
        "#{instance_info[:full_path]}/#{instance_name}/acecsv/#{sobject_name}_ref_fields.csv",
        ref_fields
        |> Enum.join(",")
      )
    else
      IO.puts "using cached data for #{file_name}"
    end
    ref_fields
  end
end
