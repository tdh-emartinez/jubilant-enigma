defmodule ElxMetaApi.DbCache do
  @moduledoc false

  def cache_instance_data(instance_info) do
    all_sobjects = ElxSfdcApi.Rest.fetch_sfdc_objects(instance_info)
    all_sobjects = Enum.reduce(
      all_sobjects,
      Map.new(),
      fn dsobj, acc ->
        #if(dsobj["name"] in ["Contact"]) do
          if(dsobj["keyPrefix"] != nil and dsobj["createable"] == true) do
          #IO.inspect dsobj
          Map.put(acc, dsobj["name"], dsobj["keyPrefix"])
        else
          acc
        end
      end
    )

    db_conn = case Postgrex.start_link(
                     hostname: "localhost",
                     database: "hub_data",
                     username: "postgres",
                     password: "postgres"
                   ) do
      {:ok, pid} -> pid
      {:error, message} -> nil
    end
    Enum.each(
      #Enum.reverse(Enum.sort(Map.keys(all_sobjects))),
      Enum.sort(Map.keys(all_sobjects)),
      fn sobject_name ->
        unless sobject_name in [
          "RecordAction",
          "ApexClass",
          "IdeaComment",
          "ContentDocumentLink",
          "ApexPage",
          "EmailTemplate",
          "Document",
          "FeedLike",
          "OutgoingEmail",
          "Task",
          "Email",
          "EmailMessage"
        ] do
          IO.puts sobject_name
          unless sobject_name == "Email" do
            cache_sobject_in_db(sobject_name, instance_info, db_conn)
          else
            cache_obj_seg_in_db(sobject_name, instance_info, db_conn)
          end
        end
      end
    )
  end
  def cache_metadata(instance_info) do
    instance_name = instance_info[:instance_name]
    db_conn = case Postgrex.start_link(
                     hostname: "localhost",
                     database: "hub_data",
                     username: "postgres",
                     password: "postgres"
                   ) do
      {:ok, pid} -> pid
      {:error, _message} -> nil
    end
    exec_ddl(db_conn, ["DROP TABLE IF EXISTS #{instance_name}.keyPrefix;"])
    exec_ddl(
      db_conn,
      ["CREATE UNLOGGED TABLE IF NOT EXISTS #{instance_name}.keyPrefix ( name varchar(255), keyPrefix varchar(3));"]
    )
    sobject_map = cache_key_prefixes(db_conn, instance_info)
    exec_ddl(db_conn, ["DROP TABLE IF EXISTS #{instance_name}.field_metadata;"])
    exec_ddl(
      db_conn,
      [
        "CREATE UNLOGGED TABLE IF NOT EXISTS #{
          instance_name
        }.field_metadata ( sobject text, name text, type text, length int, \"unique\" bool, externalId text, scale int, digits int, caseSensitive bool, precision int, label text, referenceTo text, relationshipName text, nillable bool);"
      ]
    )
    IO.puts "fetching prefix map"
    IO.puts "instance name: #{instance_name}"


    Map.keys(sobject_map)
    |> Stream.each(
         &fetch_field_info(db_conn, instance_info, &1)
       )
    |> Stream.run()
  end

  def cache_sobject_in_db(sobject_name, instance_info, db_conn, criteria \\ "") do
    instance_name = instance_info[:instance_name]
    data_prefix = "#{instance_info[:full_path]}/#{instance_name}/acecsv/"

    obj_info = ElxSfdcApi.Rest.describe_sfdc_object(instance_info, sobject_name)["fields"]

    field_list = obj_info
                 |> Stream.reject(&(&1["type"] == "address"))
                 |> Stream.reject(&(&1["type"] == "base64"))
                 |> Stream.reject(&(&1["calculated"] == true))
                 |> Stream.map(
                      &(
                        %{
                          "sobject" => sobject_name,
                          "name" => &1["name"],
                          "type" => &1["type"],
                          "length" => &1["length"],
                          "unique" => &1["unique"],
                          "externalId" => &1["externalId"],
                          "scale" => &1["scale"],
                          "digits" => &1["digits"],
                          "caseSensitive" => &1["caseSensitive"],
                          "precision" => &1["precision"],
                          "label" => &1["label"],
                          "relationshipName" => &1["relationshipName"],
                          "nillable" => &1["nillable"]
                        }
                        )
                    )
                 |> Enum.reduce(
                      [],
                      fn meta_data, acc ->
                        [meta_data | acc]
                      end
                    )


    query_fields = field_list
                   |> Enum.reduce(
                        [],
                        fn row_data, acc ->
                          [row_data["name"] | acc]
                        end
                      )

    new_query = "Select #{Enum.join(query_fields, ",")} from #{sobject_name} #{criteria}"
    #IO.puts new_query
    dst_acct_qry_hash = ElxSfdcApi.Util.fetch_raw_sobject_data_from(
      sobject_name,
      instance_info,
      new_query
    )

    outbound_file_name = data_prefix <> "/#{sobject_name}_#{dst_acct_qry_hash}_pgcopy.csv"
    {:ok, outbound_pid} = outbound_file_name
                          |> File.open([:write, :utf8])
    transform_file_name = data_prefix <> "/#{sobject_name}_#{dst_acct_qry_hash}.csv.zip"
    if File.exists?(transform_file_name) do
      ElxSfdcApi.Util.stream_zip_from(transform_file_name)
      |> CSV.decode!([headers: true])
      |> CSV.encode([separator: ?~, headers: query_fields])
      |> Stream.each(&IO.write(outbound_pid, &1))
      |> Stream.run()

      File.close(outbound_file_name)

      #		ddl_stmts = generate_ddl_stmt(instance_name, sobject_name, field_list,true)

      ddl_stmts = generate_ddl_stmt(instance_name, sobject_name, field_list)
      ddl_stmts = ddl_stmts ++ [
        "copy #{instance_name}.#{sobject_name} from '#{outbound_file_name}' DELIMITER '~' CSV HEADER;"
      ]

      IO.puts "params going into exec_ddl"
      exec_ddl(db_conn, ddl_stmts)
      #delete_if_empty(db_conn, instance_name, sobject_name)
    end
  end
  def cache_obj_seg_in_db(sobject_name, instance_info, db_conn) do
    instance_name = instance_info[:instance_name]
    data_prefix = "#{instance_info[:full_path]}/#{instance_name}/acecsv/"

    obj_info = ElxSfdcApi.Rest.describe_sfdc_object(instance_info, sobject_name)["fields"]

    field_list = obj_info
                 |> Stream.reject(&(&1["type"] == "address"))
                 |> Stream.reject(&(&1["calculated"] == true))
                 |> Stream.map(
                      &(
                        %{
                          "sobject" => sobject_name,
                          "name" => &1["name"],
                          "type" => &1["type"],
                          "length" => &1["length"],
                          "unique" => &1["unique"],
                          "externalId" => &1["externalId"],
                          "scale" => &1["scale"],
                          "digits" => &1["digits"],
                          "caseSensitive" => &1["caseSensitive"],
                          "precision" => &1["precision"],
                          "label" => &1["label"],
                          "relationshipName" => &1["relationshipName"],
                          "nillable" => &1["nillable"]
                        }
                        )
                    )
                 |> Enum.reduce(
                      [],
                      fn meta_data, acc ->
                        [meta_data | acc]
                      end
                    )


    query_fields = field_list
                   |> Enum.reduce(
                        [],
                        fn row_data, acc ->
                          [row_data["name"] | acc]
                        end
                      )
    # truncate ddl
    trunc_ddl_stmts = generate_ddl_stmt(instance_name, sobject_name, field_list)
    exec_ddl(db_conn, trunc_ddl_stmts)

    # now pull portions of tasks with each iteration
    Enum.each(
      1..12,
      fn x ->
        IO.puts "month: #{x}"
        segmented_dml(db_conn, instance_info, data_prefix, query_fields, sobject_name, " where calendar_month(createddate) = #{x}")
      end
    )
  end
  defp segmented_dml(db_conn, instance_info, data_prefix, query_fields, sobject_name, criteria) do
    instance_name = instance_info[:instance_name]
    new_query = "Select #{Enum.join(query_fields, ",")} from #{sobject_name} #{criteria}"
    #IO.puts new_query
    dst_acct_qry_hash = ElxSfdcApi.Util.fetch_raw_sobject_data_from(
      sobject_name,
      instance_info,
      new_query
    )

    outbound_file_name = data_prefix <> "/#{sobject_name}_#{dst_acct_qry_hash}_pgcopy.csv"
    {:ok, outbound_pid} = outbound_file_name
                          |> File.open([:write, :utf8])
    transform_file_name = data_prefix <> "/#{sobject_name}_#{dst_acct_qry_hash}.csv.zip"
    if File.exists?(transform_file_name) do
      Transform.File.stream_zip_from(transform_file_name)
      |> CSV.decode!([headers: true, escape_max_lines: 10000])
      |> CSV.encode([separator: ?~, headers: query_fields])
      |> Stream.each(&IO.write(outbound_pid, &1))
      |> Stream.run()

      File.close(outbound_file_name)

      ddl_stmts = ["copy #{instance_name}.#{sobject_name} from '#{outbound_file_name}' DELIMITER '~' CSV HEADER;"]

      IO.puts "params going into exec_ddl"
      exec_ddl(db_conn, ddl_stmts)
    end
  end
  defp fetch_field_info(db_conn, instance_info, sobject_name) do
    instance_name = instance_info[:instance_name]
    IO.puts "fetching field info for #{sobject_name}"
    obj_fields = ElxSfdcApi.Rest.describe_sfdc_object(instance_info, sobject_name)["fields"]
    field_list = obj_fields
                 |> Stream.reject(&(&1["type"] == "address"))
                 |> Stream.reject(&(&1["autoNumber"] == true))
                 |> Stream.reject(&(&1["calculated"] == true))
                 |> Stream.map(
                      &(
                        %{
                          "sobject" => sobject_name,
                          "name" => &1["name"],
                          "type" => &1["type"],
                          "length" => &1["length"],
                          "unique" => &1["unique"],
                          "externalId" => &1["externalId"],
                          "scale" => &1["scale"],
                          "digits" => &1["digits"],
                          "caseSensitive" => &1["caseSensitive"],
                          "precision" => &1["precision"],
                          "label" => &1["label"],
                          "nillable" => &1["nillable"],
                          "relationshipName" => &1["relationshipName"],
                          "referenceTo" => Enum.join(&1["referenceTo"], ",")
                        }
                        )
                    )
                 |> Enum.reduce(
                      [],
                      fn meta_data, acc ->
                        [meta_data | acc]
                      end
                    )

    field_list
    |> Stream.each(
         &Postgrex.query!(
           db_conn,
           prepare_insert_stmt(instance_name, sobject_name, &1),
           []
         )
       )
    |> Stream.run()
  end
  defp prepare_insert_stmt(instance_name, sobject_name, field_data) do
    stmt = "INSERT INTO #{
      instance_name
    }.field_metadata (sobject, name, type, length, \"unique\", externalId, scale, digits, caseSensitive, precision, label, referenceTo, relationshipName, nillable) values ('#{
      sobject_name
    }','#{field_data["name"]}','#{field_data["type"]}',#{field_data["length"]}, #{field_data["unique"]}, '#{
      field_data["externalId"]
    }',#{
      field_data["scale"]
    }, #{
      field_data["digits"]
    },#{field_data["caseSensitive"]},#{field_data["precision"]},'#{String.replace(field_data["label"], "'", "_")}','#{
      field_data["referenceTo"]
    }',  '#{field_data["relationshipName"]}','#{field_data["nillable"]}')"
    #IO.puts stmt
    stmt
  end
  defp cache_key_prefixes(db_conn, instance_info) do
    instance_name = instance_info[:instance_name]
    all_sobjects = ElxSfdcApi.Rest.fetch_sfdc_objects(instance_info)
    IO.puts "sfdc objects"
    IO.inspect all_sobjects
    all_sobjects = Enum.reduce(
      all_sobjects,
      Map.new(),
      fn dsobj, acc ->
        if(dsobj["keyPrefix"] != nil and dsobj["createable"] == true) do
          Map.put(acc, dsobj["name"], dsobj["keyPrefix"])
        else
          acc
        end
      end
    )

    Stream.each(
      Enum.sort(Map.keys(all_sobjects)),
      fn obj_key ->
        Postgrex.query!(
          db_conn,
          "INSERT into #{instance_name}.keyPrefix (name, keyPrefix) values ('#{obj_key}', '#{all_sobjects[obj_key]}');",
          []
        );
      end
    )
    |> Stream.run()
    all_sobjects
  end
  def exec_ddl(db_conn, ddl_stmts, params \\ []) do
    Enum.each(
      ddl_stmts,
      fn ddl ->
        #IO.puts ddl
        query = Postgrex.prepare!(db_conn, "", ddl)
        case Postgrex.execute(db_conn, query, params) do
          {:ok, _query, result} -> IO.inspect result
          {:error, error_msg} -> IO.inspect error_msg
        end
      end
    )
  end

  def generate_ddl_stmt(instance_name, sobject_name, field_list, debug \\ false) do
    ddl_fields = field_list
                 |> Enum.reduce(
                      [],
                      fn field_data, acc ->
                        field_ddl = case field_data["type"] do
                          "boolean" ->
                            "#{field_data["name"]} bool"
                          "reference" ->
                            "#{field_data["name"]} varchar(18)"
                          "id" ->
                            "#{field_data["name"]} varchar(18)"
                          "textarea" -> "#{field_data["name"]} text"
                          "picklist" ->
                            "#{field_data["name"]} varchar(255)"
                          value when value in ["string", "multipicklist", "url", "phone", "email"] ->
                            #"#{field_data["name"]} varchar(#{field_data["length"]})"
                            "#{field_data["name"]} text"
                          "int" ->
                            "#{field_data["name"]} integer"
                          "date" ->
                            "#{field_data["name"]} date"
                          "datetime" ->
                            "#{field_data["name"]} timestamptz"
                          value when value in ["double", "currency", "percent"] ->
                            "#{field_data["name"]} numeric"
                          _ -> if debug == true do
                                 IO.inspect field_data
                               end
                               "#{field_data["name"]} text"
                        end
                        [field_ddl | acc]
                      end
                    )

    unless debug == true do
      ddl_stmts = ["DROP TABLE IF EXISTS #{instance_name}.#{sobject_name};"]
      ddl_stmts ++ [
        "CREATE UNLOGGED TABLE IF NOT EXISTS #{instance_name}.#{sobject_name} (#{Enum.join(ddl_fields, ",")});"
      ]
    else
      []
    end
  end

end
