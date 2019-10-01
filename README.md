# ElxSfdc

Elixir to Salesforce API

Terms used:<br>
config_path: - relative root directory for data/config (e.g. ~/etldata)
instance_name: - named used in .json config file (e.g. hub_preprod)

JWT config file format (place in <config_path>/config/<instance_name>_config.json):<br>
`{"sub":<salesforce-username for target instance>,
"production":<false indicates test instance>,"name":<instance name used throughout API>,"iss":<clientid of app>}
`

Under <config_path> the following structure must exist for downloads:<br>
<instance_name><br>
|----/acecsv<br>
|----/vision<br>


