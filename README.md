##### **pyBeamFlow** is an ETL tool based on apache beam

Easy configurable from application.ini file

**Input sources supported:**

File supported file formats : parquet, json, csv, etc
PubSub
BigQuery
RestAPI

**Output sources supported:**

File supported file formats : parquet, json, csv, etc
BigQuery
PubSub


**Application.ini :**

a) Job = dataflow parameters

b) Input = input part

c) Sink = writing part

d) Transformations = mapping, filter : beam.Map or beam.Filter.  
It reads lambda functions in text format from application.ini file

Notes:
1. Writing to parquet files, an avro schema must be provided
2. Job supports different runners : DirectRunner, DataflowRunner
3. For mapping and filtering (PCollection at row level), it can be used lambda functions or just function_name if it is already defined in business_rules.py files
see : templates/application_template_gs_gs.ini

**Installation**:  
`pip install -r requiments.txt `  
please note you can also have to install  
`pip install apache_beam[gcp] `