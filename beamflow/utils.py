from fastavro import parse_schema
import pyarrow.parquet
from apache_beam.io.gcp import bigquery
import json

from fastavro.schema import load_schema

data_type_mapping = {
    'STRING': pyarrow.string(),
    'BYTES': pyarrow.string(),
    'INTEGER': pyarrow.int64(),
    'FLOAT': pyarrow.float64(),
    'LONG': pyarrow.int64(),
    'BOOLEAN': pyarrow.bool_(),
    'TIMESTAMP': pyarrow.timestamp(unit='s'),
    'DATE': pyarrow.date64(),
    'DATETIME': pyarrow.timestamp(unit='s'),
    # 'ARRAY': pyarrow.list_(),
    # 'RECORD': pyarrow.dictionary()
}


def readAvroSchema(file):
    avro_schema = load_schema(file)
    parsed_schema = parse_schema(avro_schema)
    return parsed_schema


def readParquetSchema(file):
    parquet_schema = pyarrow.parquet.read_schema(file)
    return parquet_schema


def get_bq_parquet_schema(project, dataset, table):
    '''Return parquet schema for specified table.'''
    project_id = project
    dataset_id = dataset
    table_id = table
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id, project=project)
    table_ref = dataset_ref.table(table_id)
    table = client.get_table(table_ref)
    parquet_schema = pyarrow.schema([])
    for column in table.schema:
        parquet_schema = parquet_schema.append(
            pyarrow.field(column.name, data_type_mapping[column.field_type]))
    return parquet_schema


def readJson(file):
    jsonContent = json.load(open(file, "r"))
    return jsonContent


def avroToParquetSchemaConverter(avroSchema):
    temp_dict = {}
    fieldList = avroSchema.get("fields")
    for item in fieldList:
        a = [v for k, v in item.items() if k == "name" or k == "type"]
        temp_dict[a[0]] = a[1][1]

    parquet_schema = pyarrow.schema([])
    for k, v in temp_dict.items():
        parquet_schema = parquet_schema.append(
            pyarrow.field(k, data_type_mapping[v.upper()]))
    return parquet_schema
