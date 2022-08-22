from apache_beam.io import WriteToParquet, WriteToText, WriteToPubSub, WriteToBigQuery, WriteToAvro

from beamflow.conf import FileConf, PubSubConf, BigQueryConf
from beamflow.utils import *


class ParquietIO(WriteToParquet):
    def __init__(self, sinkConfiguration):
        super().__init__(sinkConfiguration.filepath,
                         avroToParquetSchemaConverter(readAvroSchema(sinkConfiguration.schema)))


class CsvIO(WriteToText):
    def __init__(self, sinkConfiguration):
        super().__init__(sinkConfiguration.filepath)


class JsonIO(WriteToText):
    def __init__(self, sinkConfiguration):
        super().__init__(sinkConfiguration.filepath)


class PubSubIO(WriteToPubSub):
    def __init__(self, sinkConfiguration):
        topic_id = "projects/%s/topics/%s" % (sinkConfiguration.project, sinkConfiguration.topic)
        super().__init__(topic=topic_id)


class BiqQueryIO(WriteToBigQuery):
    def __init__(self, sinkConfiguration):
        super().__init__(sinkConfiguration.filepath)


class AvroIO(WriteToAvro):
    def __init__(self, sinkConfiguration):
        super().__init__(sinkConfiguration.filepath,
                         readAvroSchema(sinkConfiguration.schema))


class Writers:
    @classmethod
    def factoryWriter(cls, sinkConfiguration):
        if isinstance(sinkConfiguration, FileConf):
            if (sinkConfiguration.filepath.endswith("csv")):
                return CsvIO(sinkConfiguration)
            elif (sinkConfiguration.filepath.endswith("json")):
                return JsonIO(sinkConfiguration)
            elif (sinkConfiguration.filepath.endswith("parquet")):
                return ParquietIO(sinkConfiguration)
            elif (sinkConfiguration.filepath.endswith("avro")):
                return AvroIO(sinkConfiguration)
            else:
                return WriteToText(sinkConfiguration.filepath)
        if isinstance(sinkConfiguration, PubSubConf):
            return PubSubIO(sinkConfiguration)
        if isinstance(sinkConfiguration, BigQueryConf):
            return BiqQueryIO(sinkConfiguration)
