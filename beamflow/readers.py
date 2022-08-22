import logging

import requests
from apache_beam.io import ReadFromParquet, ReadFromText, ReadFromPubSub, ReadFromBigQuery, ReadFromAvro
from apache_beam import DoFn, PTransform, Create, ParDo

from beamflow.conf import FileConf, PubSubConf, BigQueryConf, RestAPIConf, InputConfiguration


class ParquietIO(ReadFromParquet):
    def __init__(self, inputConfiguration):
        super().__init__(inputConfiguration.filepath)
        logging.info(f"received {inputConfiguration.filepath}")


class CsvIO(ReadFromText):
    def __init__(self, inputConfiguration):
        super().__init__(inputConfiguration.filepath)
        logging.info(f"received {inputConfiguration.filepath}")


class JsonIO(ReadFromText):
    def __init__(self, inputConfiguration):
        super().__init__(inputConfiguration.filepath)
        logging.info(f"received {inputConfiguration.filepath}")


class PubSubIO(ReadFromPubSub):
    def __init__(self, inputConfiguration):
        topic_id = "projects/%s/topics/%s" % (inputConfiguration.project, inputConfiguration.topic)
        super().__init__(topic=topic_id)
        logging.info(f"received {inputConfiguration.topic}")


class BiqQueryIO(ReadFromBigQuery):
    def __init__(self, inputConfiguration):
        table_id = '[%s:%s.%s]' % (inputConfiguration.project,
                                   inputConfiguration.dataset,
                                   inputConfiguration.table)
        self.query = inputConfiguration.sql.replace(inputConfiguration.table, table_id)

        super().__init__(query=self.query)
        logging.info(f"received {inputConfiguration}")


class AvroIO(ReadFromAvro):
    def __init__(self, inputConfiguration):
        super().__init__(inputConfiguration.filepath)
        logging.info(f"received {inputConfiguration.filepath}")


class RestAPI(PTransform):
    def __init__(self, inputConfiguration, *args, **kwargs):
        """Initializes ``RestAPI``
        """
        super(RestAPI, self).__init__(*args, **kwargs)
        self.inputConfiguration = inputConfiguration

    def expand(self, pcoll):
        return (
                pcoll
                | Create(["start"])
                | ParDo(_ConsumeApi(self.inputConfiguration.url))
        )


class _ConsumeApi(DoFn):
    def __init__(self, url):
        logging.debug(f"fetching api data from {url}")
        self.url = url

    def process(self, url):
        api_url = self.url
        logging.debug("Now fetching from ", api_url)
        response = requests.get(api_url)
        return list(response.json())


class Readers:
    @classmethod
    def factoryReader(cls, inputConfiguration: InputConfiguration):
        if isinstance(inputConfiguration, FileConf):
            if (inputConfiguration.filepath.endswith("csv")):
                return CsvIO(inputConfiguration)
            elif (inputConfiguration.filepath.endswith("json")):
                return JsonIO(inputConfiguration)
            elif (inputConfiguration.filepath.endswith("parquet")):
                return ParquietIO(inputConfiguration)
            elif (inputConfiguration.filepath.endswith("avro")):
                return AvroIO(inputConfiguration)
            else:
                return ReadFromText(inputConfiguration.filepath)
        elif isinstance(inputConfiguration, PubSubConf):
            return PubSubIO(inputConfiguration)
        elif isinstance(inputConfiguration, BigQueryConf):
            return BiqQueryIO(inputConfiguration)
        elif isinstance(inputConfiguration, RestAPIConf):
            return RestAPI(inputConfiguration)
