import configparser
import logging
from typing import Union, Optional


class Configuration:

    def __init__(self, filename):
        self.filename = filename

    def readConf(self):
        self.parser = configparser.RawConfigParser()
        try:
            self.parser.read(self.filename)
        except FileNotFoundError:
            logging.ERROR("application.conf file not found")
        return self

    def get_config_section(self, sectionToLook):
        self.options_dict = {}
        if self.parser.has_section(sectionToLook):
            options = self.parser.options(sectionToLook)
            for option in options:
                self.options_dict[option] = self.parser.get(sectionToLook, option)
        return self

    def getInputConf(self):
        return ConfigurationFactory.create_conf(InputConfiguration(self.options_dict))

    def getJobConf(self):
        return JobConfiguration(self.options_dict)

    def getTransfConf(self):
        return TransfConfiguration(self.options_dict)

    def getSinkConf(self):
        return ConfigurationFactory.create_conf(SinkConfiguration(self.options_dict))


class Dict2Class(object):
    def __init__(self, my_dict):
        for key in my_dict:
            setattr(self, key, my_dict[key])


class JobConfiguration(Dict2Class):
    pass


class TransfConfiguration(Dict2Class):
    pass


class SinkConfiguration(Dict2Class):
    pass


class InputConfiguration(Dict2Class):
    pass


class FileConf:
    def __init__(self, configuration):
        self.filepath = configuration.filepath
        self.schema = configuration.schema if hasattr(configuration, "schema") else None


class PubSubConf:
    def __init__(self, configuration):
        self.project = configuration.project
        self.topic = configuration.topic


class BigQueryConf:
    def __init__(self, configuration):
        self.project = configuration.project
        self.dataset = configuration.dataset
        self.table = configuration.table
        self.sql = configuration.sql
        self.schema = Optional[configuration.schema]


class RestAPIConf:
    def __init__(self, configuration):
        self.url = configuration.url


class ConfigurationFactory:
    @staticmethod
    def create_conf(configuration: Union[InputConfiguration, SinkConfiguration]):
        if configuration.name == "file":
            return FileConf(configuration)
        elif configuration.name == "pubsub":
            return PubSubConf(configuration)
        elif configuration.name == "bigquery":
            return BigQueryConf(configuration)
        elif configuration.name == "restapi":
            return RestAPIConf(configuration)
