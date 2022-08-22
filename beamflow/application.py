from beamflow.conf import Configuration
from beamflow.constants import APPLICATION_PATH
from beamflow.options import JobOptions
from beamflow.readers import Readers
from beamflow.transferEngine import TransferEngine
from beamflow.transformations import mapper, filtering
from beamflow.writers import Writers

class App:
    def __init__(self):
        self.conf = Configuration(APPLICATION_PATH).readConf()
        self.inputConfiguration = self.conf.get_config_section("INPUT").getInputConf()
        self.jobConfiguration = self.conf.get_config_section("JOB").getJobConf()
        self.sinkConfiguration = self.conf.get_config_section("SINK").getSinkConf()
        self.transfConfiguration = self.conf.get_config_section("TRANSFORMATION").getTransfConf()

        self.reader = Readers.factoryReader(self.inputConfiguration)
        self.writer = Writers.factoryWriter(self.sinkConfiguration)

        self.mapper = mapper(self.transfConfiguration.mapping)
        self.filter = filtering(self.transfConfiguration.filter)
        self.options = JobOptions(self.jobConfiguration).getOptions()

    def run(self):
        TransferEngine.transfer(reader=self.reader, mapper=self.mapper, filter=self.filter, writer=self.writer,
                                options=self.options)
