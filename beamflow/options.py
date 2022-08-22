from sys import argv

from apache_beam.options.pipeline_options import PipelineOptions

from datetime import datetime

from beamflow.constants import SETUP_FILE_PATH


class JobOptions:
    def __init__(self, jobConfiguration):
        self.jobConfiguration = jobConfiguration

    def getOptions(self):
        beam_options = PipelineOptions(
            flags=argv,
            runner=self.jobConfiguration.runner,
            project=self.jobConfiguration.project,
            job_name=self.jobConfiguration.jobname + "-" + datetime.now().strftime("%Y-%m-%d-%H-%M-%S"),
            temp_location=self.jobConfiguration.templocation,
            region=self.jobConfiguration.region,
            streaming=eval(self.jobConfiguration.streaming),
            setup_file=SETUP_FILE_PATH
        )
        return beam_options
