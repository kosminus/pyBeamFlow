import apache_beam as beam
from beamflow.business_rules import *


class Engine:
    @staticmethod
    def transfer(reader, mapper, filter, writer, options):
        pass


class TransferEngine(Engine):
    @staticmethod
    def transfer(reader, mapper, filter, writer, options):
        pipeline = beam.Pipeline(options=options)
        content = (pipeline
                   | "Read" >> reader
                   | "Mapper" >> mapper
                   | "Filter" >> filter
                   #  | 'Window' >> beam.WindowInto(window.FixedWindows(6))
                   #    | 'ConvertToJsonLine' >> beam.Map(to_json_line)
                   # | "DOFN" >> beam.ParDo(GenericDoFn(lambda x : x.upper() ))
                   | "Write" >> writer
                   )
        pipeline.run()
