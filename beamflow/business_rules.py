import csv
import json
from apache_beam import DoFn


class GenericDoFn(DoFn):
    def __init__(self, function):
        self.function = function

    def process(self, element):
        return self.function(element)


def to_json_line(bq_row):
    row = dict()
    for key in bq_row:
        row[key] = bq_row[key]

    row_json = json.dumps(row, default=str)
    return row_json.encode('utf-8')


def parse_csv(element):
    for line in csv.reader([element], quotechar='"', delimiter=',', skipinitialspace=True):
        return line


def split_line(line: str):
    return line.split(",")

