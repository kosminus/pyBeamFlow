from  apache_beam import Map, Filter
from beamflow.business_rules import *

def mapper(expression):
    fn = eval(expression)
    return Map(fn)

def filtering(expression):
    fn = eval(expression)
    return Filter(fn)

