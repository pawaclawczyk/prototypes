import inspect

from dagster import OpDefinition

from workflow import ops

for mem in inspect.getmembers(ops, lambda m: isinstance(m, OpDefinition)):
    print(mem)
