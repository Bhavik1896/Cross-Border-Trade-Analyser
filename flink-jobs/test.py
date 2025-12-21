# flink-jobs/test.py
from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
env.from_collection([1, 2, 3]).print()
env.execute("test")
