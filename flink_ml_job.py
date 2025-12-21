import os
from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer


# --- CONFIGURATION ---
# Check this path matches exactly where your JAR file is inside the container
KAFKA_JAR_PATH = "file:///opt/project/flink-sql-connector-kafka-3.0.0-1.17.jar"
KAFKA_BROKERS = "kafka:29092"
TOPIC_NAME = "gdelt-live"


def run_basic_job():
    print("⏳ Initializing Basic Flink Job...")

    # 1. Setup Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    # Load the Kafka Connector JAR
    env.add_jars(KAFKA_JAR_PATH)

    # 2. Define Kafka Source
    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKERS) \
        .set_topics(TOPIC_NAME) \
        .set_group_id("basic-flink-ml_job") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # 3. Create Data Stream
    # We use 'no_watermarks' here because we are just printing, not windowing yet
    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    # 4. Action: Print to Console (stdout)
    ds.print()

    # 5. Execute
    print("🚀 Job Started. Waiting for data...")
    env.execute("Basic Kafka Reader")



run_basic_job()