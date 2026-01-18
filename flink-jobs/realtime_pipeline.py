import json
from typing import Tuple, List
from datetime import datetime, timedelta

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema, Encoder
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream.connectors.file_system import (StreamingFileSink,OutputFileConfig,DefaultRollingPolicy)


from ml_logic import get_india_business_verdict

TEMP_OUTPUT_PATH = "file:///opt/flink/temp_output/gdelt_events"


def process_message(value: str):
    try:
        event_data = json.loads(value)

        # Build richer NLP text
        text = " ".join(filter(None, [
            event_data.get("themes"),
            event_data.get("locations"),
            event_data.get("persons")
        ]))

        news_url = event_data.get("url")

        if text and len(text) > 10:
            return get_india_business_verdict(text, news_url)

        return []

    except Exception:
        import traceback
        traceback.print_exc()
        return []


# ====================================================
# Execution Of  Job
# ====================================================

def execution():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(3)

    # ====================================================
    # Kafka Source
    # ====================================================

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:29092")
        .set_topics("gdelt-live")
        .set_group_id("flink-gdelt-group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")


    # ====================================================
    # Output Schema (ROW)
    # ====================================================

    OUTPUT_SCHEMA = Types.ROW([
        Types.STRING(),  # timestamp
        Types.STRING(),  # headline
        Types.STRING(),  # verdict
        Types.FLOAT(),   # score
        Types.STRING(),  # sector
        Types.STRING(),  # target_country
        Types.STRING(),   # partner_name
        Types.STRING(),  # state
        Types.STRING()  # news_url
    ])



    processed_stream = stream.flat_map(
        process_message,
        output_type=OUTPUT_SCHEMA
    )

    # ====================================================
    # Convert Row → String (REQUIRED for FileSink)
    # ====================================================

    string_stream = processed_stream.map(
        lambda r: ",".join(str(field) for field in r),
        output_type=Types.STRING()
    )

    # ====================================================
    # Streaming File Sink (CORRECT PyFlink API)
    # ====================================================

    sink_builder = StreamingFileSink.for_row_format(
        base_path=TEMP_OUTPUT_PATH,
        encoder=Encoder.simple_string_encoder()
    )

    sink_builder.with_rolling_policy(
        DefaultRollingPolicy.default_rolling_policy(
            part_size=1024 * 1024 * 10,   # 10 MB
            rollover_interval=60000,      # Running for 1 minute
            inactivity_interval=60000
        )
    )

    sink_builder.with_output_file_config(
        OutputFileConfig.builder().build()
    )

    string_stream.add_sink(sink_builder.build())\
    .name("Timescale DB Sink")

    # ====================================================
    # Required JARs
    # ====================================================

    env.add_jars(
        "file:///opt/flink/lib/flink-connector-kafka-3.3.0-1.20.jar",
        "file:///opt/flink/lib/kafka-clients-3.3.0.jar",
        "file:///opt/flink/lib/flink-connector-files-1.20.3.jar"
    )

    # ====================================================
    # Execute Job
    # ====================================================

    env.execute("Realtime GDELT PyFlink Pipeline (StreamingFileSink)")

execution()