import json
from typing import Optional

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Types

from ml_logic import get_india_business_verdict

def process_message(value: str) -> Optional[str]:
    try:
        event_data = json.loads(value)
        headline = event_data.get("raw_data", "")

        if headline and len(headline) > 10:
            verdict = get_india_business_verdict(headline)
            return json.dumps({
                "headline": headline,
                "verdict": verdict,
                "processed": True
            })
        return None
    except Exception as e:
        print("Processing error:", e)
        return None


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_topics("gdelt-live") \
        .set_group_id("flink-gdelt-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())\
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    processed = (
        stream
        .map(process_message, output_type=Types.STRING())
        .filter(lambda x: x is not None)
    )

    processed.print()

    kafka_connector_jar = 'file:///opt/flink/lib/flink-connector-kafka-3.3.0-1.20.jar'
    kafka_clients_jar = 'file:///opt/flink/lib/kafka-clients-3.3.0.jar'
    env.add_jars(kafka_connector_jar, kafka_clients_jar)

    env.execute("Realtime GDELT PyFlink Pipeline")


if __name__ == "__main__":
    main()
