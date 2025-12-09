import json
import time
from kafka import KafkaConsumer
# Import the logic from your existing ml_logic.py file
from ml_logic import get_india_business_verdict

# --- CONFIGURATION ---
# We use 'kafka:29092' because this runs INSIDE the Docker network
KAFKA_TOPIC = "gdelt-live"
KAFKA_SERVER = "kafka:29092"


def start_consuming():
    print("-" * 50)
    print(f"🚀 REAL-TIME PIPELINE STARTING...")
    print(f"📡 Connecting to Kafka at: {KAFKA_SERVER}")
    print("-" * 50)

    consumer = None

    # Retry logic: Keep trying until Kafka is ready
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_SERVER],
                auto_offset_reset='latest',  # Start reading only NEW data
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print("✅ Successfully connected to Kafka!")
        except Exception as e:
            print(f"⏳ Waiting for Kafka... ({e})")
            time.sleep(5)

    print("👀 Watching for new GDELT headlines...")
    print("(Go to Airflow and trigger your DAG now!)\n")

    try:
        # The Main Loop: Runs forever waiting for data
        for message in consumer:
            # 1. Parse the JSON message
            event_data = message.value

            # 2. Extract the text (we used 'raw_data' in the producer)
            headline = event_data.get("raw_data", "")

            # 3. If valid text exists, run the ML Analysis
            if headline and len(headline) > 10:
                get_india_business_verdict(headline)

    except KeyboardInterrupt:
        print("\n🛑 Pipeline stopped by user.")
    except Exception as e:
        print(f"❌ Critical Error: {e}")
    finally:
        if consumer:
            consumer.close()


if __name__ == "__main__":
    start_consuming()