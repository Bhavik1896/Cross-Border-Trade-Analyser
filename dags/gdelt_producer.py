import json
import requests
import zipfile
import io
import csv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
from airflow.operators.bash import BashOperator

# --- CONFIGURATION ---
KAFKA_TOPIC = "gdelt-live"
KAFKA_SERVER = "kafka:29092"  # Internal Docker address


def fetch_gdelt_and_push_to_kafka():
    print(f"Starting Real-Time Ingestion at {datetime.now()}...")

    # 1. FIND THE LATEST URL
    try:
        resp = requests.get("http://data.gdeltproject.org/gdeltv2/lastupdate.txt")
        data = resp.text

        target_url = None
        for line in data.splitlines():
            if "gkg.csv.zip" in line:
                target_url = line.split(" ")[2]
                break

        if not target_url:
            print("Could not find GKG URL.")
            return

        print(f"Downloading: {target_url}")

    except Exception as e:
        print(f"Error getting URL: {e}")
        return

    # 2. DOWNLOAD & UNZIP
    try:
        r = requests.get(target_url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        csv_filename = z.namelist()[0]

        print(f"Extracting: {csv_filename}")


        with z.open(csv_filename) as f:
            content = f.read().decode('utf-8', errors='ignore')

    except Exception as e:
        print(f"Error downloading/unzipping: {e}")
        return

    # 3. CONNECT TO KAFKA
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
    except Exception as e:
        print(f"Kafka Connection Failed: {e}")
        return

    lines = content.split('\n')
    count = 0

    for line in lines:
        if len(line) < 10: continue  # Skip empty lines

        cols = line.split('\t')  # GDELT is tab-separated

        try:
            record = {
                "DATE": cols[1],
                "url": cols[4],
                "themes": cols[7],
                "locations": cols[9],
                "persons": cols[11],
                "tone": cols[15],
                "raw_data": cols[4] + " " + cols[7]
            }

            producer.send(KAFKA_TOPIC, value=record)
            count += 1

        except IndexError:
            continue

    producer.flush()
    print(f"Successfully cleaned and pushed {count} records to Kafka.")


# --- AIRFLOW DAG ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
        'gdelt_ingestion_15_min',
        default_args=default_args,
        description='Fetch & Clean GDELT Data every 15 mins',
        schedule_interval='*/15 * * * *',  # Runs every 15 mins
        catchup=False,
) as dag:
    run_ingestion = PythonOperator(
        task_id='fetch_clean_push',
        python_callable=fetch_gdelt_and_push_to_kafka,
    )

