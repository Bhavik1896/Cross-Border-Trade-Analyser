Real-Time News Analytics Pipeline (Kafka + Flink + TimescaleDB)
Overview

This project is a real-time streaming data pipeline that ingests global news data, processes it continuously, and stores time-series analytics for querying and visualization.

The system is designed to:

run continuously for long durations

handle sustained CPU-bound workloads

process high-volume streaming data with predictable latency

support time-based analytics using hypertables and rolling windows

The pipeline is fully containerized and runs locally using Docker.

High-Level Architecture
News Sources
     ↓
Kafka (ingestion buffer)
     ↓
Flink (stream processing)
     ↓
TimescaleDB (time-series storage)
     ↓
Grafana (SQL-based analytics)


Each component is isolated in its own container and communicates over a Docker network.

Key Features

Real-time ingestion using Kafka topics

Stream processing with PyFlink

CPU-bound workload tuning via Flink parallelism and task slots

Time-series optimized storage using TimescaleDB hypertables

Rolling window analytics (30-day windows)

SQL-only dashboards using Grafana

End-to-end containerized setup using Docker Compose

Designed to run continuously under sustained load

Technologies Used

Apache Kafka – event ingestion and buffering

Apache Flink (PyFlink) – real-time stream processing

TimescaleDB (PostgreSQL) – time-series database with hypertables

Grafana – analytics and visualization (SQL-based)

Docker & Docker Compose – containerization and orchestration

Python – processing logic and feature extraction

Data Flow

News data is ingested into Kafka topics.

Flink consumes events from Kafka and applies:

parsing and normalization

feature extraction

enrichment logic

Processed records are written to TimescaleDB hypertables.

Grafana queries TimescaleDB directly using SQL for analytics.

The system is designed to tolerate restarts and resumes processing without data loss (Kafka offsets).

Performance Characteristics

Processes hundreds of thousands of records per day

Sustained execution for multiple days without manual intervention

CPU-bound workload (validated via Flink flamegraphs)

No downstream backpressure under normal operation

Stable memory usage without swap pressure

Predictable throughput under sustained load

Database Design

Uses TimescaleDB hypertables for efficient time-range queries

Data partitioned by time for fast scans

Optimized for:

time-bucket aggregations

rolling window queries

Grafana dashboard workloads

Typical queries include:

event counts over time

category-wise trends

geographic distributions

rolling aggregates

Configuration Highlights

Flink parallelism explicitly tuned to match available CPU cores

TaskManager memory sized to avoid JVM and Python worker contention

Kafka retention configured to buffer ingestion during processing spikes

Database indexed for time-range and dimension queries

Running the Project
Prerequisites

Docker

Docker Compose

At least 16 GB RAM recommended for sustained runs

Start the pipeline
docker compose up -d

Stop the pipeline
docker compose stop

Tear down containers (without deleting volumes)
docker compose down

Observability & Debugging

Flink Web UI used to monitor:

task parallelism

backpressure

CPU utilization

flamegraphs

Docker stats used for container-level resource monitoring

Grafana dashboards provide real-time and historical analytics

Design Considerations

Focused on sustained performance, not burst benchmarks

Prioritized predictable latency over peak throughput

Avoided over-parallelization to reduce thermal and resource contention

Designed with rolling windows to bound storage growth

Treated the system as a long-running service, not a batch job

Limitations

Single-node deployment

Designed for local execution

No horizontal scaling across machines

Storage capacity limited by local disk

These trade-offs were made intentionally to focus on system behavior and performance under constrained hardware.

Future Improvements

Checkpoint-based recovery using savepoints

Continuous aggregates for pre-computed analytics

Automated retention and compression policies

Multi-node deployment

Externalized storage and dashboards

Author

Bhavik Jain

This project was built to understand real-time data systems under sustained load, focusing on performance, stability, and observability rather than toy examples.
