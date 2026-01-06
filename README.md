# Real-Time News Analytics Pipeline

## Overview

This project implements a **real-time streaming data pipeline** for ingesting, processing, and analyzing high-volume news data. The system is designed to run continuously under sustained load and focuses on **throughput, stability, and observability** rather than toy-scale examples.

The entire pipeline runs locally using containerized services and mirrors the architecture and operational challenges of production streaming systems.

---

## Architecture

News Sources
↓
Kafka (event ingestion & buffering)
↓
Flink (real-time stream processing)
↓
TimescaleDB (time-series storage)
↓
Grafana (SQL-based analytics & visualization)

Each component runs in an isolated Docker container and communicates over a private Docker network.

---

## Key Features

- Real-time ingestion using **Apache Kafka**
- Continuous stream processing with **Apache Flink (PyFlink)**
- CPU-bound workload tuning via controlled parallelism and task slots
- Time-series optimized storage using **TimescaleDB hypertables**
- Rolling window analytics (30-day windows)
- SQL-only dashboards using **Grafana**
- Fully containerized setup using **Docker Compose**
- Designed for **long-running, sustained workloads**

---

## Technologies Used

- **Apache Kafka** – streaming ingestion and buffering
- **Apache Flink (PyFlink)** – real-time processing and enrichment
- **TimescaleDB (PostgreSQL)** – time-series database with hypertables
- **Grafana** – analytics and visualization
- **Docker & Docker Compose** – containerization and orchestration
- **Python** – processing logic and feature extraction

---

## Data Flow

1. News events are ingested into Kafka topics.
2. Flink consumes events and performs:
   - parsing and normalization
   - feature extraction
   - enrichment logic
3. Processed records are written to TimescaleDB hypertables.
4. Grafana queries TimescaleDB directly using SQL for analytics.

Kafka offsets ensure that processing can resume safely after restarts.

---

## Performance Characteristics

- Processes **hundreds of thousands of records per day**
- Sustained execution over multiple days without manual intervention
- Primarily **CPU-bound workload**
- Stable memory usage without swap pressure
- Predictable throughput with no downstream backpressure under normal load
- Verified using Flink Web UI and container-level metrics

---

## Database Design

- TimescaleDB hypertables partitioned by time
- Optimized for:
  - time-bucket aggregations
  - rolling window queries
  - Grafana dashboard workloads
- Indexed for time-range and dimensional queries

Typical analytics include:
- event volume over time
- category and geographic trends
- rolling aggregates

---

## Configuration Highlights

- Flink parallelism explicitly tuned to match available CPU cores
- TaskManager memory sized to avoid JVM and Python worker contention
- Kafka retention configured to absorb ingestion spikes
- Rolling windows used to bound storage growth

---

## Running the Project

### Prerequisites
- Docker
- Docker Compose
- Recommended: 16 GB RAM for sustained runs

### Start the pipeline
```bash
docker compose up -d
```
## Stopping the Pipeline

### Stop running containers (without removing them)
```bash
docker compose stop
```
### Tear down containers (without deleting volumes)
```bash
docker compose down
```
## Observability & Debugging

The system includes multiple layers of observability to monitor performance and behavior under sustained load.

### Flink Web UI
Used for monitoring:
- Task execution
- Operator parallelism
- Backpressure
- CPU usage
- Flamegraphs

### Docker
- `docker stats` for container-level CPU and memory monitoring

### Grafana
- SQL-based dashboards
- Real-time analytics
- Historical trend analysis

---

## Design Considerations

- Optimized for sustained performance rather than burst benchmarks
- Predictable latency prioritized over peak throughput
- Parallelism carefully controlled to avoid thermal and resource contention
- Treated as a long-running service instead of a batch job

---

## Limitations

- Single-node deployment
- Local execution only
- No horizontal scaling across machines
- Storage limited by local disk capacity

These trade-offs were made intentionally to focus on system behavior under constrained hardware.

---

## Future Improvements

- Continuous aggregates for pre-computed analytics
- Automated retention and compression policies
- Checkpoint-based recovery using savepoints
- Multi-node deployment
- Externalized dashboards and storage

---

## Author

**Bhavik Jain**

Built to explore real-time data systems under sustained load, with emphasis on performance tuning, system stability, and observability.

