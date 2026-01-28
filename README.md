# Real-Time Banking Transaction Intelligence Platform

This project implements an end-to-end, industry-style data engineering platform for real-time banking transaction processing. The system ingests streaming transaction data, processes it using distributed frameworks, stores it in a lakehouse architecture, and provides analytics and machine learning insights.

The primary focus of this project is Data Engineering, with machine learning and business intelligence acting as downstream consumers of engineered data.

---

## Architecture Overview

The platform follows a modern event-driven data architecture:

- Apache Kafka for real-time data ingestion
- Apache Spark for stream and batch processing
- Hive-based Lakehouse (Bronze / Silver / Gold layers)
- Apache Airflow for workflow orchestration
- Machine Learning for fraud/risk scoring
- Power BI / Tableau for analytics and visualization
- Cloud-native deployment (AWS / Azure / GCP)

---

## Technology Stack

- **Streaming:** Apache Kafka  
- **Processing:** Apache Spark (Structured Streaming, Spark SQL)  
- **Storage:** HDFS / Cloud Object Storage (Parquet)  
- **Metadata:** Apache Hive  
- **Orchestration:** Apache Airflow  
- **Machine Learning:** Python, Scikit-learn / Spark ML  
- **Visualization:** Power BI / Tableau  
- **Infrastructure:** Docker, Cloud Platform  

---

## How to Run Locally (Week 1)

1. Start Kafka, Zookeeper, and Kafka UI using Docker:
   ```bash
   docker compose -f infra/docker-compose.yml up -d

2. Run the transaction producer:
cd producer
source .venv/bin/activate
python txn_producer.py

3. Run the transaction consumer:
cd consumer
source .venv/bin/activate
python txn_consumer.py

4. Kafka UI will be available at:
http://localhost:8080