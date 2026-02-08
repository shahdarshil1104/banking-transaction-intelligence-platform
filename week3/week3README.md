✅ Step A — Kafka → Bronze (Optional Streaming Scaffold)
⚠️ Kafka streaming connector was optional due to local dependency issues.
Bronze ingestion was completed using batch ingestion from Kafka replay.
File:
pipelines/kafka_to_bronze_stream.py   (optional scaffold)
Command (if running streaming):
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  pipelines/kafka_to_bronze_stream.py

Step B — Bronze → Silver (Clean + DLQ + Dedup)
File:
pipelines/bronze_to_silver.py
Command
spark-submit \
  --packages org.apache.kafka:kafka-clients:3.4.1 \
  pipelines/bronze_to_silver.py
What this step does
Parses timestamps
Normalizes columns (currency, channel, status, country)
Adds:
event_ts
event_date
hour
Validation rules:
null transaction_id
null timestamp
negative amount
Invalid rows → Kafka DLQ topic transactions_dlq
Deduplicates by transaction_id
Writes partitioned Silver Parquet
Output:
data/silver/transactions/event_date=YYYY-MM-DD/
Verification:
du -sh data/silver/transactions
find data/silver/transactions -type f | wc -l

Step C — Silver → Gold (KPIs + Features)
Daily KPIs
File:
pipelines/silver_to_gold.py
Command:
spark-submit pipelines/silver_to_gold.py
Creates:
data/gold/daily_kpis
data/gold/customer_features
Merchant KPIs
File:
pipelines/silver_to_gold_merchants.py
Command:
spark-submit pipelines/silver_to_gold_merchants.py
Creates:
data/gold/merchant_kpis
✅ Step D — Validation (Spark Shell)
spark-shell
val silver = spark.read.parquet("data/silver/transactions/*")
silver.count()

val goldDaily = spark.read.parquet("data/gold/daily_kpis")
goldDaily.show(false)

val goldCust = spark.read.parquet("data/gold/customer_features")
goldCust.orderBy(org.apache.spark.sql.functions.desc("total_spend")).show(10, false)

val merchants = spark.read.parquet("data/gold/merchant_kpis")
merchants.orderBy(org.apache.spark.sql.functions.desc("high_risk_rate")).show(10, false)
📊 Final Metrics
Layer	Rows
Bronze	2,647,668
Silver	2,647,668
DLQ	0
Gold KPIs	1 day
Gold Customers	~10k customers
Gold Merchants	~1k merchants
📁 Files Implemented (Week 3)
File	Purpose
bronze_to_silver.py	Clean + DLQ + dedup
silver_to_gold.py	Daily + Customer KPIs
silver_to_gold_merchants.py	Merchant risk KPIs
replay_bronze_to_kafka.py	Kafka replay
kafka_to_bronze_stream.py	Optional streaming scaffold
