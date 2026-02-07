from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

BRONZE_PATH = "data/bronze/transactions/*"
SILVER_PATH = "data/silver/transactions"
DLQ_TOPIC   = "transactions_dlq"
BOOTSTRAP   = "localhost:9092"

spark = (
    SparkSession.builder
    .appName("bronze_to_silver_with_dlq")
    # Safe-ish local defaults for laptop
    .config("spark.sql.shuffle.partitions", "64")
    .config("spark.sql.files.maxRecordsPerFile", "500000")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

bronze = spark.read.parquet(BRONZE_PATH)

# Normalize + derive event time/date (always present)
base = (
    bronze
    .withColumn("event_ts", F.to_timestamp("timestamp"))
    .withColumn("event_date", F.to_date(F.coalesce(F.col("event_ts"), F.col("ingest_ts"))))
    .withColumn("currency", F.upper(F.trim(F.col("currency"))))
    .withColumn("channel",  F.upper(F.trim(F.col("channel"))))
    .withColumn("country",  F.upper(F.trim(F.col("country"))))
    .withColumn("status",   F.upper(F.trim(F.col("status"))))
)

# Validation rules
invalid_cond = (
    F.col("transaction_id").isNull() |
    F.col("amount").isNull() |
    (F.col("amount") < 0) |
    F.col("timestamp").isNull() |
    F.col("event_date").isNull()
)

valid   = base.filter(~invalid_cond)
invalid = base.filter(invalid_cond)

# Only trigger DLQ write if there is at least 1 invalid row
has_invalid = invalid.limit(1).count() > 0
if has_invalid:
    dlq_df = invalid.select(
        F.lit(None).cast("string").alias("key"),
        F.to_json(
            F.struct(
                F.col("*"),
                F.lit("VALIDATION_FAILED").alias("error_reason"),
                F.current_timestamp().alias("dlq_ts")
            )
        ).alias("value")
    )

    (dlq_df.write
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("topic", DLQ_TOPIC)
        .save()
    )

# Deduplicate: keep latest ingest_ts for each transaction_id
w = Window.partitionBy("transaction_id").orderBy(F.col("ingest_ts").desc_nulls_last())
silver = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# IMPORTANT: Repartition before write to avoid huge single-task writers (OOM)
# Tune 32/64/128 depending on your machine.
silver_out = silver.repartition(64, "event_date")

(silver_out.write
    .mode("overwrite")
    .partitionBy("event_date")
    .parquet(SILVER_PATH)
)

# Avoid doing 4 full counts (expensive). Do 1 cache + a few counts.
bronze_cached = bronze.cache()
valid_cached = valid.cache()
invalid_cached = invalid.cache()
silver_cached = silver.cache()

print("=== Bronze -> Silver Summary ===")
print("Bronze rows:  ", bronze_cached.count())
print("Valid rows:   ", valid_cached.count())
print("Invalid rows: ", invalid_cached.count())
print("Silver rows:  ", silver_cached.count())

