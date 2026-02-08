# pipelines/silver_to_gold.py
from pyspark.sql import SparkSession, functions as F

SILVER_PATH = "data/silver/transactions/*"

GOLD_DAILY = "data/gold/daily_kpis"
GOLD_CUSTOMER = "data/gold/customer_features"
GOLD_MERCHANT = "data/gold/merchant_kpis"   # ✅ new

spark = SparkSession.builder.appName("silver_to_gold").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

silver = spark.read.parquet(SILVER_PATH)

# ✅ ensure event_date exists (in case it wasn't written as a column)
silver = silver.withColumn("event_date", F.to_date(F.col("event_ts")))

# -------------------------
# 1) Daily KPIs (Gold)
# -------------------------
daily = (silver.groupBy("event_date")
  .agg(
    F.count("*").alias("txn_count"),
    F.sum("amount").alias("total_amount"),
    F.avg(F.when(F.col("status") == "APPROVED", 1).otherwise(0)).alias("approval_rate"),
    F.avg(F.when(F.col("is_high_risk") == True, 1).otherwise(0)).alias("high_risk_rate")
  )
)

daily.write.mode("overwrite").parquet(GOLD_DAILY)

# -------------------------
# 2) Customer Features (Gold)
# -------------------------
cust = (silver.groupBy("customer_id")
  .agg(
    F.count("*").alias("txn_count"),
    F.sum("amount").alias("total_spend"),
    F.avg("amount").alias("avg_amount"),
    F.max("amount").alias("max_amount")
  )
)

cust.write.mode("overwrite").parquet(GOLD_CUSTOMER)

# -------------------------
# 3) Merchant KPIs (Gold) ✅ new
# -------------------------
merchants = (silver.groupBy("merchant_id")
  .agg(
    F.count("*").alias("txn_count"),
    F.sum("amount").alias("total_amount"),
    F.avg("amount").alias("avg_amount"),
    F.avg(F.when(F.col("is_high_risk") == True, 1).otherwise(0)).alias("high_risk_rate"),
    F.avg(F.when(F.col("status") == "APPROVED", 1).otherwise(0)).alias("approval_rate")
  )
)

merchants.write.mode("overwrite").parquet(GOLD_MERCHANT)

# -------------------------
# Summary
# -------------------------
print("=== Silver->Gold Summary ===")
print(f"Silver rows: {silver.count()}")
print(f"Gold daily rows: {spark.read.parquet(GOLD_DAILY).count()}")
print(f"Gold customer rows: {spark.read.parquet(GOLD_CUSTOMER).count()}")
print(f"Gold merchant rows: {spark.read.parquet(GOLD_MERCHANT).count()}")
