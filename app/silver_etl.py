from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, trim

spark = (
    SparkSession.builder
    .appName("Silver ETL")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

bronze_df = spark.read.parquet("/opt/spark-data/bronze/retail_sales_bronze.parquet")

print("Bronze count:", bronze_df.count())
bronze_df.printSchema()

bronze_df.groupBy("transaction_id").count().filter(col("count") > 1).show(5, truncate=False)
silver_df = bronze_df.dropDuplicates(["transaction_id"])
print("Bronze count:", silver_df.count())

bronze_df.filter(col("ship_date") < col("order_date")).show(5)
silver_df = silver_df.withColumn(
    "ship_date",
    when(col("ship_date") < col("order_date"), None).otherwise(col("ship_date"))
)

silver_df.filter(col("quantity") <= 0).show(5)
silver_df = silver_df.filter(col("quantity") > 0)

silver_df.filter(col("unit_price") <= 0).show(5)
silver_df = silver_df.withColumn(
    "unit_price",
    when(col("unit_price") <= 0, None).otherwise(col("unit_price"))
)

silver_df.filter((col("discount_pct") < 0) | (col("discount_pct") > 100)).show(5)
silver_df = silver_df.withColumn(
    "discount_pct",
    when((col("discount_pct") < 0) | (col("discount_pct") > 100), None).otherwise(col("discount_pct"))
)

silver_df.filter((col("customer_age") < 15) | (col("customer_age") > 100)).show(5)
silver_df = silver_df.withColumn(
    "customer_age",
    when((col("customer_age") < 15) | (col("customer_age") > 100), None).otherwise(col("customer_age"))
)

silver_df.groupBy("gender").count().show()
silver_df = silver_df.withColumn(
    "gender",
    when(upper(trim(col("gender"))) == "MALE", "M")
    .when(upper(trim(col("gender"))) == "FEMALE", "F")
    .when(col("gender").isin("M", "F"), col("gender"))
    .otherwise(None)
)

silver_df.filter(~col("payment_type").isin("Card", "UPI", "COD")).show(5)
silver_df = silver_df.withColumn(
    "payment_type",
    when(col("payment_type").isin("Card", "UPI", "COD"), col("payment_type")).otherwise(None)
)

print("Silver count:", silver_df.count())

(
    silver_df
    .repartition(8)
    .write
    .mode("overwrite")
    .parquet("/opt/spark-data/silver/retail_sales_clean.parquet")
)

print("✅ Silver layer created successfully")