# Spark ETL Pipeline

## Features

- Ingest raw CSV with explicit schema into Bronze layer (Parquet)
- Deduplicate records by `transaction_id`
- Fix invalid `ship_date` where it precedes `order_date`
- Drop invalid `quantity` and null out invalid `unit_price`
- Null out out-of-range `discount_pct` and `customer_age`
- Standardize `gender` to `M` / `F` and validate `payment_type`
- Aggregate Gold layer: daily sales, product category performance, city revenue

---

## How to Run in Spark Console

### 1. Start the Spark shell
```bash
spark-shell --master spark://spark-master:7077
```

### 2. Run Bronze layer
```bash
:load /opt/spark-data/scripts/bronze.py
```

### 3. Run Silver layer
```bash
:load /opt/spark-data/scripts/silver.py
```

### 4. Run Gold layer
```bash
:load /opt/spark-data/scripts/gold.py
```

### 5. Verify output
```scala
spark.read.parquet("/opt/spark-data/gold/daily_sales_metrics.parquet").show(5)
spark.read.parquet("/opt/spark-data/gold/product_category_performance.parquet").show(5)
spark.read.parquet("/opt/spark-data/gold/city_revenue_metrics.parquet").show(5)
```
