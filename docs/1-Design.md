### Pipeline Design
1. Data Ingestion

Source: IoT devices streaming traffic data (JSON, CSV, MQTT, Kafka, etc.).

Tools/Tech:
 - Kafka / MQTT → for real-time streaming.
 - Batch ingestion → via CSV/Parquet files into storage (S3, HDFS, Azure Blob).

2. Data Storage

Raw Zone: Store unprocessed data (e.g., in S3, HDFS, or Azure Data Lake).

Processing Zone: Clean and transform data (Spark, Flink, Pandas).

Serving Zone: Aggregated results stored in a database (Postgres, BigQuery, Snowflake).

3. Data Transformation

Clean missing values, normalize schema.

Convert timestamps into human-readable datetime if needed.

Aggregate by grouping keys: [serial_number, timestamp_seconds, vehicle_class]

4. Aggregation Logic

For each group:
- Sum of vehicle_volume
- Average of vehicle_avg_speed
- Count of records (optional)

(*) Scaling Up
Batch Processing: Apache Spark (PySpark) for large datasets.

Streaming: Apache Flink or Spark Structured Streaming for real-time aggregation.

Storage: Use Parquet/ORC for efficient queries.

Serving: Expose aggregated data via REST API or dashboards (Grafana, Power BI).


