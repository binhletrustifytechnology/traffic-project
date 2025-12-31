from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count, expr, to_json, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1. Start Spark session
spark = (SparkSession.builder
         .appName("TrafficStreamProcessor")
         .getOrCreate())

# 2. Define schema for incoming Kafka messages
schema = StructType([
    StructField("vehicle_id", StringType()),
    StructField("timestamp", StringType()),   # will cast to timestamp
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("speed", DoubleType()),
    StructField("heading", DoubleType()),
    StructField("road_segment_id", StringType()),
    StructField("weather_code", StringType())
])

# 3. Read from Kafka topic
raw_df = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "kafka:9092")   # adjust if outside Docker
          .option("subscribe", "traffic_stream")
          .load())


# 4. Parse JSON payload
json_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 5. Cast timestamp field
events = json_df.withColumn("ts", col("timestamp").cast(TimestampType()))

# 6. Windowed aggregations (2‑minute tumbling windows)
agg = (events.groupBy(
            window(col("ts"), "2 minutes"),
            col("road_segment_id")
        )
        .agg(
            avg("speed").alias("avg_speed"),
            count("*").alias("vehicle_count"),
            expr("sum(case when speed < 20 then 1 else 0 end)/count(*)").alias("percent_slow")
        ))

# 7. Flatten window columns
out = agg.selectExpr(
    "road_segment_id",
    "CAST(window.start AS STRING) AS window_start",
    "CAST(window.end AS STRING) AS window_end",
    "CAST(avg_speed AS DOUBLE)",
    "CAST(vehicle_count AS INT)",
    "CAST(percent_slow AS DOUBLE)"
)

# Convert all columns into a JSON string
out_kafka = out.select(
    col("road_segment_id").alias("key"), # optional, use segment ID as key
    to_json(struct(
        col("road_segment_id"),
        col("window_start"),
        col("window_end"),
        col("avg_speed"),
        col("vehicle_count"),
        col("percent_slow")
    )).alias("value") )


# 8. Write results back to Kafka (segment_metrics topic)
query = (out_kafka.writeStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "kafka:9092")
         .option("topic", "segment_metrics")
         .option("checkpointLocation", "/tmp/spark_chkpt")
         .outputMode("update")
         .start())

query.awaitTermination()
