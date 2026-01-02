from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, to_timestamp, window, sum, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

spark = SparkSession.builder \
    .appName("TrafficAggregation") \
    .getOrCreate()

# Define the schema based on your data sample
schema = StructType([
    StructField("id", StringType()),
    StructField("serial_number", StringType()),
    StructField("message_number", IntegerType()),
    StructField("timestamp_seconds", LongType()),
    StructField("lane", IntegerType()),
    StructField("vehicle_class", IntegerType()),
    StructField("vehicle_volume", IntegerType()),
    StructField("vehicle_avg_speed", DoubleType())
])

# 1. Read from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "traffic_stream") \
    .option("startingOffsets", "earliest") \
    .load()

# 2. Parse CSV/JSON value and convert timestamp
# Assuming the 'value' column from Kafka contains the comma-separated string
parsed_df = raw_df.selectExpr("CAST(value AS STRING)").select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Convert the unix seconds to a proper Spark Timestamp for windowing
formatted_df = parsed_df.withColumn("event_time", from_unixtime(col("timestamp_seconds")).cast("timestamp"))

# 3. Aggregate
# You can aggregate by a time window (e.g., 5 minutes) or just the raw timestamp
aggregated_df = formatted_df.groupBy(
    "serial_number",
    "timestamp_seconds",
    "vehicle_class"
).agg(
    sum("vehicle_volume").alias("total_volume"),
    avg("vehicle_avg_speed").alias("avg_speed")
)

# 4. Write "Batch-style" but mark as read
query = aggregated_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(availableNow=True) \
    .option("checkpointLocation", "/tmp/spark_checkpoints/traffic_agg") \
    .start()

query.awaitTermination()

# /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 stream_processor_a.py

