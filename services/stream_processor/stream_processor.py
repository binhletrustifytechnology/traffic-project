from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType


# 1. Initialize Spark with the Kafka Connector
spark = SparkSession.builder \
    .appName("KafkaEventProcessor") \
    .getOrCreate()

# 2. Define the schema matching your JSON
schema = StructType([
    StructField("event_id", IntegerType(), True),
    StructField("event_value", IntegerType(), True),
    StructField("timestamp", DoubleType(), True)
])

# 3. Read from Kafka
# Note: 'kafka:9092' is used because Spark is running inside the Docker network
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("startingOffsets", "earliest") \
    .option("subscribe", "my-topic") \
    .load()

df1 = raw_df.selectExpr("CAST(value AS STRING)")
# df1.show()

# json_df = df1.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Perform transformations
# transformed_df = json_df.withColumn("event_value_doubled", col("event_value") * 2)

# transformed_df.show()

query = (raw_df
         .writeStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "kafka:9092")
         .option("topic", "out-topic")
         .option("checkpointLocation", "/tmp/spark_chkpt")
         .outputMode("update")
         .start())

# Write the transformed data to console (for simplicity)
# query = transformed_df.writeStream.outputMode("append").format("console").start()
query.awaitTermination()

# /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 stream_processor.py
