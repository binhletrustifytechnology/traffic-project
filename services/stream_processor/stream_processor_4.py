from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, struct, col
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
    .option("subscribe", "my-topic") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parse the JSON and Apply Transformation
# Kafka 'value' arrives as binary; we cast to string then parse
transformed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_value", col("event_value") * 2) \
    .select(to_json(struct("*")).alias("value")) # Convert all columns back to one JSON string

# 5. Write to Sink Topic
query = transformed_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "output-topic") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

query.awaitTermination()