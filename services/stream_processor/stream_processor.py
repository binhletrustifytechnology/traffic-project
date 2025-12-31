from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Create Spark session
spark = SparkSession.builder.appName("KafkaSparkPipeline").getOrCreate()

# Define the schema of the data
schema = StructType([
    StructField("event_id", LongType(), True),
    StructField("event_value", LongType(), True),
    StructField("timestamp", LongType(), True)
])

# Read data from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "my-topic") \
    .load()

# Parse the JSON data from Kafka
df = df.selectExpr("CAST(value AS STRING)")
json_df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Perform transformations
transformed_df = json_df.withColumn("event_value_doubled", col("event_value") * 2)

# Write the transformed data to console (for simplicity)
query = transformed_df.writeStream.outputMode("append").format("console").start()
query.awaitTermination()