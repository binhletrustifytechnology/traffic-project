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
raw_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-1:9092") \
    .option("startingOffsets", "earliest") \
    .option("subscribe", "my-topic") \
    .load()

df1 = raw_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

df1.show()