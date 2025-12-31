# Use the official Spark image you specified
FROM spark:4.0.0-python3

# Set working directory inside the container
WORKDIR /opt/stream_processor

# Copy your streaming job into the container
COPY stream_processor_2.py /opt/stream_processor/

USER root

# Install any Python dependencies needed by your job
# (Spark image already has PySpark, but you may need kafka-python, etc.)
RUN apt-get update
RUN apt-get install -y python3-pip
RUN pip3 install kafka-python
RUN rm -rf /var/lib/apt/lists/*

# Add Kafka connector JARs to Spark's classpath
COPY ./libs/spark-sql-kafka-0-10_2.13-4.0.0.jar /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.0.0.jar
COPY ./libs/kafka-clients-3.7.0.jar /opt/spark/jars/kafka-clients-3.7.0.jar
COPY ./libs/spark-token-provider-kafka-0-10_2.13-4.0.0.jar /opt/spark/jars/spark-token-provider-kafka-0-10_2.13-4.0.0.jar
COPY ./libs/commons-pool2-2.8.0.jar /opt/spark/jars/commons-pool2-2.8.0.jar
# Default command: run the Spark job
CMD ["/opt/spark/bin/spark-submit", "/opt/stream_processor/stream_processor.py"]
