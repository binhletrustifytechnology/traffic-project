### Example: Real-Time Traffic Analytics
Ingestion: Kafka streams vehicle telemetry.
Processing: Spark Structured Streaming groups data by road segment, computes average speed every 2 minutes.
Storage: Results written to PostgreSQL or S3.
MLlib: Model predicts congestion based on features.
Visualization: Grafana dashboard shows congestion probability.

### Key Considerations
Memory vs. disk: Spark is fast because it uses memory, but large jobs may spill to disk.
Fault tolerance: If a node fails, Spark recomputes lost partitions using lineage (RDD history).
Cluster size: Performance scales with more executors, but tuning is required (parallelism, shuffle optimization).
Streaming latency: Structured Streaming provides near real-time but not microsecond precision; expect seconds-level latency.