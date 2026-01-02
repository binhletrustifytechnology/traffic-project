import json
import random
import time
import uuid

from kafka import KafkaProducer

# Configure Kafka producer
producer = KafkaProducer(
    bootstrap_servers=["kafka:9092"],  # Use "localhost:9092" if running outside Docker
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def simulate_event():
    """Generate a synthetic traffic event."""
    return {
        "id": str(uuid.uuid4()),
        "serial_number": ['0x0004B182', '0x0004B0D0', '0x0004B1BA'][random.randint(0, 2)],
        "message_number": [random.randint(0, 2)],
        "timestamp_seconds": int(time.time()),
        "lane": random.randint(0, 2),
        "vehicle_class": random.randint(0, 2),
        "vehicle_volume": random.randint(0, 10),
        "vehicle_avg_speed": random.choice([3,5,7])
    }

def main():
    print("Starting traffic producer...")
    while True:
        event = simulate_event()
        producer.send("traffic_stream", event)
        print(f"Produced event: {event}")
        time.sleep(1)  # adjust frequency as needed

if __name__ == "__main__":
    main()
