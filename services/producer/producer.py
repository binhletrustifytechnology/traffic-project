import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Configure Kafka producer
producer = KafkaProducer(
    bootstrap_servers=["kafka:9092"],  # Use "localhost:9092" if running outside Docker
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def simulate_event():
    """Generate a synthetic traffic event."""
    return {
        "vehicle_id": f"veh_{random.randint(1000,9999)}",
        "timestamp": datetime.utcnow().isoformat(),
        "lat": 10.76 + random.uniform(-0.01, 0.01),   # around HCMC
        "lon": 106.68 + random.uniform(-0.01, 0.01),
        "speed": max(0, random.gauss(35, 10)),        # km/h
        "heading": random.randint(0, 359),
        "road_segment_id": random.choice(["A1","A2","B7","C3"]),
        "weather_code": random.choice([0,1,2])        # 0=clear, 1=rain, 2=heavy rain
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
