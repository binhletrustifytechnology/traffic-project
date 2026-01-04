import pandas as pd
import time
from datetime import datetime, timezone

DEVICE_FILES = {
    "0x0004B0D6": "vehicle/data-0x0004B0D6.csv",
    "0x0004B1AF": "vehicle/data-0x0004B1AF.csv",
    "0x0004B187": "vehicle/data-0x0004B187.csv"
}


def load_data():
    devices_data = {}
    for sn, file in DEVICE_FILES.items():
        try:
            df = pd.read_csv(file)
            df['time_key'] = pd.to_datetime(df['timestamp_seconds'], unit='s').dt.strftime('%H:%M:%S')
            devices_data[sn] = df
            print(f"Loaded {len(df)} rows for device {sn}")
        except Exception as e:
            print(f"Error loading {file}: {e}")

    return devices_data


def run_simulation(interval=1):
    print("Starting Traffic Simulation (Syncing with GMT)...")
    data_store = load_data()

    # Track the last processed second to avoid double-sending or skipping
    last_processed_time = ""

    while True:
        # 1. Get current time in GMT/UTC to match CSV
        now_gmt = datetime.now(timezone.utc)
        current_time_str = now_gmt.strftime("%H:%M:%S")

        # Only process if we have moved to a new second
        if current_time_str != last_processed_time:
            for sn, df in data_store.items():
                # 2. Match the current GMT time with the time_key in CSV
                match = df[df['time_key'] == current_time_str]

                if not match.empty:
                    for _, row in match.iterrows():
                        # This is where you would send the data to your MQTT broker or API
                        print(f"[{current_time_str}] TIME {row['timestamp_seconds']} DEVICE {sn} SENDING: Speed {row['vehicle_avg_speed']} km/h, Volume {row['vehicle_volume']}")
            last_processed_time = current_time_str
        # Wait for the next second
        time.sleep(interval)

if __name__ == "__main__":
    run_simulation()