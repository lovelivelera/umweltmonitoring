import requests
import pandas as pd
from timescale import write_sensor_data  # deine eigene Funktion zum Speichern
from datetime import datetime
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:postgres@localhost:5433/env_monitoring")

SENSEBOX_ID = "6252afcfd7e732001bb6b9f7"
API_URL = f"https://api.opensensemap.org/boxes/{SENSEBOX_ID}?format=json"

def fetch_latest_measurements():
    response = requests.get(API_URL)
    box_data = response.json()

    records = []

    for sensor in box_data["sensors"]:
        sensor_type = sensor["title"]
        for measurement in sensor["lastMeasurement"]:
            timestamp = sensor["lastMeasurement"]["createdAt"]
            value = sensor["lastMeasurement"]["value"]

            records.append({
                "sensor_type": sensor_type,
                "value": float(value),
                "timestamp": pd.to_datetime(timestamp)
            })

    df = pd.DataFrame(records)
    return df

if __name__ == "__main__":
    df = fetch_latest_measurements()
    print(df.head())

    # ⬇️ In DB schreiben
    write_sensor_data(engine, SENSEBOX_ID, df)
