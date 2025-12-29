import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import os

# --- CONFIGURATION ---
DB_USER = os.getenv("POSTGRES_USER", "de_user")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "de_password")
DB_NAME = os.getenv("POSTGRES_DB", "weather_db")
DB_HOST = "postgres"  # service name in Docker Compose

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"
engine = create_engine(DB_URL)

API_URL = "https://api.open-meteo.com/v1/forecast?latitude=37.77&longitude=-122.42&daily=temperature_2m_max,temperature_2m_min&timezone=America/Los_Angeles"


def fetch_data():
    """Fetch data from API and return as DataFrame"""
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()
        daily = data.get("daily", {})
        df = pd.DataFrame(
            {
                "date": daily.get("time"),
                "temp_max": daily.get("temperature_2m_max"),
                "temp_min": daily.get("temperature_2m_min"),
            }
        )
        df["location"] = "San Francisco"
        df["ingested_at"] = datetime.now()
        return df
    except Exception as e:
        print(f"[ERROR] Failed to fetch data: {e}")
        return pd.DataFrame()  # empty DataFrame on failure


def load_to_db(df):
    """Load DataFrame to PostgreSQL raw table"""
    if df.empty:
        print("[INFO] No data to load.")
        return
    try:
        df.to_sql("raw_weather", engine, if_exists="append", index=False)
        print(f"[INFO] Loaded {len(df)} rows to raw_weather.")
    except Exception as e:
        print(f"[ERROR] Failed to load data: {e}")


if __name__ == "__main__":
    df = fetch_data()
    load_to_db(df)
