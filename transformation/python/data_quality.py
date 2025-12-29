from sqlalchemy import create_engine, text
import os

DB_USER = os.getenv("POSTGRES_USER", "de_user")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "de_password")
DB_NAME = os.getenv("POSTGRES_DB", "weather_db")
DB_HOST = "postgres"
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"


def check_data_quality():
    engine = create_engine(DB_URL)

    checks = {
        "raw_row_count": "SELECT COUNT(*) FROM raw_weather",
        "processed_row_count": "SELECT COUNT(*) FROM daily_weather_summary",
        "null_temperatures": "SELECT COUNT(*) FROM raw_weather WHERE temp_max IS NULL OR temp_min IS NULL",
        "future_dates": "SELECT COUNT(*) FROM raw_weather WHERE date > CURRENT_DATE + INTERVAL '7 days'",
    }

    results = {}

    with engine.connect() as conn:
        for check_name, query in checks.items():
            result = conn.execute(text(query)).scalar()
            results[check_name] = result
            print(f"[CHECK] {check_name}: {result}")

    if results["null_temperatures"] > 0:
        raise ValueError(
            f"Found {results['null_temperatures']} rows with null temperatures."
        )

    if results["future_dates"] > 0:
        raise ValueError(
            f"Found {results['future_dates']} rows with invalid future dates."
        )

    if results["raw_row_count"] == 0:
        raise ValueError("No data in raw_weather table")

    print("[INFO] All data quality checks passed.")


if __name__ == "__main__":
    check_data_quality()
