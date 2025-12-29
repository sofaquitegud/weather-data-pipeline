from sqlalchemy import create_engine, text
import os

DB_USER = os.getenv("POSTGRES_USER", "de_user")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "de_password")
DB_NAME = os.getenv("POSTGRES_DB", "weather_db")
DB_HOST = "postgres"
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"


def run_transformation():
    engine = create_engine(DB_URL)

    with open("/opt/airflow/transformation/sql/transform_weather.sql", "r") as f:
        sql = f.read()

    try:
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text(sql))
        print(f"[INFO] Transformation completed successfully.")
    except Exception as e:
        print(f"[ERROR] Transformation failed: {e}")
        raise


if __name__ == "__main__":
    run_transformation()
