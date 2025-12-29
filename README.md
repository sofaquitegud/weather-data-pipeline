# Weather Data Engineering Pipeline

## Overview
Automated ETL pipeline that ingests weather forecast data, transforms it for analysis, and validates data quality using Apache Airflow and PostgreSQL.

## Architecture
```
API (Open-Meteo) → Ingestion → Raw Table → Transformation → Summary Table → Quality Checks
```

## Tech Stack
- **Orchestration**: Apache Airflow 2.9.3
- **Database**: PostgreSQL 16
- **Language**: Python 3.12
- **Containerization**: Docker Compose

## Project Structure
```
data-engineering-pipeline/
├── airflow/
│   ├── dags/
│   │   └── weather_pipeline.py
│   ├── logs/
│   ├── plugins/
│   └── requirements.txt
├── database/
│   └── init.sql
├── ingestion/
│   └── fetch_weather.py
├── transformation/
│   ├── python/
│   │   ├── run_transformation.py
│   │   └── data_quality.py
│   └── sql/
│       └── transform_weather.sql
├── .env
├── docker-compose.yml
└── README.md
```

## Setup

### Prerequisites
- Docker and Docker Compose
- Ubuntu 24.04 (or similar Linux distribution)

### Installation

1. Clone the repository:
```bash
cd /home/syfqfrhnn/projects/data-engineering-pipeline
```

2. Start the services:
```bash
docker compose up -d
```

3. Access Airflow UI:
- URL: http://localhost:8080
- Username: admin
- Password: admin

4. Enable the DAG:
- Navigate to the Airflow UI
- Toggle the `weather_pipeline` DAG to ON

## Pipeline Details

### Data Flow
1. **Ingestion**: Fetches 7-day weather forecast from Open-Meteo API for San Francisco
2. **Raw Storage**: Stores raw data in `raw_weather` table
3. **Transformation**: Calculates temperature averages and ranges, loads into `daily_weather_summary`
4. **Quality Checks**: Validates row counts, null values, and date ranges

### Schedule
- Runs daily at midnight UTC
- Can be manually triggered from Airflow UI

### Tables

**raw_weather**
- date: Forecast date
- location: City name
- temp_max: Maximum temperature (°C)
- temp_min: Minimum temperature (°C)
- ingested_at: Timestamp of ingestion

**daily_weather_summary**
- date: Forecast date
- location: City name
- temp_max: Maximum temperature (°C)
- temp_min: Minimum temperature (°C)
- temp_avg: Average temperature (°C)
- temp_range: Temperature range (°C)
- processed_at: Timestamp of transformation

## Data Quality Checks
- No null values in temperature fields
- No dates beyond 7 days in future
- Raw table contains data
- Processed table receives updates

## Monitoring
View logs in Airflow UI:
- DAG runs: http://localhost:8080/dags/weather_pipeline/grid
- Task logs: Click on any task instance

## Troubleshooting

### Reset database
```bash
docker compose down
docker volume rm data-engineering-pipeline_postgres_data_pipeline
docker compose up -d
```

### View logs
```bash
docker compose logs -f airflow
docker compose logs -f postgres
```

### Connect to database
```bash
docker exec -it postgres psql -U de_user -d weather_db
```

## Future Enhancements
- Add more weather metrics (wind, precipitation)
- Implement data visualization dashboard
- Add alert notifications for quality check failures
- Expand to multiple cities
- Add historical data backfill logic