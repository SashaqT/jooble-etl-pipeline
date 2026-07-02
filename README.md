# Jooble ETL Pipeline

Automated ETL pipeline for collecting job postings from Jooble API, transforming data and loading it into PostgreSQL using Apache Airflow.

## What it does

The pipeline runs every 6 hours:

- Extracts job postings from Jooble API
- Cleans and transforms raw data
- Loads processed data into PostgreSQL database
- Orchestrates workflow using Apache Airflow

## ETL Flow


## Tech Stack

- Python 3.8
- Apache Airflow 2.7
- PostgreSQL 13
- Docker
- Pandas
- Requests

## How to Run

### 1. Clone repository

git clone <repository_url>
cd jooble_etl


### 2. Create `.env` file

Create `.env` file with:

JOOBLE_API_KEY=your_api_key

DB_HOST=your_host
DB_NAME=your_name
DB_USER=your_user
DB_PASSWORD=your_password
DB_PORT=your_port


### 3. Start project

docker compose up -d


### 4. Open Airflow UI

http://localhost:8080


## Database

PostgreSQL database and required tables are created automatically during initialization using `sql/init.sql`.

## Author

Oleksandr Hudachek