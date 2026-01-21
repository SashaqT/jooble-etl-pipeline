\# Jooble ETL Pipeline



Automated ETL pipeline for collecting job postings from Jooble API and loading into PostgreSQL.



\## What it does



Every 6 hours automatically:

\- Collects job listings from Jooble API (30+ records)

\- Cleans data from HTML tags

\- Stores in PostgreSQL database



\## Tech Stack



\- Python 3.8

\- Apache Airflow 2.7

\- PostgreSQL 13

\- Docker



\## How to Run



1\. Clone repository

2\. Create `.env` file with API key

```

&nbsp;  JOOBLE\_API\_KEY=your\_key

```

3\. Run

```bash

&nbsp;  docker-compose up -d

```

4\. Open http://localhost:8080



\## Structure

```

├── dags/          # Airflow DAG

├── scripts/       # Python scripts (extract, transform, load)

└── docker-compose.yaml

```



\## Author



Oleksandr Hudachek

