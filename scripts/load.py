import psycopg2
import csv
import logging

logging.basicConfig(level=logging.INFO)

CSV_PATH = "/opt/airflow/scripts/cleaned_jooble_jobs.csv"

DB_CONFIG = {
    "host": "postgres",
    "database": "airflow",
    "user": "airflow",
    "password": "airflow",
    "port": 5432,
}


def load_and_validate():
    logging.info("=" * 50)
    logging.info("STARTING DATA LOAD")
    logging.info("=" * 50)

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:

        cur.execute("TRUNCATE TABLE jooble_jobs;")
        logging.info("Table truncated")


        with open(CSV_PATH, "r", encoding="utf-8") as f:
            cur.copy_expert(
                """
                COPY jooble_jobs (
                    title, location, snippet, source, type, link,
                    company, id, date_posted, salary_min, salary_max,
                    currency, is_remote
                )
                FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ';')
                """,
                f,
            )

        conn.commit()


        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT company) as unique_companies,
                COUNT(DISTINCT location) as unique_locations,
                AVG(salary_min) as avg_min_salary,
                AVG(salary_max) as avg_max_salary
            FROM jooble_jobs
        """)

        stats = cur.fetchone()

        logging.info("=" * 50)
        logging.info("LOADING STATISTICS")
        logging.info("=" * 50)
        logging.info(f"Total vacancies: {stats[0]}")
        logging.info(f"Unique companies: {stats[1]}")
        logging.info(f"Unique locations: {stats[2]}")

        avg_min = f"{stats[3]:.2f}" if stats[3] is not None else 'N/A'
        avg_max = f"{stats[4]:.2f}" if stats[4] is not None else 'N/A'

        logging.info(f"Average min salary: {avg_min}")
        logging.info(f"Average max salary: {avg_max}")
        logging.info("=" * 50)
        logging.info("LOAD SUCCESS")

    except Exception as e:
        conn.rollback()
        logging.exception("LOAD FAILED")
        raise

    finally:
        cur.close()
        conn.close()