import requests
import json
import os
import logging

logging.basicConfig(level=logging.INFO)

RAW_JSON_PATH = "/opt/airflow/scripts/raw_jooble_jobs.json"


def extract_jooble_data():
    API_KEY = os.getenv('JOOBLE_API_KEY', 'YOUR_API_KEY_HERE')
    API_URL = f"https://jooble.org/api/{API_KEY}"

    logging.info("Starting data extraction from Jooble API")

    headers = {'Content-Type': 'application/json'}

    payload = {
        'keywords': 'python developer',
        'location': ''
    }

    try:
        logging.info(f"Making API request to {API_URL}")
        response = requests.post(API_URL, json=payload, headers=headers, timeout=30)
        response.raise_for_status()

        data = response.json()
        vacancies = data.get('jobs', [])

        logging.info(f"Extracted {len(vacancies)} vacancies")


        with open(RAW_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(vacancies, f, ensure_ascii=False, indent=2)

        logging.info(f"Raw data saved to {RAW_JSON_PATH}")
        return len(vacancies)

    except requests.exceptions.RequestException as e:
        logging.error(f"API request error: {e}")
        raise
    except Exception as e:
        logging.error(f"Data extraction error: {e}")
        raise


if __name__ == "__main__":
    extract_jooble_data()