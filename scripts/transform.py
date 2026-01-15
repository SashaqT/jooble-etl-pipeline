import json
import pandas as pd
import re
import logging
import html
from datetime import datetime

logging.basicConfig(level=logging.INFO)

RAW_JSON_PATH = "/opt/airflow/scripts/raw_jooble_jobs.json"
OUTPUT_CSV_PATH = "/opt/airflow/scripts/cleaned_jooble_jobs.csv"


def clean_text(text):
    if not text:
        return ""

    text = html.unescape(str(text))

    text = re.sub(r'<[^>]+>', '', text)

    text = re.sub(r'\s+', ' ', text).strip()
    
    text = text.replace(';', ',')
    return text


def transform_data():
    logging.info("Starting data transformation")

    try:
        with open(RAW_JSON_PATH, 'r', encoding='utf-8') as f:
            vacancies = json.load(f)
    except FileNotFoundError:
        logging.error(f"Raw file not found: {RAW_JSON_PATH}")
        raise

    if not vacancies:
        logging.warning("No vacancies to transform")
        return 0

    logging.info(f"Transforming {len(vacancies)} vacancies")


    records = []
    for v in vacancies:

        salary_min = 0.0
        salary_max = 0.0
        snippet = str(v.get('snippet', ''))


        salary_match = re.search(r'\$?([\d,]+)k?\s*-\s*\$?([\d,]+)k?', snippet)
        if salary_match:
            try:
                salary_min = float(salary_match.group(1).replace(',', '').replace('k', '000'))
                salary_max = float(salary_match.group(2).replace(',', '').replace('k', '000'))
            except:
                pass

        record = {
            'id': v.get('id', ''),
            'title': clean_text(v.get('title', '')),
            'company': clean_text(v.get('company', '')),
            'location': clean_text(v.get('location', '')),
            'snippet': clean_text(v.get('snippet', '')),
            'source': v.get('source', 'jooble'),
            'type': v.get('type', 'Full-time'),
            'link': v.get('link', ''),
            'date_posted': datetime.now().date(),
            'salary_min': salary_min,
            'salary_max': salary_max,
            'currency': 'USD',
            'is_remote': 'remote' in str(v.get('location', '')).lower()
        }
        records.append(record)

    df = pd.DataFrame(records)


    columns_order = ['title', 'location', 'snippet', 'source', 'type', 'link',
                     'company', 'id', 'date_posted', 'salary_min', 'salary_max',
                     'currency', 'is_remote']
    df = df[columns_order]


    df.to_csv(OUTPUT_CSV_PATH, index=False, encoding='utf-8', sep=';')

    logging.info(f"Transformed {len(df)} records saved to {OUTPUT_CSV_PATH}")
    return len(df)


if __name__ == "__main__":
    transform_data()