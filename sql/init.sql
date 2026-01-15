"""
CREATE SCHEMA IF NOT EXISTS jobs_data;

CREATE TABLE IF NOT EXISTS jobs_data.raw_vacancies (
    id SERIAL PRIMARY KEY,
    raw_data JSONB NOT NULL,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS jobs_data.vacancies (
    vacancy_id VARCHAR(255) PRIMARY KEY,
    title VARCHAR(500),
    company_name VARCHAR(500),
    location VARCHAR(255),
    salary_min NUMERIC(10,2),
    salary_max NUMERIC(10,2),
    currency VARCHAR(10),
    employment_type VARCHAR(100),
    experience_level VARCHAR(100),
    description TEXT,
    requirements TEXT,
    posted_date DATE,
    url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_vacancies_company ON jobs_data.vacancies(company_name);
CREATE INDEX IF NOT EXISTS idx_vacancies_location ON jobs_data.vacancies(location);
CREATE INDEX IF NOT EXISTS idx_vacancies_posted ON jobs_data.vacancies(posted_date);
"""