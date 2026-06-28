CREATE TABLE IF NOT EXISTS jooble_jobs (
    title TEXT,
    location TEXT,
    snippet TEXT,
    source TEXT,
    type TEXT,
    link TEXT,
    company TEXT,
    id TEXT PRIMARY KEY,
    date_posted DATE,
    salary_min NUMERIC,
    salary_max NUMERIC,
    currency TEXT,
    is_remote BOOLEAN
);