-- run on Postgres container init
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS reporting;
CREATE SCHEMA IF NOT EXISTS etl;

-- staging raw table (wide, flexible)
CREATE TABLE IF NOT EXISTS staging.raw_customers (
    id serial PRIMARY KEY,
    source_file text,
    ingested_at timestamptz DEFAULT now(),
    raw jsonb
);

-- reporting cleaned table (example schema adapted to telecom churn dataset)
CREATE TABLE IF NOT EXISTS reporting.customers (
    customer_id text PRIMARY KEY,
    gender text,
    senior_citizen boolean,
    partner boolean,
    dependents boolean,
    tenure integer,
    phone_hash text,
    email_hash text,
    contract text,
    monthly_charges numeric,
    total_charges numeric,
    churn boolean,
    updated_at timestamptz DEFAULT now()
);

-- ETL logs for monitoring
CREATE TABLE IF NOT EXISTS etl.etl_logs (
    run_id serial PRIMARY KEY,
    run_time timestamptz DEFAULT now(),
    file_name text,
    rows_in integer,
    rows_out integer,
    duration_seconds numeric,
    notes text
);
