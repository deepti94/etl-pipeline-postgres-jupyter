import os
import time
import hashlib
import json
import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from apscheduler.schedulers.background import BackgroundScheduler
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS, FEED_DIR, INTERVAL_MINUTES
import numpy as np
import math
from loguru import logger


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)

# default values mapping (adjust as needed)
DEFAULTS = {
    "gender": "Unknown",
    "SeniorCitizen": 0,
    "Partner": "No",
    "Dependents": "No",
    "tenure": 0,
    "Phone": "",
    "Email": "",
    "Contract": "Unknown",
    "MonthlyCharges": 0.0,
    "TotalCharges": 0.0,
    "Churn": "No"
}

PII_COLS = {
    "Phone": "phone_hash",
    "Email": "email_hash",
    "customerID": "customer_id",
    "Name": "name_hash"  # if present
}


def sha256_hex(s: str) -> str:
    if s is None or str(s).strip() == "":
        return None
    return hashlib.sha256(str(s).encode('utf-8')).hexdigest()

def apply_defaults_and_anonymize(df: pd.DataFrame) -> pd.DataFrame:
    # fill defaults
    for col, default in DEFAULTS.items():
        if col in df.columns:
            df[col] = df[col].fillna(default)
    # anonymize PII
    if "Phone" in df.columns:
        df["phone_hash"] = df["Phone"].map(lambda x: sha256_hex(x))
    if "Email" in df.columns:
        df["email_hash"] = df["Email"].map(lambda x: sha256_hex(x))
    if "customerID" in df.columns:
        df["customer_id"] = df["customerID"].astype(str)
    # churn normalization to boolean
    if "Churn" in df.columns:
        df["churn"] = df["Churn"].apply(lambda x: True if str(x).strip().lower() in ("yes","y","true","1") else False)
    # SeniorCitizen numeric to bool
    if "SeniorCitizen" in df.columns:
        df["senior_citizen"] = df["SeniorCitizen"].apply(lambda x: bool(int(x)) if str(x).strip().isdigit() else False)
    # convert numeric columns
    for c in ["tenure","MonthlyCharges","TotalCharges"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    return df

def load_to_staging(file_path: str):
    logger.info("Loading file %s to staging", file_path)
    df = pd.read_csv(file_path)
    df = df.replace({np.nan: None})
    raw_jsons = df.to_dict(orient="records")
    with engine.begin() as conn:
        for r in raw_jsons:
            conn.execute(text("INSERT INTO staging.raw_customers (source_file, raw) VALUES (:f, :r)"),
                         [{"f": os.path.basename(file_path), "r": json.dumps(r)}])
    logger.info("Staged %d rows", len(raw_jsons))
    return df


def safe_int(val, default=0):
    try:
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return default
        return int(float(val))
    except (ValueError, TypeError):
        return default

def safe_float(val, default=0.0):
    try:
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return default
        return float(val)
    except (ValueError, TypeError):
        return default

def safe_str(val, default=None):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return default
    return str(val)

# ---- Main Transform & Load ----
def transform_and_load(df: pd.DataFrame, file_name: str):
    start = time.time()

    # Replace NaN with None for DB inserts
    df = df.replace({np.nan: None})

    rows_in = len(df)
    failed_rows = 0

    with engine.begin() as conn:
        for idx, row in df.iterrows():
            try:
                conn.execute(text("""
                    INSERT INTO reporting.customers 
                    (customer_id, gender, senior_citizen, partner, dependents, tenure,
                     phone_hash, email_hash, contract, monthly_charges, total_charges, churn, updated_at)
                    VALUES (:customer_id, :gender, :senior_citizen, :partner, :dependents, :tenure,
                            :phone_hash, :email_hash, :contract, :monthly_charges, :total_charges, :churn, :updated_at)
                    ON CONFLICT (customer_id) DO UPDATE SET
                      gender = EXCLUDED.gender,
                      senior_citizen = EXCLUDED.senior_citizen,
                      partner = EXCLUDED.partner,
                      dependents = EXCLUDED.dependents,
                      tenure = EXCLUDED.tenure,
                      phone_hash = EXCLUDED.phone_hash,
                      email_hash = EXCLUDED.email_hash,
                      contract = EXCLUDED.contract,
                      monthly_charges = EXCLUDED.monthly_charges,
                      total_charges = EXCLUDED.total_charges,
                      churn = EXCLUDED.churn,
                      updated_at = EXCLUDED.updated_at
                """), {
                    "customer_id": safe_str(row.get("customer_id")),
                    "gender": safe_str(row.get("gender")),
                    "senior_citizen": bool(row.get("senior_citizen")) if row.get("senior_citizen") is not None else False,
                    "partner": safe_str(row.get("Partner")),
                    "dependents": safe_str(row.get("Dependents")),
                    "tenure": safe_int(row.get("tenure")),
                    "phone_hash": safe_str(row.get("phone_hash")),
                    "email_hash": safe_str(row.get("email_hash")),
                    "contract": safe_str(row.get("Contract")),
                    "monthly_charges": safe_float(row.get("MonthlyCharges")),
                    "total_charges": safe_float(row.get("TotalCharges")),
                    "churn": bool(row.get("Churn")) if row.get("Churn") is not None else False,
                    "updated_at": datetime.utcnow()
                })
            except Exception as e:
                failed_rows += 1
                logger.error(f"Row {idx} failed: {e} | Data: {row.to_dict()}")

    # Log ETL stats
    duration = time.time() - start
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO etl.etl_logs (file_name, rows_in, rows_out, duration_seconds, notes)
            VALUES (:f, :rin, :rout, :dur, :notes)
        """), {
            "f": file_name,
            "rin": rows_in,
            "rout": rows_in - failed_rows,
            "dur": duration,
            "notes": f"Completed with {failed_rows} failed rows" if failed_rows else "OK"
        })

    logger.info(f"Transformed and loaded {rows_in - failed_rows}/{rows_in} rows in {duration:.2f}s")


def process_new_files():
    # look for CSV files in FEED_DIR; process each file once by moving to processed folder
    logger.info("Checking feed dir: %s", FEED_DIR)
    files = [f for f in os.listdir(FEED_DIR) if f.lower().endswith(".csv")]
    files.sort()
    if not files:
        logger.info("No files to process")
        return

    processed_dir = os.path.join(FEED_DIR, "processed")
    os.makedirs(processed_dir, exist_ok=True)

    for fname in files:
        fpath = os.path.join(FEED_DIR, fname)
        try:
            logger.info("Processing file %s", fpath)
            df = load_to_staging(fpath)
            transform_and_load(df, fname)
            # move file to processed
            dest = os.path.join(processed_dir, fname)
            os.replace(fpath, dest)
            logger.info("Moved processed file to %s", dest)
        except Exception as e:
            logger.exception("Failed to process %s: %s", fname, e)
            # log failure
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO etl.etl_logs (file_name, rows_in, rows_out, duration_seconds, notes)
                    VALUES (:f, :rin, :rout, :dur, :notes)
                """), {"f": fname, "rin": 0, "rout": 0, "dur": 0, "notes": f"ERROR: {str(e)[:200]}"})

def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(process_new_files, 'interval', minutes=INTERVAL_MINUTES, next_run_time=datetime.now())
    scheduler.start()
    logger.info("Scheduler started. Interval: %s minutes", INTERVAL_MINUTES)
    try:
        # keep the container alive
        while True:
            time.sleep(5)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

if __name__ == "__main__":
    logger.info("Starting ETL service. DB: %s", DB_URL)
    # Run once at startup
    process_new_files()
    # Then schedule
    start_scheduler()
