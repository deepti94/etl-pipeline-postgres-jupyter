from pipeline.extract import extract_csv
from pipeline.transform import transform_data
from pipeline.load import load_to_db
import sqlite3
import pandas as pd

def run_pipeline():
    df = extract_csv()
    print(f"Extracted {len(df)} rows from CSV.")

    transformed = transform_data(df)
    print(f"✅ Transformed data contains {len(transformed)} rows.")

    load_to_db(transformed)
    print("✅ Data written to pipeline/final.db")

    # Sanity check: read 5 rows back
    conn = sqlite3.connect("pipeline/final.db")
    sample = pd.read_sql("SELECT * FROM customer_churn LIMIT 5", conn)
    conn.close()

    print("✅ Sample rows from final.db:")
    print(sample)

    print("⏱️ ETL pipeline ran at", pd.Timestamp.now())


if __name__ == "__main__":
    run_pipeline()
