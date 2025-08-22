import sqlite3
import pandas as pd
import os

DB_PATH = "pipeline/final.db"
CSV_OUTPUT = "data/exported_customer_churn.csv"

def export_db_to_csv():
    if not os.path.exists(DB_PATH):
        print(f"❌ Database file not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql("SELECT * FROM customer_churn", conn)
        df.to_csv(CSV_OUTPUT, index=False)
        print(f"✅ Exported customer_churn table to {CSV_OUTPUT}")
    except Exception as e:
        print("❌ Failed to export CSV:", e)
    finally:
        conn.close()

if __name__ == "__main__":
    export_db_to_csv()
