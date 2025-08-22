import pandas as pd
import sqlite3

def load_to_db(df: pd.DataFrame, db='pipeline/final.db', table='customer_churn'):
    conn = sqlite3.connect(db)
    df.to_sql(table, conn, if_exists='replace', index=False)
    conn.close()
