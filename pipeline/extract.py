import pandas as pd

def extract_csv(path='data/customer_churn.csv') -> pd.DataFrame:
    return pd.read_csv(path)
