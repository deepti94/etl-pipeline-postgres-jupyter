import pandas as pd
import hashlib

def anonymize_customer_id(customer_id):
    return hashlib.md5(str(customer_id).encode()).hexdigest()

def transform_data(df):
    # Fill missing values
    df.fillna({
        'Gender': 'Other',
        'Age': df['Age'].median(),
        'Tenure': 0,
        'MonthlyCharges': df['MonthlyCharges'].median(),
        # Add more as needed
    }, inplace=True)

    # Anonymize PII
    df['CustomerID'] = df['CustomerID'].apply(anonymize_customer_id)
    df['Gender'] = 'Other'  # anonymize gender

    # Return transformed DataFrame
    return df
