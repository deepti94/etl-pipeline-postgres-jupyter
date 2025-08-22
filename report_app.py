import streamlit as st
import pandas as pd
import sqlite3

st.title("Customer Churn Report")

conn = sqlite3.connect("pipeline/final.db")
df = pd.read_sql("SELECT * FROM customer_churn", conn)

st.subheader("Overview")
st.write(df.head())

st.subheader("Churn Distribution")
st.bar_chart(df['Churn'].value_counts())

st.subheader("Internet Service Types")
st.bar_chart(df['InternetService'].value_counts())

conn.close()
