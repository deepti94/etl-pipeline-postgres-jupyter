# HG Insight ETL Assignment

## 📌 Overview
This project implements an ETL (Extract, Transform, Load) pipeline to process customer data feeds every hour and load them into a PostgreSQL database for reporting.  
Originally, the project used Metabase for reporting, but to make setup faster and lighter, we replaced it with **Jupyter Notebook** for generating statistics and visualizations.

---

## 🛠 Tech Stack
- **Python** — ETL pipeline implementation
- **Pandas** — Data transformation and analysis
- **SQLAlchemy** — Database connection
- **PostgreSQL** — Staging & reporting database
- **Jupyter Notebook** — Reporting & visualization (replaces Metabase)
- **Docker Compose** — Orchestration of services

---

## 📂 Project Structure
```bash
.
├── docker-compose.yml # Orchestration for Postgres, ETL, and Jupyter
├── etl/ # ETL service code
│ ├── app.py # Main ETL logic
│ ├── requirements.txt # Python dependencies
│ └── dockerfile # Docker image for ETL
├── feed/ # Input folder for CSV feeds
├── sql/ # SQL init scripts for DB schema
├── notebooks/ # Jupyter notebooks for reporting
│ └── reporting.ipynb # Example dashboard
└── README.md

```



## ⚙️ How It Works
1. **Feed Ingestion**  
   - New CSV files are dropped into the `/feed` directory.
   - ETL service picks them up every `INTERVAL_MINUTES` (configurable via `.env` or `docker-compose.yml`).

2. **Staging**  
   - Raw data is loaded into `staging.raw_customers` in PostgreSQL for backup and auditing.

3. **Transformation**  
   - Missing values are handled.
   - Data is cleaned and standardized.
   - Tenure is safely converted to integers even if NaN or blank.

4. **Loading**  
   - Cleaned data is loaded into `reporting.customers`.

5. **Reporting**  
   - Instead of Metabase, we use **Jupyter Notebook** to connect directly to Postgres and run:
     - SQL queries
     - Data summaries
     - Visualizations using Seaborn and Matplotlib

---

## 🚀 Setup & Run

```bash

1️⃣ Start the services

docker compose up --build

This starts:

PostgreSQL database

ETL pipeline service

Jupyter Notebook server (on port 8888)


2️⃣ Access Jupyter Notebook
After containers start, you will see a Jupyter URL like:

http://127.0.0.1:8888/?token=...

Open this in your browser.


3️⃣ Example Reporting Query
Inside Jupyter Notebbok

import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://etl_user:etl_pass@postgres:5432/etl_db")
df = pd.read_sql("SELECT * FROM reporting.customers LIMIT 10", engine)
df.head()


4️⃣ Example Visualization

import seaborn as sns
import matplotlib.pyplot as plt

plt.figure(figsize=(6,4))
sns.countplot(data=df, x="contract")
plt.title("Contract Distribution")
plt.show()

```

⚙️ Configuration
Edit docker-compose.yml to change:

INTERVAL_MINUTES — how often ETL checks for new files

FEED_DIR — location of incoming data




