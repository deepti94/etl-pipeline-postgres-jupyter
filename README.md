# Customer Churn ETL Pipeline

This project implements a lightweight ETL pipeline for ingesting, transforming, and loading a customer churn dataset. It includes data anonymization, missing value handling, and visualization via Streamlit. The pipeline can be scheduled to run periodically using a simple scheduler.

---

## Features

- Ingests CSV telecom churn data  
- Loads raw data into a SQLite staging database  
- Transforms data by handling missing values and anonymizing PII  
- Loads transformed data into a reporting SQLite database  
- Exports final data to CSV for external use  
- Generates basic reports and visualizations using Streamlit  
- Scheduling of the ETL pipeline every 60 seconds using APScheduler  
- Logging of ETL runs and errors  

---

## Tech Stack

- Python 3.8+  
- pandas  
- SQLite  
- Streamlit (for visualization)  
- APScheduler (for scheduling)  

---

## Setup

1. **Clone this repository**

```bash
git clone <repo_url>
cd <repo_folder>
```

2. **Create and activate a virtual environment**

```bash
python3 -m venv venv
source venv/bin/activate    # Linux/Mac
venv\Scripts\activate       # Windows
```

3. **Install dependencies**

```bash
pip install -r requirements.txt
```
