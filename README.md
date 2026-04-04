# 💰 Financial ETL Pipeline

End-to-end **data engineering project** that processes financial transactions, builds a data warehouse, and delivers insights through a dashboard.

\---

## 📌 Overview

This project builds a complete **ETL pipeline** to process \~4.5M financial transactions.

* Automated daily pipeline using **Airflow**
* Data stored in **PostgreSQL**
* Data transformed and analyzed using **Python (Pandas)**
* Insights visualized in **Power BI**

**Goal:** turn raw transaction data into clean, reliable business insights.

\---

## ⚙️ Tech Stack

* **Airflow** → orchestration
* **PostgreSQL** → data warehouse
* **Python (Pandas)** → transformation
* **Docker** → environment setup
* **Power BI** → dashboard

\---

## 🏗️ Architecture

<img src="images/Diagram.png" width="800" alt="Architecture Diagram">

*Figure : System architecture showing data flow from CSV files through Airflow ETL to PostgreSQL data warehouse and finally to Power BI dashboard***Flow:**

1. Load raw CSV data
2. Run daily ETL pipeline
3. Clean \& transform data
4. Store aggregated results
5. Visualize in dashboard

\---

## 🔄 ETL Pipeline

**DAG Flow:**
extract → clean → aggregate → balance → quality\_check → load

### Key Logic

* Normalize currencies to USD
* Classify income vs expense
* Calculate daily totals
* Compute running balance
* Track data quality issues

\---

## 🗄️ Data Model

### Main Tables

* `raw\\\_transactions` → raw data
* `daily\\\_aggregates` → daily metrics
* `running\\\_balance` → cumulative balance
* `etl\\\_logs` → pipeline logs
* `data\\\_quality\\\_metrics` → validation

### Analytical Views

* `category\\\_country\\\_summary`
* `monthly\\\_category\\\_spending`

\---

## 📊 Dashboard

Built in **Power BI** with:

* KPI cards (Income, Expense, Cashflow, Balance)
* Running balance trend
* Income vs Expense comparison
* Category \& country analysis
* Monthly breakdown

\---

## 🚀 Setup

### 1\. Clone Repo

```bash
git clone https://github.com/your-username/financial-etl-pipeline.git
cd financial-etl-pipeline
```

### 2\. Start Services

```bash
docker-compose up -d
```

### 3\. Open Airflow

http://localhost:8080  
user: airflow  
pass: airflow

### 4\. Load Data

Run SQL script:

```sql
\\\\i sql/create\\\_tables.sql
```

Load CSV into `raw\\\_transactions`.

\---

## ▶️ Run Pipeline

Trigger DAG from Airflow UI  
or run backfill:

```bash
docker-compose exec airflow-webserver \\\\
airflow dags backfill -s 2025-01-01 -e 2025-01-31 etl\\\_daily\\\_pipeline
```

\---

## 📈 Usage

* Pipeline runs **daily (midnight)**
* Monitor via Airflow UI
* Query logs in PostgreSQL
* Refresh Power BI dashboard for latest data

\---

## 📁 Project Structure

financial\_etl\_project/
├── dags/
├── sql/
├── data/
├── scripts/
├── images/
├── docker-compose.yml
├── requirements.txt
└── README.md

\---

## 🔮 Future Improvements

* Forecasting (next 30 days)
* Real-time currency API
* Anomaly detection
* Alerts on failures

\---

## 👨‍💻 Author

Ahmed Fathi  
Data Engineer

\---

## ⭐ Support

If you found this useful, give it a ⭐ on GitHub.



