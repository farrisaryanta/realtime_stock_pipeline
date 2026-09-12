# Real-Time Stock & Crypto Streaming Data Pipeline

An end-to-end real-time data streaming pipeline engineered to ingest live cryptocurrency and stock market trades, process micro-batches using Kafka and S3-compatible object storage (MinIO), load structured metrics into PostgreSQL, and deliver low-latency analytics via Power BI DirectQuery.

---

## 🚀 Project Overview

This project automates the extraction, transformation, loading (ETL/ELT), and real-time visualization of high-velocity financial market data. 

The architecture transitions live streaming data through a robust message broker and object storage before structuring it into a relational data warehouse, enabling automated ingestion, data reconciliation, and executive visual reporting in Power BI.

**Key Technologies Used:**
* **Streaming & Message Broker:** Apache Kafka, WebSockets (Finnhub API)
* **Data Lake & Storage:** MinIO (S3 API), Apache Parquet
* **Data Warehouse:** PostgreSQL (Dockerized)
* **Orchestration & Transformation:** Python 3.14+ (Pandas, SQLAlchemy, psycopg2)
* **Advanced Analytics:** SQL (DDL, Composite Indexes)
* **Business Intelligence:** Power BI Desktop (Interactive DirectQuery Dashboard)
* **Version Control:** Git & GitHub

---

## 📊 BI Dashboard Preview

### 1. Real-Time Market Trend Analytics
This dashboard connects directly to the production database to provide an executive overview of live cryptocurrency and stock market trades, updating automatically every 5 seconds to track latest transaction prices across multiple symbols.

![Real-Time Stock & Crypto Dashboard](assets/dashboard.jpg)

---

## 🏗️ Architecture & Data Pipeline

### 1. System Architecture Flow

```text
[ Financial Market WebSockets ]
             │
             ▼
    [ Python Ingestion ]
             │
             ▼
     [ Apache Kafka ] ──► (Stream Ingestion & Buffering)
             │
             ▼
    [ MinIO / S3 Lake ] ──► (Micro-batch Parquet Storage)
             │
             ▼
  [ Python ETL Engine ] ──► (Transformation & Deduplication)
             │
             ▼
   [ PostgreSQL Data ] ──► (Warehouse / `stock_analytics`)
             │
             ▼
  [ Power BI Dashboard ] ──► (DirectQuery Auto-Refresh)

---

### 2. Project Directory Structure

The pipeline follows a strict modular design pattern to separate concerns and ensure maintainability:

```text
realtime_stock_pipeline/
│
├── assets/                                 # Visual assets for documentation
│   └── dashboard.jpg
│
├── config.py                               # Centralized configuration & environment loader
├── docker-compose.yml                      # Container orchestration for Kafka, MinIO, Postgres
├── extract.py                              # WebSocket to Kafka ingestion script
├── transform.py                            # Kafka to MinIO transformation script
├── load.py                                 # MinIO Parquet to PostgreSQL loader
├── main.py                                 # Main execution script
├── pipeline_log.py                         # Custom logging configuration
├── table_stock_analyst.sql                 # DDL schema for PostgreSQL
│
├── stock_dashboard.pbix                    # Raw Power BI dashboard file
├── .gitignore                              # Git exclusion rules (.env, __pycache__, logs)
└── README.md                               # Project Documentation

---

## 👤 Author & Contact

**Developed by Farris Aryanta Loudy Prasetya**
* **Role:** Data Engineer & Analyst
* **Background:** Experienced in end-to-end data pipelines, ETL validation (Medallion Architecture), web scraping automation, and cloud/database environments (AWS, PostgreSQL).
* **Connect with me:** 
  * 📧 Email: faryanta18@gmail.com
  * 💼 LinkedIn: [linkedin.com/in/farrisaryanta](https://linkedin.com/in/farrisaryanta)
  * 🐙 GitHub: [github.com/farrisaryanta](https://github.com/farrisaryanta)