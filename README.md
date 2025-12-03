

# Olist E-commerce Dataset: ETL Pipeline & Power BI Dashboard

This project demonstrates a complete ETL pipeline for the Olist dataset, integrating FastAPI, pandas, SQLAlchemy, Microsoft SQL server and Power BI. The aim is to clean, transform, and load e-commerce data into a SQL Server database, and visualize key business insights with interactive dashboards.

---

## 📑 Table of Contents
- [🔁 ETL Pipeline Overview](#-etl-pipeline-overview)
- [📂 Dataset Description](#-dataset-description)
- [🗃️ Data Modeling](#️-data-modeling)
- [🛠️ Installation Requirements](#-installation-requirements)
- [🚀 How to Run](#-how-to-run)
- [📊 Power BI Dashboards](#-power-bi-dashboards)

---

## 🔁 ETL Pipeline Overview

- ✅ Build an API to return `order_reviews` and `order_payments` as JSON.
- ✅ Fetch API data and convert to pandas DataFrames.
- ✅ Create a SQL Server database using SQLAlchemy.
- ✅ Extract remaining data from local CSVs.
- ✅ Transform the data:
  - Remove invalid primary/foreign keys.
  - Log data quality issues.
  - Return cleaned datasets.
- ✅ Load cleaned data into the SQL Server database.
    
---

## 📂 Dataset Description

- The dataset includes **9 CSV files** stored in the `Raw_Data/` folder.
- A **`olist_ddl.txt`** file is also provided — it defines the database schema and is required for table creation.
- All files must be downloaded and saved locally in the same working directory.

---

## 🗃️ Data Modeling

The dataset is normalized into several SQL tables. Relationships among entities are defined by primary and foreign keys.

![Data Modeling Schema](https://github.com/user-attachments/assets/a1a40abb-0fd0-4fe4-a13c-a5700fb20618)

---

## 🛠️ Installation Requirements

Ensure the following tools are installed:

- **Microsoft SQL Server**
- **Visual Studio Code**
- **Python 3.7+**
- **PowerBI**

### Python Libraries

Install required libraries:
```bash
pip install sqlalchemy
pip install fastapi uvicorn nest_asyncio
pip install requests
```

## 🚀 How To Run
- Download olist_etl.py
- Open in VS code
- Change the main function with your paths
* Paths in main:
  1. api_dat: paths of reviews, payments csv files
  2. log paths:
     
     1."olist_raw" olist data folder path
     
     2."olist_ddl" : olist_ddl.txt
     
     3."record_log" your_path\log_file.csv, this file is created while running, contains the number of raw records and the number of records finally inserted into sql DB
     
     4."pk_log" : your_path\invalid_pks.csv", this file is created while running, contains the invalid pks in all tables
     
     5."fk_log" : your_path\invalid_fks.csv", this file is created while running, contains the invalid fks in all tables
     
     6."issues_log" : your_path\data_quality_issues.csv", this file is created while running, contains the data quality issues in all tables
- Click run
- ✅ etl_scheduler providd, download in the same work directory
- Open cmd in your directory:
  
    * In the cmd write: python etl_scheduler.
    * This will run etl scripts every (interval) of time.
    * You cam modify your interval time in the etl_scheduler

## 📊 Power BI Dashboards

This dashboard delivers an in-depth analysis of Olist's sales and order data, covering the period from 2016 to 2018.
It highlights key insights into sales trends, product performance, customer behavior, and delivery efficiency—enabling stakeholders to make data-driven decisions and enhance business strategies.

![image](page1.PNG)


![image](page2.PNG)

![image](page3.PNG)


















