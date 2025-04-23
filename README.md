




# 🛒 Olist E-commerce Dataset: ETL Pipeline & Power BI Dashboard

This project demonstrates a complete ETL pipeline for the Olist dataset, integrating FastAPI, pandas, SQLAlchemy, and Power BI. The aim is to clean, transform, and load e-commerce data into a SQL Server database, and visualize key business insights with interactive dashboards.

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

### Python Libraries

Install required libraries:
```bash
pip install sqlalchemy
pip install fastapi uvicorn nest_asyncio
pip install requests






























------------------------------------------------------------
## Olist Ecommerce dataset ETL and PowerBI

## Table of Contents
- [Pipeline Explaination](#Pipeline)
- [Dataset](#Dataset)
- [Data Modeling](#DataModeling)
- [Installation](#Installation)
- [Run](#HowToRun)
- [PowerBI Dashboards](#Dashbaords)

## Pipeline
- Create API to get only 2 tables: order_reviews and order_payments
- The API return data as json files
- Fetch json and convert to pandas dataframe
- create sql server database using sqlAlchemy
- Extract data from csvs files (from your computer)
- Transform data:
  1. removing invalid pks and fks
  2. logging issues
  3. return clean data
- Load clean data into sql server database

## Dataset
- Dataset conatains 9 files upladed in this repo in folder: Raw_Data
- Download the files to your pc
- There is a file named olist_ddl.txt in this repo, it contains the DDL schema for olist dataset.
- Download it and put it in the same work directory. (This file is mandatory)
  

## DataModeling
The Olist dataset is modeled into several tables in the SQL database.
Below are the details:
![image](https://github.com/user-attachments/assets/a1a40abb-0fd0-4fe4-a13c-a5700fb20618)

## Installation
- Microsoft Sql Server
- VS code
- python 3
- Inside python or vs code or the env you will work in,  Install:
  1. !pip install sqlalchemy
  2. !pip install fastapi uvicorn nest_asyncio
  3. !pip install requests
- PowerBI

## HowToRun
- Download olist_etl.py
- Open in VS code
- Change the main unction with your paths
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

## Dashbaords

This dashboard delivers an in-depth analysis of Olist's sales and order data, covering the period from 2016 to 2018.
It highlights key insights into sales trends, product performance, customer behavior, and delivery efficiency—enabling stakeholders to make data-driven decisions and enhance business strategies.

![image](https://github.com/user-attachments/assets/92789a34-c7c0-418a-aaac-bfc65fe965b8)


![image](https://github.com/user-attachments/assets/84abceb0-51be-4413-ba13-8536e036f84e)
















