# Data Warehouse and Analytics Project with **Baraa**

This project presents a full end-to-end data warehousing and analytics ecosystem, covering everything from architecting the warehouse to producing meaningful, data-driven insights. Built as a portfolio showcase, it reflects established industry standards in modern data engineering and analytical design.

---
## 🏗️ Data Architecture

The data architecture for this project follows Medallion Architecture **Bronze**, **Silver**, and **Gold** layers:
![Data Architecture](docs/data_architecture.png)

1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics.

---
## 📖 Project Overview

This project involves:

1. **Data Architecture**: Designing a Modern Data Warehouse Using Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.


## 📂 Repository Structure
```
data-warehouse-project/
│
├── datasets/                           # Raw datasets used for the project (ERP and CRM data)
│   ├── source_CRM/                     # source folder for CRM files
│       ├── cust_info.csv               # customer information CSV file
│       ├── prd_info.csv                # product information CSV file
│       ├── sales_details.csv           # sales details CSV file
|
│   ├── source_CRM/                     # source folder for CRM files
│       ├── CUST_AZ12.csv               # customer information CSV file
│       ├── LOC_A101.csv                # location information CSV file
│       ├── PX_CAT_G1V2.csv             # product information CSV file
│
├── docs/                               # Project documentation and architecture details
│   ├── data_architecture.png           # png file shows the project's architecture
│   ├── data_catalog.md                 # Catalog of datasets, including field descriptions and metadata
│   ├── data_flow.drawio                # Draw.io file for the data flow diagram
│   ├── data_models.drawio              # Draw.io file for data models (star schema)
│   ├── naming-conventions.md           # Consistent naming guidelines for tables, columns, and files
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for extracting and loading raw data
│   ├── silver/                         # Scripts for cleaning and transforming data
│   ├── gold/                           # Scripts for creating analytical models
│   ├── intial_database.sql             # Scripts for creating database and schemas
│
├── tests/                              # Test scripts and quality files
│
└── README.md                           # Project overview and instructions
```
---

# Data Warehousing Notes

A concise set of notes I created after watching two advanced YouTube tutorials by **Ansh Lamba** and completing a portion of the **"Data Warehouse - The Ultimate Guide"** course on Udemy. These notes cover Data Warehousing, Dimensional Data Modeling, ETL architecture, Incremental Loading, and SCD implementations. All notes are now part of my GitHub repository so I can revisit the concepts anytime and expand my collection as I continue learning. 🚀

## 📘 What I Watched / Studied

1. **[Data Warehouse – The Ultimate Guide [2025] (YouTube)](https://www.youtube.com/watch?v=HKcEyHF1U00&t=8939s)**
2. **[Data Modeling Masterclass for Data Engineers [2025] (YouTube)](https://www.youtube.com/watch?v=K7C1sWKQU-o)**
3. **[Data Warehouse - The Ultimate Guide (Udemy)](https://www.udemy.com/course/data-warehouse-the-ultimate-guide/)** — notes uploaded for the completed sections.

## 📂 GitHub

The full notes are available in this repository, and I will keep adding more resources and learning materials as I progress through my Data Engineering journey.

### 📄 Notes Documents

- **[YouTube Notes (PDF)](Data%20warehousing%20and%20data%20modeling.pdf)**
- **[Udemy Notes (PDF)](Data%20warehouse.pdf)**
