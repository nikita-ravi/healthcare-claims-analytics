# 🏥 Healthcare Claims Analytics Pipeline
**End-to-End Snowflake Data Warehouse | CMS Medicare Claims | Tableau Dashboard**

An end-to-end ETL pipeline and dimensional data model built on **Snowflake**, processing **1.4M+ Medicare claims** from CMS DE-SynPUF synthetic data. Includes star schema modeling, automated stored-procedure-based ETL with error handling, data quality validation, pre-aggregated mart tables, and a Tableau executive dashboard delivering insights across **$638M in healthcare claims**.

---

## 📐 Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  SOURCE DATA                                                 │
│  CMS DE-SynPUF 2008–2010                                    │
│  Beneficiary Summary | Inpatient Claims | Outpatient Claims  │
└─────────────────────────┬────────────────────────────────────┘
                          │  CSV upload via Snowflake UI
                          ▼
┌──────────────────────────────────────────────────────────────┐
│  RAW LAYER  (RAW_DATA schema)                                │
│  BENEFICIARY_SUMMARY (343K) | INPATIENT_CLAIMS (66K)         │
│  OUTPATIENT_CLAIMS (1.19M)                                   │
└─────────────────────────┬────────────────────────────────────┘
                          │  SQL transformations
                          ▼
┌──────────────────────────────────────────────────────────────┐
│  DIMENSIONAL MODEL  (Star Schema)                            │
│                                                              │
│  DIM_DATE ─────┐                                             │
│  DIM_PATIENTS ─┼──► FACT_CLAIMS  (1.26M rows)               │
│  DIM_PROVIDERS─┤                                             │
│  DIM_DIAGNOSES─┘                                             │
└─────────────────────────┬────────────────────────────────────┘
                          │  Pre-aggregation
                          ▼
┌──────────────────────────────────────────────────────────────┐
│  MART LAYER                                                  │
│  MART_MONTHLY_CLAIMS_SUMMARY                                 │
│  MART_DIAGNOSIS_SUMMARY | MART_PROVIDER_PERFORMANCE          │
└─────────────────────────┬────────────────────────────────────┘
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
   ┌────────────┐  ┌────────────┐  ┌───────────────┐
   │  TABLEAU   │  │ ETL_JOB_LOG│  │DATA_QUALITY   │
   │ DASHBOARD  │  │ (logging)  │  │   _LOG        │
   └────────────┘  └────────────┘  └───────────────┘
```

---

## 📁 Project Structure

```
healthcare-claims-analytics/
├── README.md
├── sql/
│   ├── 01_setup.sql                 ← Database, warehouse setup
│   ├── 02_raw_tables.sql            ← Raw ingestion table definitions
│   ├── 03_dimensional_model.sql     ← Star schema (DIM + FACT)
│   ├── 04_mart_tables.sql           ← Pre-aggregated mart tables
│   ├── 05_etl_procedures.sql        ← Stored procedures + error handling
│   └── 06_data_quality.sql          ← Automated quality checks & logging
├── docs/
│   ├── data_dictionary.md           ← Every table & column documented
│   └── methodology.md               ← Design decisions explained
└── dashboards/
    └── README.md                    ← How to connect Tableau
```

---

## 🛠️ Tech Stack

| Tool | Role |
|---|---|
| Snowflake | Cloud data warehouse |
| SQL | ETL, transformations, modeling |
| CMS DE-SynPUF | Synthetic Medicare data source |
| Tableau Public | Executive dashboard |

---

## 🚀 How to Reproduce

### 1. Snowflake Account
Sign up at [signup.snowflake.com](https://signup.snowflake.com) — free 30-day trial, $400 credits. Choose **AWS → US East (N. Virginia)**.

### 2. Download CMS Data
From [CMS DE-SynPUF](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-claims-synthetic-public-use-files/cms-2008-2010-data-entrepreneurs-synthetic-public-use-file-de-synpuf), download Sample 1:
- Beneficiary Summary (2008, 2009, 2010 — all 3 files)
- Inpatient Claims Sample 1
- Outpatient Claims Sample 1

### 3. Run SQL Scripts In Order

| Order | File | What it does |
|---|---|---|
| 1 | 01_setup.sql | Creates database + warehouse |
| 2 | 02_raw_tables.sql | Creates raw tables |
| — | *Upload CSVs* | Via Snowflake UI → Data → Load data |
| 3 | 03_dimensional_model.sql | Builds star schema |
| 4 | 04_mart_tables.sql | Creates pre-aggregated marts |
| 5 | 05_etl_procedures.sql | Creates reusable ETL stored procedures |
| 6 | 06_data_quality.sql | Runs all validation checks |

### 4. Tableau
See dashboards/README.md for connection steps.

---

## 📊 Key Numbers

| Metric | Value |
|---|---|
| Total Claims | 1.26M |
| Total Cost | $638M |
| Unique Patients | 37,780 |
| Unique Providers | 2,675 |
| Avg Paid / Inpatient Claim | ~$10K |
| Avg Length of Stay | 5.6 days |

---

## 👤 Author
Nit | M.S. Data Analytics, George Washington University
