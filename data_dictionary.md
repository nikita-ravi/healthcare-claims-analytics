# Data Dictionary — Healthcare Claims Analytics

All tables live in the **RAW_DATA** schema inside the **HEALTHCARE_CLAIMS** database.

---

## Raw Tables (source layer)

### BENEFICIARY_SUMMARY
Patient demographics and annual cost summaries from CMS. All three yearly files (2008, 2009, 2010) are loaded into one table.

| Column | Type | Description |
|---|---|---|
| DESYNPUF_ID | VARCHAR | Unique synthetic beneficiary identifier |
| BENE_BIRTH_DT | INTEGER | Birth date as YYYYMMDD |
| BENE_DEATH_DT | INTEGER | Death date as YYYYMMDD (NULL if alive) |
| BENE_SEX_IDENT_CD | INTEGER | 1 = Male, 2 = Female |
| BENE_RACE_CD | INTEGER | 1 = White, 2 = Black, 3 = Other, 5 = Hispanic |
| SP_STATE_CODE | VARCHAR | 2-digit state FIPS code |
| BENE_COUNTY_CD | VARCHAR | 3-digit county FIPS code |
| SP_ALZHDMTA … SP_STRKETIA | INTEGER | Chronic condition flags: 1 = Yes, 2 = No |
| MEDREIMB_IP / OP / CAR | DECIMAL | Medicare reimbursement by setting (inpatient, outpatient, carrier) |

### INPATIENT_CLAIMS
Hospital admission claims. Each claim can have multiple SEGMENT rows (one per billing line item). Segments are rolled up in FACT_CLAIMS.

| Column | Type | Description |
|---|---|---|
| CLM_ID | BIGINT | Claim identifier (not unique in raw — repeated per segment) |
| SEGMENT | INTEGER | Billing line item number |
| CLM_PMT_AMT | DECIMAL | Total claim payment |
| CLM_UTLZTN_DAY_CNT | INTEGER | Length of stay in days |
| CLM_DRG_CD | VARCHAR | Diagnosis Related Group code |
| ICD9_DGNS_CD_1–10 | VARCHAR | Up to 10 ICD-9 diagnosis codes |
| ICD9_PRCDR_CD_1–6 | VARCHAR | Up to 6 ICD-9 procedure codes |
| PRVDR_NUM | VARCHAR | Provider identifier |

### OUTPATIENT_CLAIMS
Outpatient visits — same structure as inpatient minus admission date, DRG, and utilization days.

---

## Dimension Tables (star schema)

### DIM_DATE
Calendar lookup table covering 2008-01-01 through 2010-12-31 (1,096 rows). Generated — no source file needed.

| Column | Type | Description |
|---|---|---|
| date_key | INTEGER | Primary key. YYYYMMDD integer — joins directly to raw date columns |
| full_date | DATE | Standard date value |
| year / quarter / month | INTEGER | Calendar breakdowns |
| month_name / day_name | VARCHAR | Human-readable labels |
| is_weekend | BOOLEAN | TRUE if Saturday or Sunday |

### DIM_PATIENTS
One row per unique beneficiary (deduplicated across all three yearly files).

| Column | Type | Description |
|---|---|---|
| patient_key | INTEGER | Surrogate primary key (auto-increment) |
| patient_id | VARCHAR | Original DESYNPUF_ID |
| age_at_2010 | INTEGER | Calculated as 2010 − birth_year |
| gender / race | VARCHAR | Mapped from numeric codes |
| is_deceased | BOOLEAN | TRUE if death date is present |
| has_* (11 columns) | BOOLEAN | Chronic condition flags converted from 1/2 to TRUE/FALSE |
| chronic_condition_count | INTEGER | Sum of all 11 condition flags |
| total_cost | DECIMAL | Sum of inpatient + outpatient + carrier reimbursements |

### DIM_PROVIDERS
One row per provider, built by FULL OUTER JOIN of inpatient and outpatient provider lists.

| Column | Type | Description |
|---|---|---|
| provider_key | INTEGER | Surrogate primary key |
| provider_id | VARCHAR | Original PRVDR_NUM |
| provider_type | VARCHAR | "Inpatient Only", "Outpatient Only", or "Both" |
| total_claims | INTEGER | Combined claim count across both types |
| avg_claim_amount | DECIMAL | Average payment across all claims |

### DIM_DIAGNOSES
ICD-9 code lookup extracted from all diagnosis columns in both claim tables.

| Column | Type | Description |
|---|---|---|
| diagnosis_key | INTEGER | Surrogate primary key |
| icd9_code | VARCHAR | Original ICD-9 code |
| diagnosis_description | VARCHAR | Human-readable name (mapped for common codes; "Code: XXX" for others) |
| diagnosis_category | VARCHAR | Mapped by standard ICD-9 range (e.g. 390–459 → Circulatory System) |

---

## Fact Table

### FACT_CLAIMS
Central table. One row per claim (segments aggregated). Foreign keys link to all four dimensions.

| Column | Type | Description |
|---|---|---|
| claim_key | INTEGER | Surrogate primary key |
| claim_id | BIGINT | Original claim ID (unique in this table) |
| claim_type | VARCHAR | "Inpatient" or "Outpatient" |
| patient_key | INTEGER | FK → DIM_PATIENTS |
| provider_key | INTEGER | FK → DIM_PROVIDERS |
| primary_diagnosis_key | INTEGER | FK → DIM_DIAGNOSES (ICD9_DGNS_CD_1) |
| claim_from_date_key | INTEGER | FK → DIM_DATE |
| claim_thru_date_key | INTEGER | FK → DIM_DATE |
| admission_date_key | INTEGER | FK → DIM_DATE (NULL for outpatient) |
| claim_payment_amount | DECIMAL | Total payment on the claim |
| utilization_days | INTEGER | Length of stay (inpatient only) |
| segment_count | INTEGER | Number of billing segments rolled up |

---

## Mart Tables (pre-aggregated for Tableau)

### MART_MONTHLY_CLAIMS_SUMMARY
One row per (month × claim_type × diagnosis_category). Powers the monthly trend charts.

### MART_DIAGNOSIS_SUMMARY
One row per (diagnosis × claim_type). Includes rank_by_cost and rank_by_volume columns.

### MART_PROVIDER_PERFORMANCE
One row per inpatient provider. Includes pct_long_stays as a quality indicator.

---

## Monitoring Tables

### ETL_JOB_LOG
Logs every stored-procedure execution: job name, start/end time, status, row count, and any error message.

### DATA_QUALITY_LOG
Logs every quality check result: check name, pass/fail, records checked vs failed, and failure rate.
