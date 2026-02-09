-- =============================================================================
-- 03_DIMENSIONAL_MODEL.SQL
-- Healthcare Claims Analytics — Star Schema
--
-- DIM_DATE        → calendar lookup (2008-2010)
-- DIM_PATIENTS    → patient demographics + chronic conditions
-- DIM_PROVIDERS   → provider summary aggregated from both claim types
-- DIM_DIAGNOSES   → ICD-9 code lookup with category mapping
-- FACT_CLAIMS     → one row per claim (segments rolled up via GROUP BY)
-- =============================================================================

USE DATABASE HEALTHCARE_CLAIMS;
USE SCHEMA RAW_DATA;

-- =============================================================================
-- DIM_DATE
-- Generated using Snowflake's GENERATOR — no source table needed.
-- date_key is YYYYMMDD integer so it joins directly to raw claim date columns.
-- =============================================================================
CREATE OR REPLACE TABLE DIM_DATE (
    date_key            INTEGER PRIMARY KEY,
    full_date           DATE,
    year                INTEGER,
    quarter             INTEGER,
    month               INTEGER,
    month_name          VARCHAR(20),
    week                INTEGER,
    day_of_month        INTEGER,
    day_of_week         INTEGER,
    day_name            VARCHAR(20),
    is_weekend          BOOLEAN,
    fiscal_year         INTEGER,
    fiscal_quarter      INTEGER
);

INSERT INTO DIM_DATE
SELECT
    TO_NUMBER(TO_CHAR(dateadd(day, value, '2008-01-01'::DATE), 'YYYYMMDD')),
    dateadd(day, value, '2008-01-01'::DATE),
    YEAR(dateadd(day, value, '2008-01-01'::DATE)),
    QUARTER(dateadd(day, value, '2008-01-01'::DATE)),
    MONTH(dateadd(day, value, '2008-01-01'::DATE)),
    MONTHNAME(dateadd(day, value, '2008-01-01'::DATE)),
    WEEKOFYEAR(dateadd(day, value, '2008-01-01'::DATE)),
    DAYOFMONTH(dateadd(day, value, '2008-01-01'::DATE)),
    DAYOFWEEK(dateadd(day, value, '2008-01-01'::DATE)),
    DAYNAME(dateadd(day, value, '2008-01-01'::DATE)),
    DAYOFWEEK(dateadd(day, value, '2008-01-01'::DATE)) IN (0, 6),
    YEAR(dateadd(day, value, '2008-01-01'::DATE)),
    QUARTER(dateadd(day, value, '2008-01-01'::DATE))
FROM TABLE(GENERATOR(ROWCOUNT => 1096))
WHERE dateadd(day, value, '2008-01-01'::DATE) <= '2010-12-31'::DATE;

-- =============================================================================
-- DIM_PATIENTS
-- One row per unique beneficiary.
-- Chronic condition codes (1=Yes, 2=No) are converted to BOOLEAN.
-- Chronic_condition_count sums up the 11 condition flags.
-- =============================================================================
CREATE OR REPLACE TABLE DIM_PATIENTS (
    patient_key                         INTEGER AUTOINCREMENT PRIMARY KEY,
    patient_id                          VARCHAR(16) UNIQUE,
    birth_year                          INTEGER,
    death_year                          INTEGER,
    age_at_2010                         INTEGER,
    gender                              VARCHAR(10),
    race                                VARCHAR(50),
    state_code                          VARCHAR(2),
    county_code                         VARCHAR(3),
    is_deceased                         BOOLEAN,
    has_alzheimers                      BOOLEAN,
    has_heart_failure                   BOOLEAN,
    has_chronic_kidney_disease          BOOLEAN,
    has_cancer                          BOOLEAN,
    has_copd                            BOOLEAN,
    has_depression                      BOOLEAN,
    has_diabetes                        BOOLEAN,
    has_ischemic_heart_disease          BOOLEAN,
    has_osteoporosis                    BOOLEAN,
    has_rheumatoid_arthritis            BOOLEAN,
    has_stroke                          BOOLEAN,
    total_hi_coverage_months            INTEGER,
    total_smi_coverage_months           INTEGER,
    total_hmo_coverage_months           INTEGER,
    total_inpatient_reimbursement       DECIMAL(10,2),
    total_outpatient_reimbursement      DECIMAL(10,2),
    total_carrier_reimbursement         DECIMAL(10,2),
    total_cost                          DECIMAL(10,2),
    chronic_condition_count             INTEGER
);

INSERT INTO DIM_PATIENTS (
    patient_id, birth_year, death_year, age_at_2010, gender, race,
    state_code, county_code, is_deceased,
    has_alzheimers, has_heart_failure, has_chronic_kidney_disease,
    has_cancer, has_copd, has_depression, has_diabetes,
    has_ischemic_heart_disease, has_osteoporosis, has_rheumatoid_arthritis, has_stroke,
    total_hi_coverage_months, total_smi_coverage_months, total_hmo_coverage_months,
    total_inpatient_reimbursement, total_outpatient_reimbursement,
    total_carrier_reimbursement, total_cost, chronic_condition_count
)
SELECT DISTINCT
    DESYNPUF_ID,
    FLOOR(BENE_BIRTH_DT / 10000),
    FLOOR(BENE_DEATH_DT / 10000),
    2010 - FLOOR(BENE_BIRTH_DT / 10000),
    CASE BENE_SEX_IDENT_CD WHEN 1 THEN 'Male' WHEN 2 THEN 'Female' ELSE 'Unknown' END,
    CASE BENE_RACE_CD WHEN 1 THEN 'White' WHEN 2 THEN 'Black' WHEN 3 THEN 'Other' WHEN 5 THEN 'Hispanic' ELSE 'Unknown' END,
    SP_STATE_CODE,
    BENE_COUNTY_CD,
    BENE_DEATH_DT IS NOT NULL,
    SP_ALZHDMTA = 1, SP_CHF = 1, SP_CHRNKIDN = 1,
    SP_CNCR = 1, SP_COPD = 1, SP_DEPRESSN = 1, SP_DIABETES = 1,
    SP_ISCHMCHT = 1, SP_OSTEOPRS = 1, SP_RA_OA = 1, SP_STRKETIA = 1,
    BENE_HI_CVRAGE_TOT_MONS,
    BENE_SMI_CVRAGE_TOT_MONS,
    BENE_HMO_CVRAGE_TOT_MONS,
    MEDREIMB_IP, MEDREIMB_OP, MEDREIMB_CAR,
    COALESCE(MEDREIMB_IP,0) + COALESCE(MEDREIMB_OP,0) + COALESCE(MEDREIMB_CAR,0),
    (CASE WHEN SP_ALZHDMTA=1 THEN 1 ELSE 0 END +
     CASE WHEN SP_CHF=1       THEN 1 ELSE 0 END +
     CASE WHEN SP_CHRNKIDN=1  THEN 1 ELSE 0 END +
     CASE WHEN SP_CNCR=1      THEN 1 ELSE 0 END +
     CASE WHEN SP_COPD=1      THEN 1 ELSE 0 END +
     CASE WHEN SP_DEPRESSN=1  THEN 1 ELSE 0 END +
     CASE WHEN SP_DIABETES=1  THEN 1 ELSE 0 END +
     CASE WHEN SP_ISCHMCHT=1  THEN 1 ELSE 0 END +
     CASE WHEN SP_OSTEOPRS=1  THEN 1 ELSE 0 END +
     CASE WHEN SP_RA_OA=1     THEN 1 ELSE 0 END +
     CASE WHEN SP_STRKETIA=1  THEN 1 ELSE 0 END)
FROM RAW_DATA.BENEFICIARY_SUMMARY;

-- =============================================================================
-- DIM_PROVIDERS
-- One row per provider, derived from both inpatient and outpatient claims
-- using a FULL OUTER JOIN on provider number.
-- =============================================================================
CREATE OR REPLACE TABLE DIM_PROVIDERS (
    provider_key                INTEGER AUTOINCREMENT PRIMARY KEY,
    provider_id                 VARCHAR(6) UNIQUE,
    provider_type               VARCHAR(50),        -- Inpatient Only / Outpatient Only / Both
    total_claims                INTEGER,
    total_inpatient_claims      INTEGER,
    total_outpatient_claims     INTEGER,
    avg_claim_amount            DECIMAL(10,2),
    total_payment_amount        DECIMAL(10,2)
);

INSERT INTO DIM_PROVIDERS (
    provider_id, provider_type, total_claims,
    total_inpatient_claims, total_outpatient_claims,
    avg_claim_amount, total_payment_amount
)
WITH ip AS (
    SELECT PRVDR_NUM, COUNT(*) as cnt, SUM(CLM_PMT_AMT) as total
    FROM INPATIENT_CLAIMS WHERE PRVDR_NUM IS NOT NULL
    GROUP BY PRVDR_NUM
),
op AS (
    SELECT PRVDR_NUM, COUNT(*) as cnt, SUM(CLM_PMT_AMT) as total
    FROM OUTPATIENT_CLAIMS WHERE PRVDR_NUM IS NOT NULL
    GROUP BY PRVDR_NUM
)
SELECT
    COALESCE(ip.PRVDR_NUM, op.PRVDR_NUM),
    CASE
        WHEN ip.PRVDR_NUM IS NOT NULL AND op.PRVDR_NUM IS NOT NULL THEN 'Both'
        WHEN ip.PRVDR_NUM IS NOT NULL THEN 'Inpatient Only'
        ELSE 'Outpatient Only'
    END,
    COALESCE(ip.cnt,0) + COALESCE(op.cnt,0),
    COALESCE(ip.cnt,0),
    COALESCE(op.cnt,0),
    (COALESCE(ip.total,0) + COALESCE(op.total,0)) /
        NULLIF(COALESCE(ip.cnt,0) + COALESCE(op.cnt,0), 0),
    COALESCE(ip.total,0) + COALESCE(op.total,0)
FROM ip FULL OUTER JOIN op ON ip.PRVDR_NUM = op.PRVDR_NUM;

-- =============================================================================
-- DIM_DIAGNOSES
-- Unique ICD-9 codes extracted from all diagnosis columns across both tables.
-- Category is mapped by standard ICD-9 numeric ranges.
-- =============================================================================
CREATE OR REPLACE TABLE DIM_DIAGNOSES (
    diagnosis_key           INTEGER AUTOINCREMENT PRIMARY KEY,
    icd9_code               VARCHAR(7) UNIQUE,
    diagnosis_description   VARCHAR(200),
    diagnosis_category      VARCHAR(100)
);

INSERT INTO DIM_DIAGNOSES (icd9_code, diagnosis_description, diagnosis_category)
WITH all_codes AS (
    SELECT DISTINCT ICD9_DGNS_CD_1 as code FROM INPATIENT_CLAIMS  WHERE ICD9_DGNS_CD_1  IS NOT NULL
    UNION SELECT DISTINCT ICD9_DGNS_CD_2  FROM INPATIENT_CLAIMS  WHERE ICD9_DGNS_CD_2  IS NOT NULL
    UNION SELECT DISTINCT ICD9_DGNS_CD_3  FROM INPATIENT_CLAIMS  WHERE ICD9_DGNS_CD_3  IS NOT NULL
    UNION SELECT DISTINCT ICD9_DGNS_CD_4  FROM INPATIENT_CLAIMS  WHERE ICD9_DGNS_CD_4  IS NOT NULL
    UNION SELECT DISTINCT ICD9_DGNS_CD_5  FROM INPATIENT_CLAIMS  WHERE ICD9_DGNS_CD_5  IS NOT NULL
    UNION SELECT DISTINCT ICD9_DGNS_CD_1  FROM OUTPATIENT_CLAIMS WHERE ICD9_DGNS_CD_1  IS NOT NULL
    UNION SELECT DISTINCT ICD9_DGNS_CD_2  FROM OUTPATIENT_CLAIMS WHERE ICD9_DGNS_CD_2  IS NOT NULL
    UNION SELECT DISTINCT ICD9_DGNS_CD_3  FROM OUTPATIENT_CLAIMS WHERE ICD9_DGNS_CD_3  IS NOT NULL
)
SELECT
    code,
    CASE
        WHEN code LIKE '250%' THEN 'Diabetes Mellitus'
        WHEN code LIKE '428%' THEN 'Heart Failure'
        WHEN code LIKE '401%' THEN 'Hypertension'
        WHEN code LIKE '414%' THEN 'Ischemic Heart Disease'
        WHEN code LIKE '496%' THEN 'COPD'
        WHEN code LIKE '585%' THEN 'Chronic Kidney Disease'
        WHEN code LIKE 'V%'   THEN 'Supplementary Code V' || code
        WHEN code LIKE 'E%'   THEN 'External Cause E' || code
        ELSE 'Code: ' || code
    END,
    CASE
        WHEN code BETWEEN '390' AND '459' THEN 'Circulatory System'
        WHEN code BETWEEN '250' AND '259' THEN 'Endocrine/Metabolic'
        WHEN code BETWEEN '460' AND '519' THEN 'Respiratory System'
        WHEN code BETWEEN '580' AND '629' THEN 'Genitourinary System'
        WHEN code BETWEEN '140' AND '239' THEN 'Neoplasms'
        WHEN code LIKE 'V%'              THEN 'V Codes'
        WHEN code LIKE 'E%'              THEN 'E Codes'
        ELSE 'Other'
    END
FROM all_codes;

-- =============================================================================
-- FACT_CLAIMS
-- Central fact table. One row per claim.
-- Each raw claim has multiple SEGMENT rows (billing line items).
-- We GROUP BY claim_id and use MAX() for single-value fields and COUNT(DISTINCT)
-- for segment_count so every claim appears exactly once.
-- =============================================================================
CREATE OR REPLACE TABLE FACT_CLAIMS (
    claim_key                   INTEGER AUTOINCREMENT PRIMARY KEY,
    claim_id                    BIGINT UNIQUE,
    claim_type                  VARCHAR(20),            -- 'Inpatient' or 'Outpatient'
    -- Foreign keys
    patient_key                 INTEGER REFERENCES DIM_PATIENTS(patient_key),
    provider_key                INTEGER REFERENCES DIM_PROVIDERS(provider_key),
    primary_diagnosis_key       INTEGER REFERENCES DIM_DIAGNOSES(diagnosis_key),
    claim_from_date_key         INTEGER REFERENCES DIM_DATE(date_key),
    claim_thru_date_key         INTEGER REFERENCES DIM_DATE(date_key),
    admission_date_key          INTEGER,                -- NULL for outpatient
    -- Measures
    claim_payment_amount        DECIMAL(10,2),
    primary_payer_amount        DECIMAL(10,2),
    deductible_amount           DECIMAL(10,2),
    coinsurance_amount          DECIMAL(10,2),
    utilization_days            INTEGER,                -- Length of stay (inpatient)
    drg_code                    VARCHAR(3),
    segment_count               INTEGER,
    -- Physicians
    attending_physician_npi     VARCHAR(10),
    operating_physician_npi     VARCHAR(10),
    other_physician_npi         VARCHAR(10)
);

-- --- Load inpatient claims ---
INSERT INTO FACT_CLAIMS (
    claim_id, claim_type,
    patient_key, provider_key, primary_diagnosis_key,
    claim_from_date_key, claim_thru_date_key, admission_date_key,
    claim_payment_amount, primary_payer_amount, deductible_amount, coinsurance_amount,
    utilization_days, drg_code, segment_count,
    attending_physician_npi, operating_physician_npi, other_physician_npi
)
SELECT
    i.CLM_ID,
    'Inpatient',
    p.patient_key,
    prov.provider_key,
    d.diagnosis_key,
    MAX(i.CLM_FROM_DT),
    MAX(i.CLM_THRU_DT),
    MAX(i.CLM_ADMSN_DT),
    MAX(i.CLM_PMT_AMT),
    MAX(i.NCH_PRMRY_PYR_CLM_PD_AMT),
    MAX(i.NCH_BENE_IP_DDCTBL_AMT),
    MAX(i.NCH_BENE_PTA_COINSRNC_LBLTY_AM),
    MAX(i.CLM_UTLZTN_DAY_CNT),
    MAX(i.CLM_DRG_CD),
    COUNT(DISTINCT i.SEGMENT),
    MAX(i.AT_PHYSN_NPI),
    MAX(i.OP_PHYSN_NPI),
    MAX(i.OT_PHYSN_NPI)
FROM INPATIENT_CLAIMS i
LEFT JOIN DIM_PATIENTS  p    ON i.DESYNPUF_ID = p.patient_id
LEFT JOIN DIM_PROVIDERS prov ON i.PRVDR_NUM   = prov.provider_id
LEFT JOIN DIM_DIAGNOSES d    ON i.ICD9_DGNS_CD_1 = d.icd9_code
GROUP BY i.CLM_ID, p.patient_key, prov.provider_key, d.diagnosis_key;

-- --- Load outpatient claims ---
INSERT INTO FACT_CLAIMS (
    claim_id, claim_type,
    patient_key, provider_key, primary_diagnosis_key,
    claim_from_date_key, claim_thru_date_key,
    claim_payment_amount, primary_payer_amount, deductible_amount,
    segment_count
)
SELECT
    o.CLM_ID,
    'Outpatient',
    p.patient_key,
    prov.provider_key,
    d.diagnosis_key,
    MAX(o.CLM_FROM_DT),
    MAX(o.CLM_THRU_DT),
    MAX(o.CLM_PMT_AMT),
    MAX(o.NCH_PRMRY_PYR_CLM_PD_AMT),
    MAX(o.NCH_BENE_PTB_DDCTBL_AMT),
    COUNT(DISTINCT o.SEGMENT)
FROM OUTPATIENT_CLAIMS o
LEFT JOIN DIM_PATIENTS  p    ON o.DESYNPUF_ID = p.patient_id
LEFT JOIN DIM_PROVIDERS prov ON o.PRVDR_NUM   = prov.provider_id
LEFT JOIN DIM_DIAGNOSES d    ON o.ICD9_DGNS_CD_1 = d.icd9_code
GROUP BY o.CLM_ID, p.patient_key, prov.provider_key, d.diagnosis_key;

-- =============================================================================
-- Verification
-- =============================================================================
SELECT table_name, row_count FROM (
    SELECT 'DIM_DATE'       as table_name, COUNT(*) as row_count FROM DIM_DATE
    UNION ALL SELECT 'DIM_PATIENTS',   COUNT(*) FROM DIM_PATIENTS
    UNION ALL SELECT 'DIM_PROVIDERS',  COUNT(*) FROM DIM_PROVIDERS
    UNION ALL SELECT 'DIM_DIAGNOSES',  COUNT(*) FROM DIM_DIAGNOSES
    UNION ALL SELECT 'FACT_CLAIMS',    COUNT(*) FROM FACT_CLAIMS
) ORDER BY table_name;
