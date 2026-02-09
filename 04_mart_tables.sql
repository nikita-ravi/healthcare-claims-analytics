-- =============================================================================
-- 04_MART_TABLES.SQL
-- Healthcare Claims Analytics — Pre-Aggregated Mart Tables
--
-- These tables flatten and pre-aggregate the star schema so Tableau queries
-- run fast.  Rebuild them any time the fact/dimension data changes.
--
-- MART_MONTHLY_CLAIMS_SUMMARY   → powers trend charts
-- MART_DIAGNOSIS_SUMMARY        → powers diagnosis bar charts
-- MART_PROVIDER_PERFORMANCE     → powers provider analysis
-- =============================================================================

USE DATABASE HEALTHCARE_CLAIMS;
USE SCHEMA RAW_DATA;

-- =============================================================================
-- MART_MONTHLY_CLAIMS_SUMMARY
-- One row per (month, claim_type, diagnosis_category).
-- =============================================================================
CREATE OR REPLACE TABLE MART_MONTHLY_CLAIMS_SUMMARY AS
SELECT
    d.year,
    d.month,
    d.month_name,
    DATE_TRUNC('month', d.full_date)        AS month_date,
    f.claim_type,
    diag.diagnosis_category,

    COUNT(DISTINCT f.claim_key)             AS claim_count,
    COUNT(DISTINCT f.patient_key)           AS unique_patients,
    SUM(f.claim_payment_amount)             AS total_paid,
    AVG(f.claim_payment_amount)             AS avg_paid_per_claim,
    AVG(f.utilization_days)                 AS avg_length_of_stay,
    SUM(f.utilization_days)                 AS total_utilization_days

FROM FACT_CLAIMS f
JOIN DIM_DATE     d    ON f.claim_from_date_key        = d.date_key
LEFT JOIN DIM_DIAGNOSES diag ON f.primary_diagnosis_key = diag.diagnosis_key
GROUP BY
    d.year, d.month, d.month_name,
    DATE_TRUNC('month', d.full_date),
    f.claim_type,
    diag.diagnosis_category;

-- =============================================================================
-- MART_DIAGNOSIS_SUMMARY
-- One row per (icd9_code, claim_type).  Includes rank columns so you can
-- easily pull "Top 10 diagnoses by cost" without a subquery.
-- =============================================================================
CREATE OR REPLACE TABLE MART_DIAGNOSIS_SUMMARY AS
SELECT
    diag.icd9_code,
    diag.diagnosis_description,
    diag.diagnosis_category,
    f.claim_type,

    COUNT(DISTINCT f.claim_key)             AS claim_count,
    COUNT(DISTINCT f.patient_key)           AS patient_count,
    SUM(f.claim_payment_amount)             AS total_paid,
    AVG(f.claim_payment_amount)             AS avg_paid_per_claim,
    AVG(f.utilization_days)                 AS avg_length_of_stay,

    ROW_NUMBER() OVER (
        PARTITION BY f.claim_type
        ORDER BY SUM(f.claim_payment_amount) DESC
    )                                       AS rank_by_cost,

    ROW_NUMBER() OVER (
        PARTITION BY f.claim_type
        ORDER BY COUNT(*) DESC
    )                                       AS rank_by_volume

FROM FACT_CLAIMS f
JOIN DIM_DIAGNOSES diag ON f.primary_diagnosis_key = diag.diagnosis_key
GROUP BY
    diag.icd9_code, diag.diagnosis_description, diag.diagnosis_category,
    f.claim_type;

-- =============================================================================
-- MART_PROVIDER_PERFORMANCE
-- One row per provider (inpatient focus).  Includes % long stays as a
-- simple quality-of-care indicator.
-- =============================================================================
CREATE OR REPLACE TABLE MART_PROVIDER_PERFORMANCE AS
SELECT
    prov.provider_id,
    prov.provider_type,

    COUNT(DISTINCT f.claim_key)             AS total_claims,
    COUNT(DISTINCT f.patient_key)           AS unique_patients,
    SUM(f.claim_payment_amount)             AS total_revenue,
    AVG(f.claim_payment_amount)             AS avg_claim_amount,
    AVG(f.utilization_days)                 AS avg_length_of_stay,

    ROUND(
        SUM(CASE WHEN f.utilization_days > 7 THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0),
    2)                                      AS pct_long_stays,

    ROW_NUMBER() OVER (ORDER BY SUM(f.claim_payment_amount) DESC) AS revenue_rank,
    ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC)                    AS volume_rank

FROM FACT_CLAIMS f
JOIN DIM_PROVIDERS prov ON f.provider_key = prov.provider_key
WHERE f.claim_type = 'Inpatient'
GROUP BY prov.provider_id, prov.provider_type;

-- =============================================================================
-- Verify
-- =============================================================================
SELECT table_name, row_count FROM (
    SELECT 'MART_MONTHLY_CLAIMS_SUMMARY'  as table_name, COUNT(*) as row_count FROM MART_MONTHLY_CLAIMS_SUMMARY
    UNION ALL SELECT 'MART_DIAGNOSIS_SUMMARY',     COUNT(*) FROM MART_DIAGNOSIS_SUMMARY
    UNION ALL SELECT 'MART_PROVIDER_PERFORMANCE',  COUNT(*) FROM MART_PROVIDER_PERFORMANCE
) ORDER BY table_name;
