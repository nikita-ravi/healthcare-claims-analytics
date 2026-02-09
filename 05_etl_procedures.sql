-- =============================================================================
-- 05_ETL_PROCEDURES.SQL
-- Healthcare Claims Analytics — Reusable ETL Stored Procedures
--
-- Every procedure logs start/end time, row counts, and errors into
-- ETL_JOB_LOG so you can monitor pipeline health.
--
-- sp_load_dim_patients()           → refreshes DIM_PATIENTS
-- sp_load_fact_claims()            → refreshes FACT_CLAIMS (both types)
-- sp_refresh_marts()               → refreshes all three mart tables
-- sp_run_full_etl_pipeline()       → orchestrator: calls all three in order
-- =============================================================================

USE DATABASE HEALTHCARE_CLAIMS;
USE SCHEMA RAW_DATA;

-- =============================================================================
-- ETL_JOB_LOG  — tracks every procedure execution
-- =============================================================================
CREATE TABLE IF NOT EXISTS ETL_JOB_LOG (
    log_id          INTEGER AUTOINCREMENT PRIMARY KEY,
    job_name        VARCHAR(100),
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    status          VARCHAR(20),        -- RUNNING | SUCCESS | FAILED
    rows_processed  INTEGER,
    error_message   VARCHAR(1000)
);

-- =============================================================================
-- sp_load_dim_patients
-- =============================================================================
CREATE OR REPLACE PROCEDURE sp_load_dim_patients()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_start TIMESTAMP;
    v_rows  INTEGER;
BEGIN
    v_start := CURRENT_TIMESTAMP();
    INSERT INTO RAW_DATA.ETL_JOB_LOG (job_name, start_time, status)
    VALUES ('sp_load_dim_patients', v_start, 'RUNNING');

    TRUNCATE TABLE RAW_DATA.DIM_PATIENTS;

    INSERT INTO RAW_DATA.DIM_PATIENTS (
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
        SP_STATE_CODE, BENE_COUNTY_CD,
        BENE_DEATH_DT IS NOT NULL,
        SP_ALZHDMTA=1, SP_CHF=1, SP_CHRNKIDN=1, SP_CNCR=1, SP_COPD=1,
        SP_DEPRESSN=1, SP_DIABETES=1, SP_ISCHMCHT=1, SP_OSTEOPRS=1, SP_RA_OA=1, SP_STRKETIA=1,
        BENE_HI_CVRAGE_TOT_MONS, BENE_SMI_CVRAGE_TOT_MONS, BENE_HMO_CVRAGE_TOT_MONS,
        MEDREIMB_IP, MEDREIMB_OP, MEDREIMB_CAR,
        COALESCE(MEDREIMB_IP,0)+COALESCE(MEDREIMB_OP,0)+COALESCE(MEDREIMB_CAR,0),
        (CASE WHEN SP_ALZHDMTA=1 THEN 1 ELSE 0 END + CASE WHEN SP_CHF=1 THEN 1 ELSE 0 END +
         CASE WHEN SP_CHRNKIDN=1 THEN 1 ELSE 0 END + CASE WHEN SP_CNCR=1 THEN 1 ELSE 0 END +
         CASE WHEN SP_COPD=1 THEN 1 ELSE 0 END + CASE WHEN SP_DEPRESSN=1 THEN 1 ELSE 0 END +
         CASE WHEN SP_DIABETES=1 THEN 1 ELSE 0 END + CASE WHEN SP_ISCHMCHT=1 THEN 1 ELSE 0 END +
         CASE WHEN SP_OSTEOPRS=1 THEN 1 ELSE 0 END + CASE WHEN SP_RA_OA=1 THEN 1 ELSE 0 END +
         CASE WHEN SP_STRKETIA=1 THEN 1 ELSE 0 END)
    FROM RAW_DATA.BENEFICIARY_SUMMARY;

    v_rows := SQLROWCOUNT;
    UPDATE RAW_DATA.ETL_JOB_LOG
    SET end_time = CURRENT_TIMESTAMP(), status = 'SUCCESS', rows_processed = v_rows
    WHERE job_name = 'sp_load_dim_patients' AND start_time = v_start;

    RETURN 'SUCCESS: ' || v_rows || ' patients loaded';
EXCEPTION WHEN OTHER THEN
    UPDATE RAW_DATA.ETL_JOB_LOG
    SET end_time = CURRENT_TIMESTAMP(), status = 'FAILED', error_message = SQLERRM
    WHERE job_name = 'sp_load_dim_patients' AND start_time = v_start;
    RETURN 'FAILED: ' || SQLERRM;
END;
$$;

-- =============================================================================
-- sp_load_fact_claims
-- =============================================================================
CREATE OR REPLACE PROCEDURE sp_load_fact_claims()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_start TIMESTAMP;
    v_rows  INTEGER;
BEGIN
    v_start := CURRENT_TIMESTAMP();
    INSERT INTO RAW_DATA.ETL_JOB_LOG (job_name, start_time, status)
    VALUES ('sp_load_fact_claims', v_start, 'RUNNING');

    TRUNCATE TABLE RAW_DATA.FACT_CLAIMS;

    -- Inpatient
    INSERT INTO RAW_DATA.FACT_CLAIMS (
        claim_id, claim_type, patient_key, provider_key, primary_diagnosis_key,
        claim_from_date_key, claim_thru_date_key, admission_date_key,
        claim_payment_amount, primary_payer_amount, deductible_amount, coinsurance_amount,
        utilization_days, drg_code, segment_count,
        attending_physician_npi, operating_physician_npi, other_physician_npi
    )
    SELECT i.CLM_ID, 'Inpatient',
        p.patient_key, prov.provider_key, d.diagnosis_key,
        MAX(i.CLM_FROM_DT), MAX(i.CLM_THRU_DT), MAX(i.CLM_ADMSN_DT),
        MAX(i.CLM_PMT_AMT), MAX(i.NCH_PRMRY_PYR_CLM_PD_AMT),
        MAX(i.NCH_BENE_IP_DDCTBL_AMT), MAX(i.NCH_BENE_PTA_COINSRNC_LBLTY_AM),
        MAX(i.CLM_UTLZTN_DAY_CNT), MAX(i.CLM_DRG_CD), COUNT(DISTINCT i.SEGMENT),
        MAX(i.AT_PHYSN_NPI), MAX(i.OP_PHYSN_NPI), MAX(i.OT_PHYSN_NPI)
    FROM RAW_DATA.INPATIENT_CLAIMS i
    LEFT JOIN RAW_DATA.DIM_PATIENTS  p    ON i.DESYNPUF_ID    = p.patient_id
    LEFT JOIN RAW_DATA.DIM_PROVIDERS prov ON i.PRVDR_NUM       = prov.provider_id
    LEFT JOIN RAW_DATA.DIM_DIAGNOSES d    ON i.ICD9_DGNS_CD_1  = d.icd9_code
    GROUP BY i.CLM_ID, p.patient_key, prov.provider_key, d.diagnosis_key;

    -- Outpatient
    INSERT INTO RAW_DATA.FACT_CLAIMS (
        claim_id, claim_type, patient_key, provider_key, primary_diagnosis_key,
        claim_from_date_key, claim_thru_date_key,
        claim_payment_amount, primary_payer_amount, deductible_amount, segment_count
    )
    SELECT o.CLM_ID, 'Outpatient',
        p.patient_key, prov.provider_key, d.diagnosis_key,
        MAX(o.CLM_FROM_DT), MAX(o.CLM_THRU_DT),
        MAX(o.CLM_PMT_AMT), MAX(o.NCH_PRMRY_PYR_CLM_PD_AMT),
        MAX(o.NCH_BENE_PTB_DDCTBL_AMT), COUNT(DISTINCT o.SEGMENT)
    FROM RAW_DATA.OUTPATIENT_CLAIMS o
    LEFT JOIN RAW_DATA.DIM_PATIENTS  p    ON o.DESYNPUF_ID    = p.patient_id
    LEFT JOIN RAW_DATA.DIM_PROVIDERS prov ON o.PRVDR_NUM       = prov.provider_id
    LEFT JOIN RAW_DATA.DIM_DIAGNOSES d    ON o.ICD9_DGNS_CD_1  = d.icd9_code
    GROUP BY o.CLM_ID, p.patient_key, prov.provider_key, d.diagnosis_key;

    v_rows := SQLROWCOUNT;
    UPDATE RAW_DATA.ETL_JOB_LOG
    SET end_time = CURRENT_TIMESTAMP(), status = 'SUCCESS', rows_processed = v_rows
    WHERE job_name = 'sp_load_fact_claims' AND start_time = v_start;

    RETURN 'SUCCESS: fact table reloaded';
EXCEPTION WHEN OTHER THEN
    UPDATE RAW_DATA.ETL_JOB_LOG
    SET end_time = CURRENT_TIMESTAMP(), status = 'FAILED', error_message = SQLERRM
    WHERE job_name = 'sp_load_fact_claims' AND start_time = v_start;
    RETURN 'FAILED: ' || SQLERRM;
END;
$$;

-- =============================================================================
-- sp_refresh_marts
-- =============================================================================
CREATE OR REPLACE PROCEDURE sp_refresh_marts()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_start TIMESTAMP;
BEGIN
    v_start := CURRENT_TIMESTAMP();
    INSERT INTO RAW_DATA.ETL_JOB_LOG (job_name, start_time, status)
    VALUES ('sp_refresh_marts', v_start, 'RUNNING');

    -- Monthly summary
    CREATE OR REPLACE TABLE RAW_DATA.MART_MONTHLY_CLAIMS_SUMMARY AS
    SELECT d.year, d.month, d.month_name,
        DATE_TRUNC('month', d.full_date) AS month_date,
        f.claim_type, diag.diagnosis_category,
        COUNT(DISTINCT f.claim_key) AS claim_count,
        COUNT(DISTINCT f.patient_key) AS unique_patients,
        SUM(f.claim_payment_amount) AS total_paid,
        AVG(f.claim_payment_amount) AS avg_paid_per_claim,
        AVG(f.utilization_days) AS avg_length_of_stay
    FROM RAW_DATA.FACT_CLAIMS f
    JOIN RAW_DATA.DIM_DATE d ON f.claim_from_date_key = d.date_key
    LEFT JOIN RAW_DATA.DIM_DIAGNOSES diag ON f.primary_diagnosis_key = diag.diagnosis_key
    GROUP BY d.year, d.month, d.month_name, DATE_TRUNC('month', d.full_date), f.claim_type, diag.diagnosis_category;

    -- Diagnosis summary
    CREATE OR REPLACE TABLE RAW_DATA.MART_DIAGNOSIS_SUMMARY AS
    SELECT diag.icd9_code, diag.diagnosis_description, diag.diagnosis_category, f.claim_type,
        COUNT(DISTINCT f.claim_key) AS claim_count,
        COUNT(DISTINCT f.patient_key) AS patient_count,
        SUM(f.claim_payment_amount) AS total_paid,
        AVG(f.claim_payment_amount) AS avg_paid_per_claim,
        AVG(f.utilization_days) AS avg_length_of_stay
    FROM RAW_DATA.FACT_CLAIMS f
    JOIN RAW_DATA.DIM_DIAGNOSES diag ON f.primary_diagnosis_key = diag.diagnosis_key
    GROUP BY diag.icd9_code, diag.diagnosis_description, diag.diagnosis_category, f.claim_type;

    -- Provider performance
    CREATE OR REPLACE TABLE RAW_DATA.MART_PROVIDER_PERFORMANCE AS
    SELECT prov.provider_id, prov.provider_type,
        COUNT(DISTINCT f.claim_key) AS total_claims,
        COUNT(DISTINCT f.patient_key) AS unique_patients,
        SUM(f.claim_payment_amount) AS total_revenue,
        AVG(f.claim_payment_amount) AS avg_claim_amount,
        AVG(f.utilization_days) AS avg_length_of_stay,
        ROUND(SUM(CASE WHEN f.utilization_days > 7 THEN 1 ELSE 0 END)*100.0 / NULLIF(COUNT(*),0), 2) AS pct_long_stays
    FROM RAW_DATA.FACT_CLAIMS f
    JOIN RAW_DATA.DIM_PROVIDERS prov ON f.provider_key = prov.provider_key
    WHERE f.claim_type = 'Inpatient'
    GROUP BY prov.provider_id, prov.provider_type;

    UPDATE RAW_DATA.ETL_JOB_LOG
    SET end_time = CURRENT_TIMESTAMP(), status = 'SUCCESS', rows_processed = 3
    WHERE job_name = 'sp_refresh_marts' AND start_time = v_start;

    RETURN 'SUCCESS: all marts refreshed';
EXCEPTION WHEN OTHER THEN
    UPDATE RAW_DATA.ETL_JOB_LOG
    SET end_time = CURRENT_TIMESTAMP(), status = 'FAILED', error_message = SQLERRM
    WHERE job_name = 'sp_refresh_marts' AND start_time = v_start;
    RETURN 'FAILED: ' || SQLERRM;
END;
$$;

-- =============================================================================
-- sp_run_full_etl_pipeline  — master orchestrator
-- =============================================================================
CREATE OR REPLACE PROCEDURE sp_run_full_etl_pipeline()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_result VARCHAR;
BEGIN
    v_result := '';

    CALL RAW_DATA.sp_load_dim_patients();
    CALL RAW_DATA.sp_load_fact_claims();
    CALL RAW_DATA.sp_refresh_marts();

    v_result := 'Full ETL pipeline completed successfully';
    RETURN v_result;
EXCEPTION WHEN OTHER THEN
    RETURN 'Pipeline FAILED: ' || SQLERRM;
END;
$$;

-- =============================================================================
-- Test: run the full pipeline and check logs
-- =============================================================================
-- CALL sp_run_full_etl_pipeline();
--
-- SELECT job_name, status, rows_processed,
--        DATEDIFF(second, start_time, end_time) as duration_sec,
--        error_message
-- FROM ETL_JOB_LOG
-- ORDER BY start_time DESC;
