-- =============================================================================
-- 06_DATA_QUALITY.SQL
-- Healthcare Claims Analytics — Automated Data Quality Checks
--
-- DATA_QUALITY_LOG stores every check result so you can track trends.
-- sp_run_data_quality_checks() runs all checks in one call.
--
-- Checks implemented:
--   1. Negative claim amounts
--   2. Date integrity (from > thru)
--   3. Orphaned patient keys
--   4. Duplicate claim IDs
--   5. NULL primary diagnosis
--   6. Claims outside expected date range
--   7. Patients with impossible ages
-- =============================================================================

USE DATABASE HEALTHCARE_CLAIMS;
USE SCHEMA RAW_DATA;

-- =============================================================================
-- DATA_QUALITY_LOG
-- =============================================================================
CREATE TABLE IF NOT EXISTS DATA_QUALITY_LOG (
    check_id        INTEGER AUTOINCREMENT PRIMARY KEY,
    check_name      VARCHAR(100),
    check_time      TIMESTAMP,
    table_name      VARCHAR(100),
    status          VARCHAR(20),        -- PASS | FAIL
    records_checked INTEGER,
    records_failed  INTEGER,
    failure_rate    DECIMAL(5,2),       -- percentage
    details         VARCHAR(500)
);

-- =============================================================================
-- sp_run_data_quality_checks
-- =============================================================================
CREATE OR REPLACE PROCEDURE sp_run_data_quality_checks()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_total     INTEGER;
    v_passed    INTEGER;
BEGIN
    v_total   := 0;
    v_passed  := 0;

    -- ------------------------------------------------------------------
    -- CHECK 1: Negative claim amounts
    -- ------------------------------------------------------------------
    INSERT INTO RAW_DATA.DATA_QUALITY_LOG (
        check_name, check_time, table_name, status,
        records_checked, records_failed, failure_rate, details
    )
    SELECT
        'Negative Claim Amounts',
        CURRENT_TIMESTAMP(),
        'FACT_CLAIMS',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        (SELECT COUNT(*) FROM RAW_DATA.FACT_CLAIMS),
        COUNT(*),
        ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM RAW_DATA.FACT_CLAIMS), 0), 2),
        'Claims where payment < 0'
    FROM RAW_DATA.FACT_CLAIMS
    WHERE claim_payment_amount < 0;

    -- ------------------------------------------------------------------
    -- CHECK 2: Date integrity — from_date must be <= thru_date
    -- ------------------------------------------------------------------
    INSERT INTO RAW_DATA.DATA_QUALITY_LOG (
        check_name, check_time, table_name, status,
        records_checked, records_failed, failure_rate, details
    )
    SELECT
        'Date Integrity (from > thru)',
        CURRENT_TIMESTAMP(),
        'FACT_CLAIMS',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        (SELECT COUNT(*) FROM RAW_DATA.FACT_CLAIMS),
        COUNT(*),
        ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM RAW_DATA.FACT_CLAIMS), 0), 2),
        'Claims where from_date > thru_date'
    FROM RAW_DATA.FACT_CLAIMS
    WHERE claim_from_date_key > claim_thru_date_key;

    -- ------------------------------------------------------------------
    -- CHECK 3: Orphaned patient keys
    -- ------------------------------------------------------------------
    INSERT INTO RAW_DATA.DATA_QUALITY_LOG (
        check_name, check_time, table_name, status,
        records_checked, records_failed, failure_rate, details
    )
    SELECT
        'Orphaned Patient Keys',
        CURRENT_TIMESTAMP(),
        'FACT_CLAIMS',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        (SELECT COUNT(*) FROM RAW_DATA.FACT_CLAIMS),
        COUNT(*),
        ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM RAW_DATA.FACT_CLAIMS), 0), 2),
        'Claims with patient_key not in DIM_PATIENTS'
    FROM RAW_DATA.FACT_CLAIMS
    WHERE patient_key IS NULL
       OR patient_key NOT IN (SELECT patient_key FROM RAW_DATA.DIM_PATIENTS);

    -- ------------------------------------------------------------------
    -- CHECK 4: Duplicate claim IDs
    -- ------------------------------------------------------------------
    INSERT INTO RAW_DATA.DATA_QUALITY_LOG (
        check_name, check_time, table_name, status,
        records_checked, records_failed, failure_rate, details
    )
    SELECT
        'Duplicate Claim IDs',
        CURRENT_TIMESTAMP(),
        'FACT_CLAIMS',
        CASE WHEN COALESCE(SUM(dups), 0) = 0 THEN 'PASS' ELSE 'FAIL' END,
        (SELECT COUNT(*) FROM RAW_DATA.FACT_CLAIMS),
        COALESCE(SUM(dups), 0),
        ROUND(COALESCE(SUM(dups), 0) * 100.0 / NULLIF((SELECT COUNT(*) FROM RAW_DATA.FACT_CLAIMS), 0), 2),
        'Claims that share a claim_id with another row'
    FROM (
        SELECT claim_id, COUNT(*) - 1 AS dups
        FROM RAW_DATA.FACT_CLAIMS
        GROUP BY claim_id
        HAVING COUNT(*) > 1
    );

    -- ------------------------------------------------------------------
    -- CHECK 5: NULL primary diagnosis
    -- ------------------------------------------------------------------
    INSERT INTO RAW_DATA.DATA_QUALITY_LOG (
        check_name, check_time, table_name, status,
        records_checked, records_failed, failure_rate, details
    )
    SELECT
        'NULL Primary Diagnosis',
        CURRENT_TIMESTAMP(),
        'FACT_CLAIMS',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        (SELECT COUNT(*) FROM RAW_DATA.FACT_CLAIMS),
        COUNT(*),
        ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM RAW_DATA.FACT_CLAIMS), 0), 2),
        'Claims missing primary_diagnosis_key'
    FROM RAW_DATA.FACT_CLAIMS
    WHERE primary_diagnosis_key IS NULL;

    -- ------------------------------------------------------------------
    -- CHECK 6: Claims outside 2008-2010 date range
    -- ------------------------------------------------------------------
    INSERT INTO RAW_DATA.DATA_QUALITY_LOG (
        check_name, check_time, table_name, status,
        records_checked, records_failed, failure_rate, details
    )
    SELECT
        'Dates Outside 2008-2010',
        CURRENT_TIMESTAMP(),
        'FACT_CLAIMS',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        (SELECT COUNT(*) FROM RAW_DATA.FACT_CLAIMS),
        COUNT(*),
        ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM RAW_DATA.FACT_CLAIMS), 0), 2),
        'Claims with from_date outside 20080101-20101231'
    FROM RAW_DATA.FACT_CLAIMS
    WHERE claim_from_date_key < 20080101 OR claim_from_date_key > 20101231;

    -- ------------------------------------------------------------------
    -- CHECK 7: Patients with impossible ages
    -- ------------------------------------------------------------------
    INSERT INTO RAW_DATA.DATA_QUALITY_LOG (
        check_name, check_time, table_name, status,
        records_checked, records_failed, failure_rate, details
    )
    SELECT
        'Impossible Patient Ages',
        CURRENT_TIMESTAMP(),
        'DIM_PATIENTS',
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
        (SELECT COUNT(*) FROM RAW_DATA.DIM_PATIENTS),
        COUNT(*),
        ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM RAW_DATA.DIM_PATIENTS), 0), 2),
        'Patients with age < 0 or age > 120'
    FROM RAW_DATA.DIM_PATIENTS
    WHERE age_at_2010 < 0 OR age_at_2010 > 120;

    -- ------------------------------------------------------------------
    -- Summary return
    -- ------------------------------------------------------------------
    SELECT COUNT(*) INTO v_total
    FROM RAW_DATA.DATA_QUALITY_LOG
    WHERE check_time >= DATEADD(minute, -5, CURRENT_TIMESTAMP());

    SELECT COUNT(*) INTO v_passed
    FROM RAW_DATA.DATA_QUALITY_LOG
    WHERE check_time >= DATEADD(minute, -5, CURRENT_TIMESTAMP())
      AND status = 'PASS';

    RETURN v_passed || ' / ' || v_total || ' checks passed';

EXCEPTION WHEN OTHER THEN
    RETURN 'QUALITY CHECK FAILED: ' || SQLERRM;
END;
$$;

-- =============================================================================
-- Run checks and view results
-- =============================================================================
-- CALL sp_run_data_quality_checks();
--
-- -- Latest run summary
-- SELECT check_name, table_name, status, records_failed, failure_rate, details
-- FROM DATA_QUALITY_LOG
-- WHERE check_time >= DATEADD(minute, -5, CURRENT_TIMESTAMP())
-- ORDER BY check_id;
--
-- -- All-time summary by check type
-- SELECT check_name,
--        COUNT(*)                                                         AS total_runs,
--        SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END)                 AS times_passed,
--        SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END)                 AS times_failed,
--        MAX(records_failed)                                              AS max_failures_seen
-- FROM DATA_QUALITY_LOG
-- GROUP BY check_name;
