-- =============================================================================
-- 02_RAW_TABLES.SQL
-- Healthcare Claims Analytics — Raw Data Table Definitions
-- After running this, upload CSVs via: Snowflake UI → Data → Load data
--   Settings: Header lines to skip = 1 | On error = Continue
-- =============================================================================

USE DATABASE HEALTHCARE_CLAIMS;
USE SCHEMA RAW_DATA;

-- -----------------------------------------------------------------------------
-- BENEFICIARY SUMMARY
-- Patient demographics + annual cost totals.
-- Upload all 3 yearly CSVs (2008, 2009, 2010) into this one table.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE BENEFICIARY_SUMMARY (
    DESYNPUF_ID                         VARCHAR(16),
    BENE_BIRTH_DT                       INTEGER,        -- YYYYMMDD
    BENE_DEATH_DT                       INTEGER,
    BENE_SEX_IDENT_CD                   INTEGER,        -- 1=Male, 2=Female
    BENE_RACE_CD                        INTEGER,        -- 1=White 2=Black 3=Other 5=Hispanic
    BENE_ESRD_IND                       VARCHAR(1),
    SP_STATE_CODE                       VARCHAR(2),
    BENE_COUNTY_CD                      VARCHAR(3),
    BENE_HI_CVRAGE_TOT_MONS             INTEGER,
    BENE_SMI_CVRAGE_TOT_MONS            INTEGER,
    BENE_HMO_CVRAGE_TOT_MONS            INTEGER,
    SP_ALZHDMTA                         INTEGER,        -- 1=Yes, 2=No
    SP_CHF                              INTEGER,
    SP_CHRNKIDN                         INTEGER,
    SP_CNCR                             INTEGER,
    SP_COPD                             INTEGER,
    SP_DEPRESSN                         INTEGER,
    SP_DIABETES                         INTEGER,
    SP_ISCHMCHT                         INTEGER,
    SP_OSTEOPRS                         INTEGER,
    SP_RA_OA                            INTEGER,
    SP_STRKETIA                         INTEGER,
    MEDREIMB_IP                         DECIMAL(10,2),
    BENRES_IP                           DECIMAL(10,2),
    PPPYMT_IP                           DECIMAL(10,2),
    MEDREIMB_OP                         DECIMAL(10,2),
    BENRES_OP                           DECIMAL(10,2),
    PPPYMT_OP                           DECIMAL(10,2),
    MEDREIMB_CAR                        DECIMAL(10,2),
    BENRES_CAR                          DECIMAL(10,2),
    PPPYMT_CAR                          DECIMAL(10,2)
);

-- -----------------------------------------------------------------------------
-- INPATIENT CLAIMS
-- Hospital admission claims. One row per claim-segment combination.
-- Segments are billing line items — we aggregate these later in the fact table.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE INPATIENT_CLAIMS (
    DESYNPUF_ID                         VARCHAR(16),
    CLM_ID                              BIGINT,
    SEGMENT                             INTEGER,
    CLM_FROM_DT                         INTEGER,
    CLM_THRU_DT                         INTEGER,
    CLM_ADMSN_DT                        INTEGER,
    NCH_BENE_DSCHRG_DT                  INTEGER,
    CLM_PMT_AMT                         DECIMAL(10,2),
    NCH_PRMRY_PYR_CLM_PD_AMT            DECIMAL(10,2),
    NCH_BENE_IP_DDCTBL_AMT              DECIMAL(10,2),
    NCH_BENE_PTA_COINSRNC_LBLTY_AM      DECIMAL(10,2),
    CLM_UTLZTN_DAY_CNT                  INTEGER,        -- Length of stay
    PRVDR_NUM                           VARCHAR(6),
    CLM_DRG_CD                          VARCHAR(3),     -- DRG code
    AT_PHYSN_NPI                        VARCHAR(10),
    OP_PHYSN_NPI                        VARCHAR(10),
    OT_PHYSN_NPI                        VARCHAR(10),
    ICD9_DGNS_CD_1                      VARCHAR(5),     -- Up to 10 diagnosis codes
    ICD9_DGNS_CD_2                      VARCHAR(5),
    ICD9_DGNS_CD_3                      VARCHAR(5),
    ICD9_DGNS_CD_4                      VARCHAR(5),
    ICD9_DGNS_CD_5                      VARCHAR(5),
    ICD9_DGNS_CD_6                      VARCHAR(5),
    ICD9_DGNS_CD_7                      VARCHAR(5),
    ICD9_DGNS_CD_8                      VARCHAR(5),
    ICD9_DGNS_CD_9                      VARCHAR(5),
    ICD9_DGNS_CD_10                     VARCHAR(5),
    ICD9_PRCDR_CD_1                     VARCHAR(5),     -- Up to 6 procedure codes
    ICD9_PRCDR_CD_2                     VARCHAR(5),
    ICD9_PRCDR_CD_3                     VARCHAR(5),
    ICD9_PRCDR_CD_4                     VARCHAR(5),
    ICD9_PRCDR_CD_5                     VARCHAR(5),
    ICD9_PRCDR_CD_6                     VARCHAR(5)
);

-- -----------------------------------------------------------------------------
-- OUTPATIENT CLAIMS
-- Outpatient visits (ER, clinics, same-day procedures)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE OUTPATIENT_CLAIMS (
    DESYNPUF_ID                         VARCHAR(16),
    CLM_ID                              BIGINT,
    SEGMENT                             INTEGER,
    CLM_FROM_DT                         INTEGER,
    CLM_THRU_DT                         INTEGER,
    CLM_PMT_AMT                         DECIMAL(10,2),
    NCH_PRMRY_PYR_CLM_PD_AMT            DECIMAL(10,2),
    NCH_BENE_PTB_DDCTBL_AMT             DECIMAL(10,2),
    NCH_BENE_PTB_COINSRNC_AMT           DECIMAL(10,2),
    PRVDR_NUM                           VARCHAR(6),
    AT_PHYSN_NPI                        VARCHAR(10),
    OP_PHYSN_NPI                        VARCHAR(10),
    OT_PHYSN_NPI                        VARCHAR(10),
    ICD9_DGNS_CD_1                      VARCHAR(5),
    ICD9_DGNS_CD_2                      VARCHAR(5),
    ICD9_DGNS_CD_3                      VARCHAR(5),
    ICD9_DGNS_CD_4                      VARCHAR(5),
    ICD9_DGNS_CD_5                      VARCHAR(5),
    ICD9_DGNS_CD_6                      VARCHAR(5),
    ICD9_DGNS_CD_7                      VARCHAR(5),
    ICD9_DGNS_CD_8                      VARCHAR(5),
    ICD9_DGNS_CD_9                      VARCHAR(5),
    ICD9_DGNS_CD_10                     VARCHAR(5),
    ICD9_PRCDR_CD_1                     VARCHAR(5),
    ICD9_PRCDR_CD_2                     VARCHAR(5),
    ICD9_PRCDR_CD_3                     VARCHAR(5),
    ICD9_PRCDR_CD_4                     VARCHAR(5),
    ICD9_PRCDR_CD_5                     VARCHAR(5),
    ICD9_PRCDR_CD_6                     VARCHAR(5)
);

-- -----------------------------------------------------------------------------
-- Verify row counts after upload
-- -----------------------------------------------------------------------------
SELECT 'BENEFICIARY_SUMMARY' as table_name, COUNT(*) as row_count FROM BENEFICIARY_SUMMARY
UNION ALL
SELECT 'INPATIENT_CLAIMS',  COUNT(*) FROM INPATIENT_CLAIMS
UNION ALL
SELECT 'OUTPATIENT_CLAIMS', COUNT(*) FROM OUTPATIENT_CLAIMS;
