-- =============================================================================
-- 01_SETUP.SQL
-- Healthcare Claims Analytics — Initial Snowflake Setup
-- =============================================================================

CREATE DATABASE IF NOT EXISTS HEALTHCARE_CLAIMS;
USE DATABASE HEALTHCARE_CLAIMS;

-- X-Small warehouse; auto-suspends after 60s of inactivity to save credits
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

USE WAREHOUSE COMPUTE_WH;

CREATE SCHEMA IF NOT EXISTS RAW_DATA;
USE SCHEMA RAW_DATA;

-- Verify
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE();
