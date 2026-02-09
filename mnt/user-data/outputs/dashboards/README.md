# Connecting Tableau to Snowflake

---

## Prerequisites

- Tableau Desktop or Tableau Public (free)
- Snowflake account with the HEALTHCARE_CLAIMS database set up
- Snowflake JDBC driver installed (Tableau bundles this automatically)

---

## Steps

1. Open Tableau Desktop / Public.
2. Click **Connect → To a Server → Snowflake**.
3. Enter your connection details:
   - **Server:** `your-account.snowflakecomputing.com`
   - **Username / Password:** your Snowflake credentials
4. Select **Database:** HEALTHCARE_CLAIMS → **Schema:** RAW_DATA.
5. Choose the tables you need. The recommended starting points are the mart tables:
   - `MART_MONTHLY_CLAIMS_SUMMARY` — for trend charts
   - `MART_DIAGNOSIS_SUMMARY` — for diagnosis bar charts
   - `MART_PROVIDER_PERFORMANCE` — for provider analysis

---

## Recommended Dashboard Layout

The dashboard in the screenshot is organized into three rows:

**Row 1 — KPI Cards:** Total Paid, Claim Count, Unique Patients, Avg Paid per Claim, Avg Length of Stay. These pull from MART_MONTHLY_CLAIMS_SUMMARY with no filters.

**Row 2 — Comparison Charts:** Top Diagnoses by Total Paid (horizontal bar), Diagnoses by Avg Length of Stay (horizontal bar), Cost vs Utilization scatter plot. Use MART_DIAGNOSIS_SUMMARY.

**Row 3 — Trend Lines:** Monthly Total Claims Cost, Monthly Claim Volume, Average Length of Stay Trend. All three use MART_MONTHLY_CLAIMS_SUMMARY filtered to Inpatient and grouped by month_date.

---

## Filters

Add a **Diagnosis Category** filter connected to all sheets for interactive drill-down by disease category (Circulatory System, Endocrine/Metabolic, Respiratory System, etc.).

---

## Publishing

Upload your finished workbook to **Tableau Public** for a free, shareable URL you can link from your portfolio and LinkedIn.
