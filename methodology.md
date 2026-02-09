# Methodology & Design Decisions

---

## Why Star Schema?

A star schema was chosen over a normalized (snowflake) schema because the primary consumer is a BI dashboard. Star schemas let Tableau join facts to dimensions in a single hop, keeping queries simple and fast. The trade-off is some data duplication in the dimension tables, which is acceptable at this data volume.

---

## Why Aggregate Segments in FACT_CLAIMS?

Each raw claim in the CMS data has multiple SEGMENT rows — one per billing line item. A naive INSERT would create 9× more rows than actual claims (e.g. 66K claims → 596K rows). The fact table uses GROUP BY on claim_id with MAX() for single-value fields and COUNT(DISTINCT SEGMENT) to produce exactly one row per claim. This makes claim-level analysis correct and intuitive.

---

## Why YYYYMMDD Integer Date Keys?

The CMS source data stores dates as YYYYMMDD integers (e.g. 20081015 for Oct 15, 2008). Rather than converting every date to a DATE type and then back, the dimension table's date_key is also an integer in the same format. This lets FACT_CLAIMS join to DIM_DATE with zero casting, which is simpler and faster.

---

## Why Stored Procedures Instead of dbt?

dbt is a great tool for production environments, but for a portfolio project the goal is to demonstrate understanding of the underlying ETL mechanics. Snowflake stored procedures show explicit control over truncate/reload, error handling (TRY/CATCH), and logging — all skills interviewers care about. dbt abstracts most of this away.

---

## Why Pre-Aggregated Mart Tables?

The star schema supports any ad-hoc query, but the Tableau dashboard always needs the same aggregations (monthly totals, top diagnoses, provider stats). Mart tables pre-compute these so the dashboard loads in milliseconds instead of seconds. They are rebuilt by sp_refresh_marts() whenever the underlying data changes.

---

## Why Data Quality Checks?

Raw CMS data is synthetic but still has edge cases: missing codes, unusual amounts, dates outside the expected range. The quality framework catches these before they reach the dashboard. Each check logs its result so you can see whether data quality is improving or degrading over time — a pattern that matters in production.

---

## Data Source: CMS DE-SynPUF

The Centers for Medicare & Medicaid Services (CMS) publishes the DE-SynPUF dataset specifically for development and testing. It contains realistic claim structures, ICD-9 codes, and spending patterns but is not real patient data. This makes it legally safe to publish and share while still being representative enough to demonstrate real analytics skills.

---

## Limitations

- ICD-9 diagnosis descriptions are mapped only for the most common codes. Less common codes appear as "Code: XXX". A full lookup table (e.g. from CMS) could be added.
- The date dimension covers only 2008–2010. Extend the GENERATOR range if you add data from other years.
- Outpatient claims do not have utilization_days or DRG codes — those columns are NULL for outpatient rows in FACT_CLAIMS.
