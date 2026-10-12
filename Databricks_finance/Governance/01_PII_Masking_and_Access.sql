-- Databricks notebook source
-- =====================================================
-- PII MASKING + BRANCH-LEVEL ACCESS (Unity Catalog)
-- Read-only on top of Silver: does NOT change the pipeline.
-- =====================================================

-- COMMAND ----------

-- DBTITLE 1,1. Secure schema
CREATE SCHEMA IF NOT EXISTS banking.secure;

-- COMMAND ----------

-- DBTITLE 1,2. Masking functions
-- PAN: show only the last 4 characters
CREATE OR REPLACE FUNCTION banking.secure.mask_pan(pan STRING)
RETURNS STRING
RETURN CASE WHEN pan IS NULL THEN NULL ELSE concat('XXXXXX', right(pan, 4)) END;

-- Email: keep first letter and domain  (vikram.gupta73@gmail.com -> v*****@gmail.com)
CREATE OR REPLACE FUNCTION banking.secure.mask_email(email STRING)
RETURNS STRING
RETURN CASE
         WHEN email IS NULL THEN NULL
         WHEN instr(email, '@') = 0 THEN '*****'
         ELSE concat(left(email, 1), '*****', substr(email, instr(email, '@')))
       END;

-- Phone: show only the last 4 digits
CREATE OR REPLACE FUNCTION banking.secure.mask_phone(phone STRING)
RETURNS STRING
RETURN CASE WHEN phone IS NULL THEN NULL ELSE concat('******', right(phone, 4)) END;

-- COMMAND ----------

-- DBTITLE 1,3. Access control table (who can see which branch, and raw PII or not)
-- branch_code = 'ALL' gives access to every branch.
CREATE TABLE IF NOT EXISTS banking.secure.user_access (
    user_email    STRING,
    branch_code   STRING,
    can_view_pii  BOOLEAN
);

-- Start yourself as a "Mumbai branch analyst": one branch, masked PII
DELETE FROM banking.secure.user_access WHERE user_email = current_user();
INSERT INTO banking.secure.user_access VALUES (current_user(), 'BR001', false);

-- COMMAND ----------

-- DBTITLE 1,4. Secure view: row-level filter + column masking
CREATE OR REPLACE VIEW banking.secure.customers_v AS
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    CASE WHEN a.can_view_pii THEN CAST(c.date_of_birth AS STRING)
         ELSE CAST(year(c.date_of_birth) AS STRING) END             AS date_of_birth,
    CASE WHEN a.can_view_pii THEN c.pan_number
         ELSE banking.secure.mask_pan(c.pan_number) END               AS pan_number,
    CASE WHEN a.can_view_pii THEN c.email
         ELSE banking.secure.mask_email(c.email) END                  AS email,
    CASE WHEN a.can_view_pii THEN c.phone_number
         ELSE banking.secure.mask_phone(c.phone_number) END           AS phone_number,
    c.kyc_status,
    c.branch_code
FROM banking.silver.customers c
JOIN banking.secure.user_access a
  ON  a.user_email = current_user()
  AND (a.branch_code = 'ALL' OR a.branch_code = c.branch_code);

-- COMMAND ----------

-- DBTITLE 1,5. DEMO part 1: branch analyst -> only BR001, PII masked
SELECT branch_code, count(*) AS customers FROM banking.secure.customers_v GROUP BY branch_code;

-- COMMAND ----------

SELECT * FROM banking.secure.customers_v LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,6. DEMO part 2: promote yourself to compliance officer -> all branches, raw PII
UPDATE banking.secure.user_access
SET branch_code = 'ALL', can_view_pii = true
WHERE user_email = current_user();

SELECT * FROM banking.secure.customers_v LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,7. How this is used in a real team (run only if you have other users/groups)
-- Analysts get the VIEW, never the raw Silver table:
-- GRANT USE SCHEMA ON SCHEMA banking.secure TO `analysts`;
-- GRANT SELECT ON VIEW banking.secure.customers_v TO `analysts`;
-- REVOKE SELECT ON TABLE banking.silver.customers FROM `analysts`;