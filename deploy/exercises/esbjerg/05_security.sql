-- ============================================================================
-- STUDENT EXERCISE - STEP 5: SECURITY (RLS + Dynamic Masking)
-- ============================================================================
-- Replace ESBJERG with your assigned municipality (uppercase)
-- Replace 561 with your municipality code
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE ESBJERG_DB;
USE WAREHOUSE KMD_WH;

-- ============================================================================
-- CREATE ANALYST ROLE FOR YOUR MUNICIPALITY
-- ============================================================================
CREATE ROLE IF NOT EXISTS ESBJERG_ANALYST;

-- Grant permissions
GRANT USAGE ON DATABASE ESBJERG_DB TO ROLE ESBJERG_ANALYST;
GRANT USAGE ON SCHEMA ESBJERG_DB.RAW TO ROLE ESBJERG_ANALYST;
GRANT USAGE ON SCHEMA ESBJERG_DB.CLEAN TO ROLE ESBJERG_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA ESBJERG_DB.RAW TO ROLE ESBJERG_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA ESBJERG_DB.CLEAN TO ROLE ESBJERG_ANALYST;
GRANT USAGE ON WAREHOUSE KMD_WH TO ROLE ESBJERG_ANALYST;

-- ============================================================================
-- ROW ACCESS POLICY (Optional - for multi-tenant within municipality)
-- ============================================================================
-- Since each student has their own database, RLS is optional
-- But here's how you would implement it:

/*
CREATE OR REPLACE ROW ACCESS POLICY ESBJERG_RLS AS (municipality_code NUMBER) 
RETURNS BOOLEAN ->
    CASE
        WHEN IS_ROLE_IN_SESSION('ACCOUNTADMIN') THEN TRUE
        WHEN IS_ROLE_IN_SESSION('ESBJERG_ANALYST') AND municipality_code = 561 THEN TRUE
        ELSE FALSE
    END;

-- Apply to tables
ALTER TABLE RAW.STUDENTS_RAW ADD ROW ACCESS POLICY ESBJERG_RLS ON (municipality_code);
ALTER TABLE RAW.TEACHERS_RAW ADD ROW ACCESS POLICY ESBJERG_RLS ON (municipality_code);
ALTER TABLE RAW.CLASSES_RAW ADD ROW ACCESS POLICY ESBJERG_RLS ON (municipality_code);
*/

-- ============================================================================
-- DYNAMIC DATA MASKING - CPR Numbers (Danish Personal ID)
-- ============================================================================
-- CPR format: DDMMYY-XXXX
-- Masked: Shows birth date portion, hides last 4 digits

CREATE OR REPLACE MASKING POLICY CPR_MASK AS (val VARCHAR) RETURNS VARCHAR ->
    CASE 
        WHEN IS_ROLE_IN_SESSION('ACCOUNTADMIN') THEN val
        ELSE CONCAT(LEFT(val, 6), '-XXXX')
    END;

-- Apply to RAW tables (CPR columns)
ALTER TABLE RAW.STUDENTS_RAW MODIFY COLUMN cpr_number SET MASKING POLICY CPR_MASK;
ALTER TABLE RAW.TEACHERS_RAW MODIFY COLUMN cpr_number SET MASKING POLICY CPR_MASK;

-- ============================================================================
-- DYNAMIC DATA MASKING - Email
-- ============================================================================
CREATE OR REPLACE MASKING POLICY EMAIL_MASK AS (val VARCHAR) RETURNS VARCHAR ->
    CASE 
        WHEN IS_ROLE_IN_SESSION('ACCOUNTADMIN') THEN val
        ELSE CONCAT(LEFT(val, 2), '***@***.dk')
    END;

-- Apply to tables
ALTER TABLE RAW.STUDENTS_RAW MODIFY COLUMN guardian_email SET MASKING POLICY EMAIL_MASK;
ALTER TABLE RAW.TEACHERS_RAW MODIFY COLUMN email SET MASKING POLICY EMAIL_MASK;

-- ============================================================================
-- DYNAMIC DATA MASKING - Phone
-- ============================================================================
CREATE OR REPLACE MASKING POLICY PHONE_MASK AS (val VARCHAR) RETURNS VARCHAR ->
    CASE 
        WHEN IS_ROLE_IN_SESSION('ACCOUNTADMIN') THEN val
        ELSE CONCAT('+45 ** ** ** ', RIGHT(val, 2))
    END;

-- Apply to tables
ALTER TABLE RAW.STUDENTS_RAW MODIFY COLUMN guardian_phone SET MASKING POLICY PHONE_MASK;
ALTER TABLE RAW.TEACHERS_RAW MODIFY COLUMN phone SET MASKING POLICY PHONE_MASK;

-- ============================================================================
-- TEST MASKING
-- ============================================================================
-- As ACCOUNTADMIN (should see full data):
SELECT student_id, first_name, cpr_number, guardian_email, guardian_phone 
FROM RAW.STUDENTS_RAW LIMIT 5;

-- As ANALYST (should see masked data):
/*
USE ROLE ESBJERG_ANALYST;
SELECT student_id, first_name, cpr_number, guardian_email, guardian_phone 
FROM RAW.STUDENTS_RAW LIMIT 5;
*/
