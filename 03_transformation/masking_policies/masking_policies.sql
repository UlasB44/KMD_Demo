/*
=============================================================================
KMD Denmark - Dynamic Data Masking Policies
Protecting PII data (CPR numbers, personal information)
=============================================================================
BEST PRACTICES:
1. Apply masking at the lowest level (column) for granular control
2. Use role-based access to determine masking behavior
3. Test masking policies thoroughly before production
4. Document which roles can see unmasked data
5. Consider partial masking for usability while protecting PII
=============================================================================
*/

USE ROLE SYSADMIN;
USE DATABASE KMD_SCHOOLS;
USE WAREHOUSE KMD_WH;

-- ============================================================================
-- SECTION 1: Create Access Roles for Masking
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Role that can see full PII data (e.g., school administrators)
CREATE ROLE IF NOT EXISTS KMD_PII_ADMIN
    COMMENT = 'Role with access to full PII data (unmasked)';

-- Role for analysts who should see masked data
CREATE ROLE IF NOT EXISTS KMD_ANALYST
    COMMENT = 'Analyst role - sees masked PII data';

-- Role for developers/testers
CREATE ROLE IF NOT EXISTS KMD_DEVELOPER
    COMMENT = 'Developer role - sees masked PII data';

-- Grant roles to SYSADMIN for management
GRANT ROLE KMD_PII_ADMIN TO ROLE SYSADMIN;
GRANT ROLE KMD_ANALYST TO ROLE SYSADMIN;
GRANT ROLE KMD_DEVELOPER TO ROLE SYSADMIN;

-- Grant database access to roles
GRANT USAGE ON DATABASE KMD_SCHOOLS TO ROLE KMD_PII_ADMIN;
GRANT USAGE ON DATABASE KMD_SCHOOLS TO ROLE KMD_ANALYST;
GRANT USAGE ON DATABASE KMD_SCHOOLS TO ROLE KMD_DEVELOPER;

GRANT USAGE ON DATABASE KMD_STAGING TO ROLE KMD_PII_ADMIN;
GRANT USAGE ON DATABASE KMD_STAGING TO ROLE KMD_ANALYST;
GRANT USAGE ON DATABASE KMD_STAGING TO ROLE KMD_DEVELOPER;

GRANT USAGE ON DATABASE KMD_ANALYTICS TO ROLE KMD_PII_ADMIN;
GRANT USAGE ON DATABASE KMD_ANALYTICS TO ROLE KMD_ANALYST;
GRANT USAGE ON DATABASE KMD_ANALYTICS TO ROLE KMD_DEVELOPER;

-- Grant warehouse access
GRANT USAGE ON WAREHOUSE KMD_WH TO ROLE KMD_PII_ADMIN;
GRANT USAGE ON WAREHOUSE KMD_WH TO ROLE KMD_ANALYST;
GRANT USAGE ON WAREHOUSE KMD_WH TO ROLE KMD_DEVELOPER;

-- ============================================================================
-- SECTION 2: Create Masking Policies
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE KMD_SCHOOLS;
USE SCHEMA SHARED;

-- CPR Number Masking Policy (Danish Personal ID)
-- Format: DDMMYY-XXXX -> Shows birth date, masks last 4 digits
CREATE OR REPLACE MASKING POLICY CPR_MASK AS (val STRING) 
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'KMD_PII_ADMIN') 
            THEN val
        WHEN val IS NULL 
            THEN NULL
        ELSE 
            SUBSTRING(val, 1, 7) || 'XXXX'  -- Show DDMMYY-, mask last 4 digits
    END
COMMENT = 'Masks CPR number - shows birth date portion only for non-privileged roles';

-- Full CPR Masking (complete redaction)
CREATE OR REPLACE MASKING POLICY CPR_FULL_MASK AS (val STRING) 
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'KMD_PII_ADMIN') 
            THEN val
        WHEN val IS NULL 
            THEN NULL
        ELSE 
            '******-****'  -- Complete redaction
    END
COMMENT = 'Fully masks CPR number for non-privileged roles';

-- Email Masking Policy
-- Shows domain but masks username: john.smith@school.dk -> j***@school.dk
CREATE OR REPLACE MASKING POLICY EMAIL_MASK AS (val STRING) 
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'KMD_PII_ADMIN') 
            THEN val
        WHEN val IS NULL 
            THEN NULL
        WHEN POSITION('@' IN val) > 0 
            THEN SUBSTRING(val, 1, 1) || '***@' || SPLIT_PART(val, '@', 2)
        ELSE 
            '***'
    END
COMMENT = 'Masks email - shows first character and domain only';

-- Phone Number Masking Policy
-- Shows last 4 digits: +45 12 34 56 78 -> +45 XX XX XX 78
CREATE OR REPLACE MASKING POLICY PHONE_MASK AS (val STRING) 
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'KMD_PII_ADMIN') 
            THEN val
        WHEN val IS NULL 
            THEN NULL
        WHEN LENGTH(val) >= 4 
            THEN REPEAT('*', LENGTH(val) - 4) || RIGHT(val, 4)
        ELSE 
            '****'
    END
COMMENT = 'Masks phone number - shows last 4 digits only';

-- Name Masking Policy (partial - shows first letter)
CREATE OR REPLACE MASKING POLICY NAME_PARTIAL_MASK AS (val STRING) 
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'KMD_PII_ADMIN') 
            THEN val
        WHEN val IS NULL 
            THEN NULL
        WHEN LENGTH(val) > 1 
            THEN SUBSTRING(val, 1, 1) || REPEAT('*', LENGTH(val) - 1)
        ELSE 
            '*'
    END
COMMENT = 'Partially masks names - shows first letter only';

-- Address Masking Policy
CREATE OR REPLACE MASKING POLICY ADDRESS_MASK AS (val STRING) 
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'KMD_PII_ADMIN') 
            THEN val
        WHEN val IS NULL 
            THEN NULL
        ELSE 
            '[REDACTED ADDRESS]'
    END
COMMENT = 'Fully masks address for non-privileged roles';

-- Birth Date Masking (shows year only for age calculation)
CREATE OR REPLACE MASKING POLICY BIRTHDATE_MASK AS (val DATE) 
RETURNS DATE ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'KMD_PII_ADMIN') 
            THEN val
        WHEN val IS NULL 
            THEN NULL
        ELSE 
            DATE_TRUNC('YEAR', val)  -- Returns January 1st of birth year
    END
COMMENT = 'Masks birth date - shows year only (for age calculation)';

-- ============================================================================
-- SECTION 3: Apply Masking Policies to Tables
-- ============================================================================

-- Apply to staging tables
USE DATABASE KMD_STAGING;
USE SCHEMA STAGING;

-- Apply CPR masking to students
ALTER TABLE IF EXISTS DIM_STUDENTS_STAGING 
    MODIFY COLUMN cpr_number SET MASKING POLICY KMD_SCHOOLS.SHARED.CPR_MASK;

-- Apply email masking
ALTER TABLE IF EXISTS DIM_STUDENTS_STAGING 
    MODIFY COLUMN guardian_email SET MASKING POLICY KMD_SCHOOLS.SHARED.EMAIL_MASK;

-- Apply phone masking
ALTER TABLE IF EXISTS DIM_STUDENTS_STAGING 
    MODIFY COLUMN guardian_phone SET MASKING POLICY KMD_SCHOOLS.SHARED.PHONE_MASK;

-- Apply address masking
ALTER TABLE IF EXISTS DIM_STUDENTS_STAGING 
    MODIFY COLUMN address SET MASKING POLICY KMD_SCHOOLS.SHARED.ADDRESS_MASK;

-- Apply birth date masking
ALTER TABLE IF EXISTS DIM_STUDENTS_STAGING 
    MODIFY COLUMN birth_date SET MASKING POLICY KMD_SCHOOLS.SHARED.BIRTHDATE_MASK;

-- Apply to teachers table
ALTER TABLE IF EXISTS DIM_TEACHERS_STAGING 
    MODIFY COLUMN cpr_number SET MASKING POLICY KMD_SCHOOLS.SHARED.CPR_MASK;

ALTER TABLE IF EXISTS DIM_TEACHERS_STAGING 
    MODIFY COLUMN email SET MASKING POLICY KMD_SCHOOLS.SHARED.EMAIL_MASK;

ALTER TABLE IF EXISTS DIM_TEACHERS_STAGING 
    MODIFY COLUMN phone SET MASKING POLICY KMD_SCHOOLS.SHARED.PHONE_MASK;

ALTER TABLE IF EXISTS DIM_TEACHERS_STAGING 
    MODIFY COLUMN birth_date SET MASKING POLICY KMD_SCHOOLS.SHARED.BIRTHDATE_MASK;

-- ============================================================================
-- SECTION 4: Test Masking Policies
-- ============================================================================

-- Create a test view to demonstrate masking
USE DATABASE KMD_SCHOOLS;
USE SCHEMA SHARED;

CREATE OR REPLACE VIEW MASKING_DEMO AS
SELECT 
    '010190-1234' AS sample_cpr,
    'magnus.jensen@skole.dk' AS sample_email,
    '+45 12 34 56 78' AS sample_phone,
    'Nordrevej 123' AS sample_address,
    '1990-01-15'::DATE AS sample_birthdate;

-- Apply policies to demo view
CREATE OR REPLACE SECURE VIEW MASKING_DEMO_PROTECTED AS
SELECT 
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'KMD_PII_ADMIN') 
            THEN sample_cpr
        ELSE SUBSTRING(sample_cpr, 1, 7) || 'XXXX'
    END AS cpr_number,
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'KMD_PII_ADMIN') 
            THEN sample_email
        ELSE SUBSTRING(sample_email, 1, 1) || '***@' || SPLIT_PART(sample_email, '@', 2)
    END AS email,
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'KMD_PII_ADMIN') 
            THEN sample_phone
        ELSE REPEAT('*', LENGTH(sample_phone) - 4) || RIGHT(sample_phone, 4)
    END AS phone,
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'KMD_PII_ADMIN') 
            THEN sample_address
        ELSE '[REDACTED ADDRESS]'
    END AS address,
    sample_birthdate,
    CURRENT_ROLE() AS viewed_by_role
FROM MASKING_DEMO;

-- ============================================================================
-- SECTION 5: Verify Masking Policies
-- ============================================================================

-- Show all masking policies
SHOW MASKING POLICIES IN DATABASE KMD_SCHOOLS;

-- Show policy references (where policies are applied)
SELECT *
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    POLICY_NAME => 'KMD_SCHOOLS.SHARED.CPR_MASK'
));

-- Test query (run as different roles to see different results)
-- As SYSADMIN: Will see full data
-- As KMD_ANALYST: Will see masked data

SELECT 'Masking policies created successfully!' AS status;

/*
-- TEST QUERIES (run with different roles):

USE ROLE SYSADMIN;
SELECT * FROM KMD_SCHOOLS.SHARED.MASKING_DEMO_PROTECTED;
-- Expected: Full data visible

USE ROLE KMD_ANALYST;
SELECT * FROM KMD_SCHOOLS.SHARED.MASKING_DEMO_PROTECTED;
-- Expected: Masked data (CPR: 010190-XXXX, email: m***@skole.dk, etc.)

*/
