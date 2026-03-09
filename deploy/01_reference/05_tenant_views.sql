-- ============================================================================
-- KMD WORKSHOP - STEP 5: MULTI-TENANT VIEWS
-- ============================================================================
-- Creates municipality-specific views in each tenant schema
-- Demonstrates schema-per-tenant isolation pattern
-- ============================================================================

USE ROLE SYSADMIN;
USE WAREHOUSE KMD_WH;

-- ============================================================================
-- COPENHAGEN VIEWS (Municipality 101)
-- ============================================================================
CREATE OR REPLACE VIEW KMD_SCHOOLS.COPENHAGEN.SCHOOLS AS 
SELECT * FROM KMD_STAGING.RAW.SCHOOLS_RAW WHERE municipality_code = 101;

CREATE OR REPLACE VIEW KMD_SCHOOLS.COPENHAGEN.TEACHERS AS 
SELECT * FROM KMD_STAGING.RAW.TEACHERS_RAW WHERE municipality_code = 101;

CREATE OR REPLACE VIEW KMD_SCHOOLS.COPENHAGEN.STUDENTS AS 
SELECT * FROM KMD_STAGING.RAW.STUDENTS_RAW WHERE municipality_code = 101;

CREATE OR REPLACE VIEW KMD_SCHOOLS.COPENHAGEN.CLASSES AS 
SELECT * FROM KMD_STAGING.RAW.CLASSES_RAW WHERE municipality_code = 101;

-- ============================================================================
-- AARHUS VIEWS (Municipality 751)
-- ============================================================================
CREATE OR REPLACE VIEW KMD_SCHOOLS.AARHUS.SCHOOLS AS 
SELECT * FROM KMD_STAGING.RAW.SCHOOLS_RAW WHERE municipality_code = 751;

CREATE OR REPLACE VIEW KMD_SCHOOLS.AARHUS.TEACHERS AS 
SELECT * FROM KMD_STAGING.RAW.TEACHERS_RAW WHERE municipality_code = 751;

CREATE OR REPLACE VIEW KMD_SCHOOLS.AARHUS.STUDENTS AS 
SELECT * FROM KMD_STAGING.RAW.STUDENTS_RAW WHERE municipality_code = 751;

CREATE OR REPLACE VIEW KMD_SCHOOLS.AARHUS.CLASSES AS 
SELECT * FROM KMD_STAGING.RAW.CLASSES_RAW WHERE municipality_code = 751;

-- ============================================================================
-- ODENSE VIEWS (Municipality 461)
-- ============================================================================
CREATE OR REPLACE VIEW KMD_SCHOOLS.ODENSE.SCHOOLS AS 
SELECT * FROM KMD_STAGING.RAW.SCHOOLS_RAW WHERE municipality_code = 461;

CREATE OR REPLACE VIEW KMD_SCHOOLS.ODENSE.TEACHERS AS 
SELECT * FROM KMD_STAGING.RAW.TEACHERS_RAW WHERE municipality_code = 461;

CREATE OR REPLACE VIEW KMD_SCHOOLS.ODENSE.STUDENTS AS 
SELECT * FROM KMD_STAGING.RAW.STUDENTS_RAW WHERE municipality_code = 461;

CREATE OR REPLACE VIEW KMD_SCHOOLS.ODENSE.CLASSES AS 
SELECT * FROM KMD_STAGING.RAW.CLASSES_RAW WHERE municipality_code = 461;

-- ============================================================================
-- AALBORG VIEWS (Municipality 851)
-- ============================================================================
CREATE OR REPLACE VIEW KMD_SCHOOLS.AALBORG.SCHOOLS AS 
SELECT * FROM KMD_STAGING.RAW.SCHOOLS_RAW WHERE municipality_code = 851;

CREATE OR REPLACE VIEW KMD_SCHOOLS.AALBORG.TEACHERS AS 
SELECT * FROM KMD_STAGING.RAW.TEACHERS_RAW WHERE municipality_code = 851;

CREATE OR REPLACE VIEW KMD_SCHOOLS.AALBORG.STUDENTS AS 
SELECT * FROM KMD_STAGING.RAW.STUDENTS_RAW WHERE municipality_code = 851;

CREATE OR REPLACE VIEW KMD_SCHOOLS.AALBORG.CLASSES AS 
SELECT * FROM KMD_STAGING.RAW.CLASSES_RAW WHERE municipality_code = 851;

-- ============================================================================
-- ESBJERG VIEWS (Municipality 561)
-- ============================================================================
CREATE OR REPLACE VIEW KMD_SCHOOLS.ESBJERG.SCHOOLS AS 
SELECT * FROM KMD_STAGING.RAW.SCHOOLS_RAW WHERE municipality_code = 561;

CREATE OR REPLACE VIEW KMD_SCHOOLS.ESBJERG.TEACHERS AS 
SELECT * FROM KMD_STAGING.RAW.TEACHERS_RAW WHERE municipality_code = 561;

CREATE OR REPLACE VIEW KMD_SCHOOLS.ESBJERG.STUDENTS AS 
SELECT * FROM KMD_STAGING.RAW.STUDENTS_RAW WHERE municipality_code = 561;

CREATE OR REPLACE VIEW KMD_SCHOOLS.ESBJERG.CLASSES AS 
SELECT * FROM KMD_STAGING.RAW.CLASSES_RAW WHERE municipality_code = 561;

-- ============================================================================
-- VERIFY TENANT VIEWS
-- ============================================================================
SELECT 'COPENHAGEN' as municipality, 
       (SELECT COUNT(*) FROM KMD_SCHOOLS.COPENHAGEN.SCHOOLS) as schools,
       (SELECT COUNT(*) FROM KMD_SCHOOLS.COPENHAGEN.STUDENTS) as students
UNION ALL SELECT 'AARHUS', 
       (SELECT COUNT(*) FROM KMD_SCHOOLS.AARHUS.SCHOOLS),
       (SELECT COUNT(*) FROM KMD_SCHOOLS.AARHUS.STUDENTS)
UNION ALL SELECT 'ODENSE', 
       (SELECT COUNT(*) FROM KMD_SCHOOLS.ODENSE.SCHOOLS),
       (SELECT COUNT(*) FROM KMD_SCHOOLS.ODENSE.STUDENTS)
UNION ALL SELECT 'AALBORG', 
       (SELECT COUNT(*) FROM KMD_SCHOOLS.AALBORG.SCHOOLS),
       (SELECT COUNT(*) FROM KMD_SCHOOLS.AALBORG.STUDENTS)
UNION ALL SELECT 'ESBJERG', 
       (SELECT COUNT(*) FROM KMD_SCHOOLS.ESBJERG.SCHOOLS),
       (SELECT COUNT(*) FROM KMD_SCHOOLS.ESBJERG.STUDENTS);
