/*
=============================================================================
KMD Denmark - Snowflake Onboarding Workshop
Database and Schema Setup Script
=============================================================================
This script creates the multi-tenant database structure for Danish municipalities.
Architecture: Schema-per-Tenant with shared reference data.
=============================================================================
*/

-- ============================================================================
-- SECTION 1: Create Databases
-- ============================================================================

USE ROLE SYSADMIN;

-- Main schools database (multi-tenant)
CREATE DATABASE IF NOT EXISTS KMD_SCHOOLS
    COMMENT = 'KMD Denmark - Multi-tenant School Data for Danish Municipalities';

-- Staging database for raw data ingestion
CREATE DATABASE IF NOT EXISTS KMD_STAGING
    COMMENT = 'KMD Denmark - Raw data landing zone for Snowpipe and batch loads';

-- Analytics database for cross-tenant reporting
CREATE DATABASE IF NOT EXISTS KMD_ANALYTICS
    COMMENT = 'KMD Denmark - Cross-tenant analytics and reporting layer';

-- ============================================================================
-- SECTION 2: Create Warehouse
-- ============================================================================

CREATE WAREHOUSE IF NOT EXISTS KMD_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'KMD Workshop compute warehouse';

USE WAREHOUSE KMD_WH;

-- ============================================================================
-- SECTION 3: Create Tenant Schemas (Municipality-specific)
-- ============================================================================

USE DATABASE KMD_SCHOOLS;

-- Copenhagen (Kobenhavn) - Largest municipality
CREATE SCHEMA IF NOT EXISTS COPENHAGEN
    COMMENT = 'Municipality: Copenhagen (101) - School data tenant';

-- Aarhus - Second largest
CREATE SCHEMA IF NOT EXISTS AARHUS
    COMMENT = 'Municipality: Aarhus (751) - School data tenant';

-- Odense - Third largest
CREATE SCHEMA IF NOT EXISTS ODENSE
    COMMENT = 'Municipality: Odense (461) - School data tenant';

-- Aalborg
CREATE SCHEMA IF NOT EXISTS AALBORG
    COMMENT = 'Municipality: Aalborg (851) - School data tenant';

-- Esbjerg
CREATE SCHEMA IF NOT EXISTS ESBJERG
    COMMENT = 'Municipality: Esbjerg (561) - School data tenant';

-- Shared reference data schema
CREATE SCHEMA IF NOT EXISTS SHARED
    COMMENT = 'Shared reference data across all municipalities';

-- ============================================================================
-- SECTION 4: Create Staging Schemas
-- ============================================================================

USE DATABASE KMD_STAGING;

CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Raw data landing zone (Bronze layer)';

CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Cleaned/validated data (Silver layer)';

CREATE SCHEMA IF NOT EXISTS EXTERNAL_STAGES
    COMMENT = 'External stage definitions for S3';

-- ============================================================================
-- SECTION 5: Create Analytics Schemas
-- ============================================================================

USE DATABASE KMD_ANALYTICS;

CREATE SCHEMA IF NOT EXISTS GOLD
    COMMENT = 'Aggregated analytics data (Gold layer)';

CREATE SCHEMA IF NOT EXISTS SEMANTIC
    COMMENT = 'Semantic layer for Cortex Analyst';

CREATE SCHEMA IF NOT EXISTS STREAMLIT
    COMMENT = 'Streamlit application data and stages';

-- ============================================================================
-- SECTION 6: Create Shared Reference Tables
-- ============================================================================

USE DATABASE KMD_SCHOOLS;
USE SCHEMA SHARED;

-- Municipality reference table
CREATE OR REPLACE TABLE DIM_MUNICIPALITIES (
    municipality_code VARCHAR(10) PRIMARY KEY,
    municipality_name VARCHAR(100) NOT NULL,
    region VARCHAR(100),
    population INT,
    area_km2 DECIMAL(10,2),
    mayor_name VARCHAR(200),
    website VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert municipality reference data
INSERT INTO DIM_MUNICIPALITIES (municipality_code, municipality_name, region, population, area_km2, is_active)
VALUES 
    ('101', 'Copenhagen', 'Hovedstaden', 650000, 86.4, TRUE),
    ('751', 'Aarhus', 'Midtjylland', 350000, 468.0, TRUE),
    ('461', 'Odense', 'Syddanmark', 205000, 304.3, TRUE),
    ('851', 'Aalborg', 'Nordjylland', 120000, 1137.0, TRUE),
    ('561', 'Esbjerg', 'Syddanmark', 72000, 795.0, TRUE);

-- Grade scale reference
CREATE OR REPLACE TABLE DIM_GRADE_SCALE (
    grade_value VARCHAR(5) PRIMARY KEY,
    grade_name VARCHAR(50),
    grade_description VARCHAR(255),
    numeric_value INT,
    is_passing BOOLEAN
);

INSERT INTO DIM_GRADE_SCALE VALUES
    ('12', 'Fremragende', 'Den fremragende praestation', 12, TRUE),
    ('10', 'Fortrinlig', 'Den fortrinlige praestation', 10, TRUE),
    ('7', 'God', 'Den gode praestation', 7, TRUE),
    ('4', 'Jaevn', 'Den jaevne praestation', 4, TRUE),
    ('02', 'Tilstraekkelig', 'Den tilstraekkelige praestation', 2, TRUE),
    ('00', 'Utilstraekkelig', 'Den utilstraekkelige praestation', 0, FALSE),
    ('-3', 'Ringe', 'Den ringe praestation', -3, FALSE);

-- Subject reference
CREATE OR REPLACE TABLE DIM_SUBJECTS (
    subject_code VARCHAR(20) PRIMARY KEY,
    subject_name VARCHAR(100) NOT NULL,
    subject_category VARCHAR(50),
    is_mandatory BOOLEAN DEFAULT TRUE,
    min_grade_level INT DEFAULT 0,
    max_grade_level INT DEFAULT 9
);

INSERT INTO DIM_SUBJECTS VALUES
    ('DAN', 'Dansk', 'Sprog', TRUE, 0, 9),
    ('MAT', 'Matematik', 'Naturvidenskab', TRUE, 0, 9),
    ('ENG', 'Engelsk', 'Sprog', TRUE, 3, 9),
    ('TYS', 'Tysk', 'Sprog', FALSE, 5, 9),
    ('FYS', 'Fysik/Kemi', 'Naturvidenskab', TRUE, 7, 9),
    ('BIO', 'Biologi', 'Naturvidenskab', TRUE, 7, 9),
    ('GEO', 'Geografi', 'Samfundsfag', TRUE, 7, 9),
    ('HIS', 'Historie', 'Samfundsfag', TRUE, 3, 9),
    ('SAM', 'Samfundsfag', 'Samfundsfag', TRUE, 8, 9),
    ('MUS', 'Musik', 'Kreativ', FALSE, 0, 6),
    ('BIL', 'Billedkunst', 'Kreativ', FALSE, 0, 5),
    ('IDR', 'Idraet', 'Fysisk', TRUE, 0, 9);

-- Special needs categories
CREATE OR REPLACE TABLE DIM_SPECIAL_NEEDS (
    category_code VARCHAR(20) PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL,
    category_description VARCHAR(500),
    support_level VARCHAR(20),
    additional_resources_needed BOOLEAN DEFAULT FALSE
);

INSERT INTO DIM_SPECIAL_NEEDS VALUES
    ('DYS', 'Dysleksi', 'Laesevanskeligheder', 'Medium', TRUE),
    ('ADHD', 'ADHD', 'Opmaarksomhedsforstyrelse', 'High', TRUE),
    ('AUT', 'Autisme', 'Autismespektrumforstyrrelse', 'High', TRUE),
    ('HOR', 'Horenedsaettelse', 'Nedsat horelse', 'Medium', TRUE),
    ('SYN', 'Synsvanskeligheder', 'Nedsat syn', 'Medium', TRUE),
    ('MOT', 'Motoriske vanskeligheder', 'Motoriske udfordringer', 'Medium', TRUE),
    ('SPR', 'Sprogvanskeligheder', 'Tale- og sprogvanskeligheder', 'Medium', TRUE),
    ('NONE', 'Ingen', 'Ingen saerlige behov', 'None', FALSE);

-- ============================================================================
-- SECTION 7: Create File Formats
-- ============================================================================

USE DATABASE KMD_STAGING;
USE SCHEMA EXTERNAL_STAGES;

-- CSV file format for standard imports
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    NULL_IF = ('NULL', 'null', '', 'NA', 'N/A')
    COMMENT = 'Standard CSV format for school data imports';

-- Parquet file format
CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
    TYPE = 'PARQUET'
    COMPRESSION = 'SNAPPY'
    COMMENT = 'Parquet format for efficient data storage';

-- JSON file format
CREATE OR REPLACE FILE FORMAT JSON_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    COMMENT = 'JSON format for API data imports';

-- ============================================================================
-- SECTION 8: Display Setup Summary
-- ============================================================================

SELECT 'Setup Complete!' AS status;

SELECT 
    'Databases Created' AS item,
    COUNT(*) AS count
FROM INFORMATION_SCHEMA.DATABASES
WHERE DATABASE_NAME LIKE 'KMD_%'

UNION ALL

SELECT 
    'Total Schemas' AS item,
    COUNT(*) AS count
FROM (
    SELECT SCHEMA_NAME FROM KMD_SCHOOLS.INFORMATION_SCHEMA.SCHEMATA
    UNION ALL
    SELECT SCHEMA_NAME FROM KMD_STAGING.INFORMATION_SCHEMA.SCHEMATA
    UNION ALL
    SELECT SCHEMA_NAME FROM KMD_ANALYTICS.INFORMATION_SCHEMA.SCHEMATA
);

SHOW DATABASES LIKE 'KMD_%';
