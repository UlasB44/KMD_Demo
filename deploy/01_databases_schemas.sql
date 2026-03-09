-- ============================================================================
-- KMD WORKSHOP - STEP 1: DATABASE & SCHEMA SETUP
-- ============================================================================
-- Creates all databases, schemas, and file formats
-- ============================================================================

USE ROLE SYSADMIN;
USE WAREHOUSE KMD_WH;

-- ============================================================================
-- DATABASES
-- ============================================================================
CREATE DATABASE IF NOT EXISTS KMD_SCHOOLS COMMENT = 'Multi-tenant School Data';
CREATE DATABASE IF NOT EXISTS KMD_STAGING COMMENT = 'Raw data landing zone';
CREATE DATABASE IF NOT EXISTS KMD_ANALYTICS COMMENT = 'Analytics layer';

-- ============================================================================
-- SCHEMAS - KMD_SCHOOLS (Multi-tenant: schema per municipality)
-- ============================================================================
USE DATABASE KMD_SCHOOLS;
CREATE SCHEMA IF NOT EXISTS COPENHAGEN;   -- Municipality 101
CREATE SCHEMA IF NOT EXISTS AARHUS;       -- Municipality 751
CREATE SCHEMA IF NOT EXISTS ODENSE;       -- Municipality 461
CREATE SCHEMA IF NOT EXISTS AALBORG;      -- Municipality 851
CREATE SCHEMA IF NOT EXISTS ESBJERG;      -- Municipality 561
CREATE SCHEMA IF NOT EXISTS SHARED;       -- Shared objects (policies, reference data)

-- ============================================================================
-- SCHEMAS - KMD_STAGING
-- ============================================================================
USE DATABASE KMD_STAGING;
CREATE SCHEMA IF NOT EXISTS EXTERNAL_STAGES;
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS CLEAN;
CREATE SCHEMA IF NOT EXISTS CDC;

-- ============================================================================
-- SCHEMAS - KMD_ANALYTICS
-- ============================================================================
USE DATABASE KMD_ANALYTICS;
CREATE SCHEMA IF NOT EXISTS MARTS;
CREATE SCHEMA IF NOT EXISTS SEMANTIC_MODELS;

-- ============================================================================
-- FILE FORMATS
-- ============================================================================
USE DATABASE KMD_STAGING;
USE SCHEMA EXTERNAL_STAGES;

-- Standard CSV format for data loading
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    TRIM_SPACE = TRUE;

-- CSV format with schema evolution support (uses PARSE_HEADER for column name matching)
CREATE OR REPLACE FILE FORMAT CSV_SCHEMA_EVOLUTION
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    PARSE_HEADER = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;
