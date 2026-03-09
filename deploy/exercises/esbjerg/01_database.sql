-- ============================================================================
-- STUDENT EXERCISE - STEP 1: DATABASE & SCHEMAS
-- ============================================================================
-- Replace ESBJERG with your assigned municipality name (uppercase)
-- Replace 561 with your municipality code
--
-- Municipality Codes:
--   COPENHAGEN = 101
--   AARHUS     = 751
--   ODENSE     = 461
--   AALBORG    = 851
--   ESBJERG    = 561
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE KMD_WH;

-- Create your municipality database
CREATE DATABASE IF NOT EXISTS ESBJERG_DB;

USE DATABASE ESBJERG_DB;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS RAW;      -- Landing zone for Snowpipe
CREATE SCHEMA IF NOT EXISTS CLEAN;    -- Deduplicated silver layer
CREATE SCHEMA IF NOT EXISTS CDC;      -- Streams and Tasks

-- Verify
SHOW SCHEMAS IN DATABASE ESBJERG_DB;
