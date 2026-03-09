-- ============================================================================
-- STUDENT EXERCISE - STEP 1: DATABASE & SCHEMAS
-- ============================================================================
-- Replace {MUNICIPALITY} with your assigned municipality name (uppercase)
-- Replace {CODE} with your municipality code
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
CREATE DATABASE IF NOT EXISTS {MUNICIPALITY}_DB;

USE DATABASE {MUNICIPALITY}_DB;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS RAW;      -- Landing zone for Snowpipe
CREATE SCHEMA IF NOT EXISTS CLEAN;    -- Deduplicated silver layer
CREATE SCHEMA IF NOT EXISTS CDC;      -- Streams and Tasks

-- Verify
SHOW SCHEMAS IN DATABASE {MUNICIPALITY}_DB;
