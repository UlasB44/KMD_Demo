-- ============================================================================
-- KMD WORKSHOP - STEP 2: EXTERNAL STAGES
-- ============================================================================
-- Creates external stages pointing to S3
-- Requires: KMD_S3_INTEGRATION storage integration
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE KMD_STAGING;
USE SCHEMA EXTERNAL_STAGES;
USE WAREHOUSE KMD_WH;

-- ============================================================================
-- EXTERNAL STAGES (7 total)
-- Update URLs if your S3 bucket/path is different
-- ============================================================================

-- Root stage for all KMD data
CREATE OR REPLACE STAGE KMD_S3_STAGE
    STORAGE_INTEGRATION = KMD_S3_INTEGRATION
    URL = 's3://ubulut-iceberg-oregon/data/'
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'Root external stage for all KMD school data';

-- Municipality-specific stages
CREATE OR REPLACE STAGE COPENHAGEN_STAGE
    STORAGE_INTEGRATION = KMD_S3_INTEGRATION
    URL = 's3://ubulut-iceberg-oregon/data/copenhagen/'
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'External stage for Copenhagen municipality data';

CREATE OR REPLACE STAGE AARHUS_STAGE
    STORAGE_INTEGRATION = KMD_S3_INTEGRATION
    URL = 's3://ubulut-iceberg-oregon/data/aarhus/'
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'External stage for Aarhus municipality data';

CREATE OR REPLACE STAGE ODENSE_STAGE
    STORAGE_INTEGRATION = KMD_S3_INTEGRATION
    URL = 's3://ubulut-iceberg-oregon/data/odense/'
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'External stage for Odense municipality data';

CREATE OR REPLACE STAGE AALBORG_STAGE
    STORAGE_INTEGRATION = KMD_S3_INTEGRATION
    URL = 's3://ubulut-iceberg-oregon/data/aalborg/'
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'External stage for Aalborg municipality data';

CREATE OR REPLACE STAGE ESBJERG_STAGE
    STORAGE_INTEGRATION = KMD_S3_INTEGRATION
    URL = 's3://ubulut-iceberg-oregon/data/esbjerg/'
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'External stage for Esbjerg municipality data';

CREATE OR REPLACE STAGE COMBINED_STAGE
    STORAGE_INTEGRATION = KMD_S3_INTEGRATION
    URL = 's3://ubulut-iceberg-oregon/data/combined/'
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'External stage for combined municipality data';

-- Verify stages
SHOW STAGES IN SCHEMA KMD_STAGING.EXTERNAL_STAGES;
