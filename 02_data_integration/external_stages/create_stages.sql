/*
=============================================================================
KMD Denmark - External Stage Setup
Creates S3 external stages for data ingestion from AWS
Uses existing TRACKMAN storage integration
=============================================================================
*/

USE ROLE SYSADMIN;
USE DATABASE KMD_STAGING;
CREATE SCHEMA IF NOT EXISTS EXTERNAL_STAGES;
USE SCHEMA EXTERNAL_STAGES;
USE WAREHOUSE KMD_WH;

-- ============================================================================
-- SECTION 1: File Formats
-- ============================================================================

-- Standard CSV format for data loading
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    TRIM_SPACE = TRUE;

-- CSV format with schema evolution support
CREATE OR REPLACE FILE FORMAT CSV_SCHEMA_EVOLUTION
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    PARSE_HEADER = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- ============================================================================
-- SECTION 2: External Stages (using KMD_S3_INTEGRATION storage integration)
-- ============================================================================

-- Main external stage for all KMD data (root)
CREATE OR REPLACE STAGE KMD_S3_STAGE
    STORAGE_INTEGRATION = KMD_S3_INTEGRATION
    URL = 's3://ubulut-iceberg-oregon/data/'
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'Root external stage for all KMD school data';

-- Municipality-specific external stages
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

-- Combined data stage (all municipalities)
CREATE OR REPLACE STAGE COMBINED_STAGE
    STORAGE_INTEGRATION = KMD_S3_INTEGRATION
    URL = 's3://ubulut-iceberg-oregon/data/combined/'
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'External stage for combined municipality data';

-- ============================================================================
-- SECTION 3: Verify Stage Setup
-- ============================================================================

-- List all stages
SHOW STAGES IN SCHEMA KMD_STAGING.EXTERNAL_STAGES;

-- Verify S3 connectivity
LIST @KMD_S3_STAGE;
LIST @COMBINED_STAGE;
LIST @COPENHAGEN_STAGE;

-- ============================================================================
-- SECTION 4: Usage Examples
-- ============================================================================

/*
-- Load schools from combined stage:
COPY INTO KMD_STAGING.RAW.SCHOOLS_RAW
FROM @COMBINED_STAGE/dim_schools_all.csv
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Load Copenhagen-specific data:
COPY INTO KMD_STAGING.RAW.SCHOOLS_RAW
FROM @COPENHAGEN_STAGE/dim_schools.csv
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Load with schema evolution (auto-add new columns):
COPY INTO KMD_STAGING.RAW.SCHOOLS_EVOLUTION
FROM @COMBINED_STAGE/dim_schools_all.csv
FILE_FORMAT = CSV_SCHEMA_EVOLUTION
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';
*/

SELECT 'External stages created successfully!' AS status;
