# KMD Denmark - Snowflake Onboarding Workshop
## Hands-on Lab Guide

---

## Workshop Overview

| Module | Topic | Duration |
|--------|-------|----------|
| 1 | Data Integration: External Stages & Snowpipe | 90 min |
| 2 | Data Integration: Streams & Tasks (CDC) | 45 min |
| 3 | Schema Evolution | 30 min |
| 4 | Transformation: Dynamic Data Masking | 30 min |
| 5 | Transformation: Dynamic Tables & dbt | 60 min |
| 6 | AI & Reporting: Semantic Views & Cortex | 60 min |
| 7 | AI & Reporting: Streamlit Dashboard | 45 min |

**Total Duration: ~6.5 hours** (with breaks = 8 hour day)

---

## Prerequisites

- Snowflake account with SYSADMIN role
- Storage Integration `TRACKMAN` configured for `s3://ubulut-iceberg-oregon/`
- CSV files uploaded to S3 bucket (already done)
- dbt-snowflake installed (for Module 5)

---

## S3 Data Structure

```
s3://ubulut-iceberg-oregon/kmd/
├── combined/                 # All municipalities combined
│   ├── dim_schools_all.csv
│   ├── dim_teachers_all.csv
│   ├── dim_students_all.csv
│   ├── dim_classes_all.csv
│   ├── fact_budgets_all.csv
│   ├── fact_wellness_all.csv
│   └── fact_attendance_sample.csv
├── copenhagen/               # Municipality-specific
│   ├── dim_schools.csv
│   ├── dim_teachers.csv
│   ├── dim_students.csv
│   └── ...
├── aarhus/
├── odense/
├── aalborg/
└── esbjerg/
```

---

## Module 1: Data Integration - External Stages & Snowpipe

### Learning Objectives
- Understand storage integrations and external stages
- Create external stages connecting to S3
- Load CSV data using COPY INTO
- Configure Snowpipe for automatic data ingestion

### Lab 1.1: Setup Database Structure

```sql
USE ROLE SYSADMIN;

-- Create databases for multi-tenant architecture
CREATE DATABASE IF NOT EXISTS KMD_SCHOOLS
    COMMENT = 'Multi-tenant School Data';

CREATE DATABASE IF NOT EXISTS KMD_STAGING
    COMMENT = 'Raw data landing zone';

CREATE DATABASE IF NOT EXISTS KMD_ANALYTICS
    COMMENT = 'Analytics layer';

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS KMD_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

USE WAREHOUSE KMD_WH;
```

### Lab 1.2: Create Tenant Schemas

```sql
USE DATABASE KMD_SCHOOLS;

-- Create schema per municipality (tenant isolation)
CREATE SCHEMA IF NOT EXISTS COPENHAGEN;
CREATE SCHEMA IF NOT EXISTS AARHUS;
CREATE SCHEMA IF NOT EXISTS ODENSE;
CREATE SCHEMA IF NOT EXISTS AALBORG;
CREATE SCHEMA IF NOT EXISTS ESBJERG;
CREATE SCHEMA IF NOT EXISTS SHARED;

-- Verify schemas
SHOW SCHEMAS IN DATABASE KMD_SCHOOLS;
```

### Lab 1.3: Create File Formats

```sql
USE DATABASE KMD_STAGING;
CREATE SCHEMA IF NOT EXISTS EXTERNAL_STAGES;
USE SCHEMA EXTERNAL_STAGES;

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
```

### Lab 1.4: Create External Stages

> **Note**: We use the existing `TRACKMAN` storage integration which has access to `s3://ubulut-iceberg-oregon/`

```sql
-- Main external stage for all KMD data (root)
CREATE OR REPLACE STAGE KMD_S3_STAGE
    STORAGE_INTEGRATION = TRACKMAN
    URL = 's3://ubulut-iceberg-oregon/kmd/'
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'Root external stage for all KMD school data';

-- Municipality-specific external stages
CREATE OR REPLACE STAGE COPENHAGEN_STAGE
    STORAGE_INTEGRATION = TRACKMAN
    URL = 's3://ubulut-iceberg-oregon/kmd/copenhagen/'
    FILE_FORMAT = CSV_FORMAT;

CREATE OR REPLACE STAGE AARHUS_STAGE
    STORAGE_INTEGRATION = TRACKMAN
    URL = 's3://ubulut-iceberg-oregon/kmd/aarhus/'
    FILE_FORMAT = CSV_FORMAT;

CREATE OR REPLACE STAGE ODENSE_STAGE
    STORAGE_INTEGRATION = TRACKMAN
    URL = 's3://ubulut-iceberg-oregon/kmd/odense/'
    FILE_FORMAT = CSV_FORMAT;

CREATE OR REPLACE STAGE AALBORG_STAGE
    STORAGE_INTEGRATION = TRACKMAN
    URL = 's3://ubulut-iceberg-oregon/kmd/aalborg/'
    FILE_FORMAT = CSV_FORMAT;

CREATE OR REPLACE STAGE ESBJERG_STAGE
    STORAGE_INTEGRATION = TRACKMAN
    URL = 's3://ubulut-iceberg-oregon/kmd/esbjerg/'
    FILE_FORMAT = CSV_FORMAT;

-- Combined data stage
CREATE OR REPLACE STAGE COMBINED_STAGE
    STORAGE_INTEGRATION = TRACKMAN
    URL = 's3://ubulut-iceberg-oregon/kmd/combined/'
    FILE_FORMAT = CSV_FORMAT;

-- Verify stages created
SHOW STAGES IN SCHEMA KMD_STAGING.EXTERNAL_STAGES;
```

### Lab 1.5: Verify S3 Connectivity

```sql
-- List all files in root stage
LIST @KMD_S3_STAGE;

-- List combined data files
LIST @COMBINED_STAGE;

-- List Copenhagen-specific files
LIST @COPENHAGEN_STAGE;
```

### Lab 1.6: Create RAW Tables

```sql
USE DATABASE KMD_STAGING;
CREATE SCHEMA IF NOT EXISTS RAW;
USE SCHEMA RAW;

-- Schools table
CREATE OR REPLACE TABLE SCHOOLS_RAW (
    school_id VARCHAR(50),
    municipality_code VARCHAR(10),
    school_name VARCHAR(200),
    school_type VARCHAR(50),
    address VARCHAR(300),
    postal_code VARCHAR(10),
    city VARCHAR(100),
    phone VARCHAR(20),
    email VARCHAR(200),
    founded_year INT,
    student_capacity INT,
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file VARCHAR(500)
);

-- Teachers table
CREATE OR REPLACE TABLE TEACHERS_RAW (
    teacher_id VARCHAR(50),
    school_id VARCHAR(50),
    municipality_code VARCHAR(10),
    cpr_number VARCHAR(20),
    cpr_masked VARCHAR(20),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    gender VARCHAR(10),
    birth_date DATE,
    email VARCHAR(200),
    phone VARCHAR(20),
    hire_date DATE,
    subjects VARCHAR(500),
    salary_band VARCHAR(10),
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file VARCHAR(500)
);

-- Students table
CREATE OR REPLACE TABLE STUDENTS_RAW (
    student_id VARCHAR(50),
    class_id VARCHAR(50),
    school_id VARCHAR(50),
    municipality_code VARCHAR(10),
    cpr_number VARCHAR(20),
    cpr_masked VARCHAR(20),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    gender VARCHAR(10),
    birth_date DATE,
    enrollment_date DATE,
    guardian_name VARCHAR(200),
    guardian_phone VARCHAR(20),
    guardian_email VARCHAR(200),
    address VARCHAR(300),
    postal_code VARCHAR(10),
    special_needs VARCHAR(100),
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file VARCHAR(500)
);

-- Classes table
CREATE OR REPLACE TABLE CLASSES_RAW (
    class_id VARCHAR(50),
    school_id VARCHAR(50),
    municipality_code VARCHAR(10),
    grade INT,
    section VARCHAR(10),
    class_name VARCHAR(50),
    academic_year VARCHAR(20),
    max_students INT,
    classroom_number VARCHAR(20),
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file VARCHAR(500)
);
```

### Lab 1.7: Load Data from S3

```sql
-- Load Schools
COPY INTO SCHOOLS_RAW (school_id, municipality_code, school_name, school_type, 
    address, postal_code, city, phone, email, founded_year, student_capacity, 
    is_active, created_at, updated_at)
FROM @KMD_STAGING.EXTERNAL_STAGES.COMBINED_STAGE/dim_schools_all.csv
FILE_FORMAT = (FORMAT_NAME = 'KMD_STAGING.EXTERNAL_STAGES.CSV_FORMAT')
ON_ERROR = 'CONTINUE';

-- Load Teachers
COPY INTO TEACHERS_RAW (teacher_id, school_id, municipality_code, cpr_number, 
    cpr_masked, first_name, last_name, gender, birth_date, email, phone, 
    hire_date, subjects, salary_band, is_active, created_at, updated_at)
FROM @KMD_STAGING.EXTERNAL_STAGES.COMBINED_STAGE/dim_teachers_all.csv
FILE_FORMAT = (FORMAT_NAME = 'KMD_STAGING.EXTERNAL_STAGES.CSV_FORMAT')
ON_ERROR = 'CONTINUE';

-- Load Students
COPY INTO STUDENTS_RAW (student_id, class_id, school_id, municipality_code, 
    cpr_number, cpr_masked, first_name, last_name, gender, birth_date, 
    enrollment_date, guardian_name, guardian_phone, guardian_email, address, 
    postal_code, special_needs, is_active, created_at, updated_at)
FROM @KMD_STAGING.EXTERNAL_STAGES.COMBINED_STAGE/dim_students_all.csv
FILE_FORMAT = (FORMAT_NAME = 'KMD_STAGING.EXTERNAL_STAGES.CSV_FORMAT')
ON_ERROR = 'CONTINUE';

-- Load Classes
COPY INTO CLASSES_RAW (class_id, school_id, municipality_code, grade, section, 
    class_name, academic_year, max_students, classroom_number, is_active, 
    created_at, updated_at)
FROM @KMD_STAGING.EXTERNAL_STAGES.COMBINED_STAGE/dim_classes_all.csv
FILE_FORMAT = (FORMAT_NAME = 'KMD_STAGING.EXTERNAL_STAGES.CSV_FORMAT')
ON_ERROR = 'CONTINUE';

-- Verify row counts
SELECT 'SCHOOLS_RAW' as table_name, COUNT(*) as rows FROM SCHOOLS_RAW
UNION ALL SELECT 'TEACHERS_RAW', COUNT(*) FROM TEACHERS_RAW
UNION ALL SELECT 'STUDENTS_RAW', COUNT(*) FROM STUDENTS_RAW
UNION ALL SELECT 'CLASSES_RAW', COUNT(*) FROM CLASSES_RAW;
```

**Expected Results:**
| Table | Rows |
|-------|------|
| SCHOOLS_RAW | 35 |
| TEACHERS_RAW | 993 |
| STUDENTS_RAW | 14,614 |
| CLASSES_RAW | 699 |

### Lab 1.8: Configure Snowpipe (Conceptual)

```sql
-- Snowpipe for automatic ingestion when new files arrive
-- Note: Requires S3 event notification setup in AWS

CREATE OR REPLACE PIPE SCHOOLS_PIPE
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest for school data'
AS
COPY INTO KMD_STAGING.RAW.SCHOOLS_RAW (school_id, municipality_code, school_name, 
    school_type, address, postal_code, city, phone, email, founded_year, 
    student_capacity, is_active, created_at, updated_at)
FROM @KMD_STAGING.EXTERNAL_STAGES.KMD_S3_STAGE/schools/
FILE_FORMAT = (FORMAT_NAME = 'KMD_STAGING.EXTERNAL_STAGES.CSV_FORMAT');

-- Get notification channel ARN for AWS S3 setup
SHOW PIPES LIKE 'SCHOOLS_PIPE';
-- Use the notification_channel value to configure S3 bucket event notifications

-- Monitor pipe status
SELECT SYSTEM$PIPE_STATUS('SCHOOLS_PIPE');

-- View copy history
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'SCHOOLS_RAW',
    START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
));
```

### Best Practices Discussion
- Use storage integrations (not credentials) for security
- Create separate stages per data domain/tenant
- Use separate pipes per table for monitoring
- Implement ON_ERROR = 'CONTINUE' for fault tolerance
- Monitor with COPY_HISTORY and PIPE_STATUS

---

## Module 2: Streams & Tasks (CDC)

### Learning Objectives
- Create streams for change data capture
- Build tasks for automated processing
- Implement incremental data pipelines

### Lab 2.1: Create Stream on Source Table

```sql
USE DATABASE KMD_STAGING;
USE SCHEMA RAW;

-- Create stream to track changes
CREATE OR REPLACE STREAM SCHOOLS_STREAM
    ON TABLE SCHOOLS_RAW
    APPEND_ONLY = FALSE
    COMMENT = 'CDC stream for schools';

-- Verify stream
SHOW STREAMS;

-- Check if stream has data
SELECT SYSTEM$STREAM_HAS_DATA('SCHOOLS_STREAM');
```

### Lab 2.2: Create Staging Table (Silver Layer)

```sql
CREATE SCHEMA IF NOT EXISTS STAGING;
USE SCHEMA STAGING;

CREATE OR REPLACE TABLE DIM_SCHOOLS (
    school_id VARCHAR(50) PRIMARY KEY,
    municipality_code VARCHAR(10),
    school_name VARCHAR(200),
    school_type VARCHAR(50),
    address VARCHAR(300),
    postal_code VARCHAR(10),
    city VARCHAR(100),
    phone VARCHAR(20),
    email VARCHAR(200),
    founded_year INT,
    student_capacity INT,
    is_active BOOLEAN,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### Lab 2.3: Create Task for CDC Processing

```sql
USE SCHEMA RAW;

CREATE OR REPLACE TASK PROCESS_SCHOOLS_CDC
    WAREHOUSE = KMD_WH
    SCHEDULE = '5 MINUTE'
    COMMENT = 'Process schools CDC'
WHEN
    SYSTEM$STREAM_HAS_DATA('SCHOOLS_STREAM')
AS
MERGE INTO KMD_STAGING.STAGING.DIM_SCHOOLS tgt
USING (
    SELECT 
        school_id,
        municipality_code,
        TRIM(school_name) AS school_name,
        school_type,
        address,
        postal_code,
        city,
        phone,
        LOWER(email) AS email,
        founded_year,
        student_capacity,
        COALESCE(is_active, TRUE) AS is_active
    FROM SCHOOLS_STREAM
    WHERE METADATA$ACTION = 'INSERT'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY school_id ORDER BY _loaded_at DESC) = 1
) src
ON tgt.school_id = src.school_id
WHEN MATCHED THEN UPDATE SET
    municipality_code = src.municipality_code,
    school_name = src.school_name,
    school_type = src.school_type,
    address = src.address,
    postal_code = src.postal_code,
    city = src.city,
    phone = src.phone,
    email = src.email,
    founded_year = src.founded_year,
    student_capacity = src.student_capacity,
    is_active = src.is_active,
    updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    school_id, municipality_code, school_name, school_type, address,
    postal_code, city, phone, email, founded_year, student_capacity, is_active
) VALUES (
    src.school_id, src.municipality_code, src.school_name, src.school_type,
    src.address, src.postal_code, src.city, src.phone, src.email,
    src.founded_year, src.student_capacity, src.is_active
);

-- Enable the task
ALTER TASK PROCESS_SCHOOLS_CDC RESUME;
```

### Lab 2.4: Test CDC Pipeline

```sql
-- Manually execute task to process existing data
EXECUTE TASK PROCESS_SCHOOLS_CDC;

-- Verify data in staging
SELECT COUNT(*) FROM KMD_STAGING.STAGING.DIM_SCHOOLS;

-- Insert a new record to test incremental processing
INSERT INTO KMD_STAGING.RAW.SCHOOLS_RAW 
(school_id, municipality_code, school_name, school_type, student_capacity, is_active)
VALUES ('TEST-SCH-001', '101', 'Test Skole', 'Folkeskole', 500, TRUE);

-- Check stream has data
SELECT SYSTEM$STREAM_HAS_DATA('SCHOOLS_STREAM');

-- Execute task again
EXECUTE TASK PROCESS_SCHOOLS_CDC;

-- Verify new data
SELECT * FROM KMD_STAGING.STAGING.DIM_SCHOOLS WHERE school_id = 'TEST-SCH-001';

-- Cleanup test data
DELETE FROM KMD_STAGING.RAW.SCHOOLS_RAW WHERE school_id = 'TEST-SCH-001';
DELETE FROM KMD_STAGING.STAGING.DIM_SCHOOLS WHERE school_id = 'TEST-SCH-001';
```

---

## Module 3: Schema Evolution

### Learning Objectives
- Understand schema evolution in Snowflake
- Handle new columns, missing columns, renamed columns
- Configure tables for automatic schema evolution

### Lab 3.1: Enable Schema Evolution

```sql
USE DATABASE KMD_STAGING;
CREATE SCHEMA IF NOT EXISTS SCHEMA_EVOLUTION_DEMO;
USE SCHEMA SCHEMA_EVOLUTION_DEMO;

-- Create table with schema evolution enabled
CREATE OR REPLACE TABLE SCHOOLS_EVOLUTION (
    school_id VARCHAR(50),
    municipality_code VARCHAR(10),
    school_name VARCHAR(200),
    school_type VARCHAR(50),
    student_capacity INT,
    is_active BOOLEAN,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file VARCHAR(500)
)
ENABLE_SCHEMA_EVOLUTION = TRUE;

-- Load initial data
INSERT INTO SCHOOLS_EVOLUTION (school_id, municipality_code, school_name, school_type, student_capacity, is_active, _source_file)
VALUES 
    ('101-SCH-001', '101', 'Kirke Folkeskole', 'Folkeskole', 529, TRUE, 'schools_v1.csv'),
    ('101-SCH-002', '101', 'Slot Skole', 'Folkeskole', 609, TRUE, 'schools_v1.csv');

SELECT * FROM SCHOOLS_EVOLUTION;
```

### Lab 3.2: Scenario - New Column Added

```sql
-- What happens when new CSV has additional column 'email'?
-- Snowflake automatically adds the column with ENABLE_SCHEMA_EVOLUTION = TRUE

-- Simulate by adding column
ALTER TABLE SCHOOLS_EVOLUTION ADD COLUMN email VARCHAR(200);

-- Insert data with the new column
INSERT INTO SCHOOLS_EVOLUTION (school_id, municipality_code, school_name, school_type, student_capacity, is_active, email, _source_file)
VALUES ('101-SCH-003', '101', 'Sondre Skole', 'Folkeskole', 449, TRUE, 'kontor@sondre.dk', 'schools_v2.csv');

-- Notice: Old rows have NULL for email
SELECT school_id, school_name, email, _source_file FROM SCHOOLS_EVOLUTION;
```

### Lab 3.3: Scenario - Column Missing

```sql
-- What happens when new CSV is missing 'student_capacity'?
-- Snowflake drops NOT NULL constraint, new rows get NULL

INSERT INTO SCHOOLS_EVOLUTION (school_id, municipality_code, school_name, school_type, is_active, email, _source_file)
VALUES ('461-SCH-001', '461', 'Odense Friskole', 'Friskole', TRUE, 'kontor@odense.dk', 'schools_v3_missing_capacity.csv');

-- Check: student_capacity is NULL
SELECT school_id, school_name, student_capacity, _source_file 
FROM SCHOOLS_EVOLUTION
ORDER BY _loaded_at DESC;
```

### Lab 3.4: Scenario - Column Renamed (THE TRICKY ONE!)

```sql
-- CRITICAL: Snowflake CANNOT detect column renames!
-- 'school_name' renamed to 'skole_navn' is treated as:
--   1. NEW column 'skole_navn'
--   2. OLD column 'school_name' missing

-- Simulate renamed column
ALTER TABLE SCHOOLS_EVOLUTION ADD COLUMN skole_navn VARCHAR(200);

INSERT INTO SCHOOLS_EVOLUTION (school_id, municipality_code, skole_navn, school_type, student_capacity, is_active, _source_file)
VALUES ('851-SCH-001', '851', 'Aalborg Nordvest Skole', 'Folkeskole', 380, TRUE, 'schools_v4_renamed.csv');

-- PROBLEM: school_name is NULL, skole_navn has value
SELECT school_id, school_name, skole_navn, _source_file FROM SCHOOLS_EVOLUTION;

-- SOLUTION: Use COALESCE in a view
CREATE OR REPLACE VIEW V_SCHOOLS_UNIFIED AS
SELECT 
    school_id,
    municipality_code,
    COALESCE(school_name, skole_navn) AS school_name,
    school_type,
    student_capacity,
    is_active,
    email,
    _source_file
FROM SCHOOLS_EVOLUTION;

SELECT * FROM V_SCHOOLS_UNIFIED;
```

### Schema Evolution Summary

| Scenario | Snowflake Behavior | Your Action |
|----------|-------------------|-------------|
| New Column Added | Auto-added | None - automatic |
| Column Missing | NOT NULL dropped, NULLs inserted | Monitor data quality |
| Column Renamed | Treated as new + missing | Manual: COALESCE view |
| Type Changed | COPY fails or truncates | Manual: Transform in COPY |
| Order Changed | MATCH_BY_COLUMN_NAME handles | Use CASE_INSENSITIVE |

---

## Module 4: Dynamic Data Masking

### Learning Objectives
- Understand masking policy concepts
- Create role-based masking policies
- Apply masking to PII columns (Danish CPR numbers)

### Lab 4.1: Create Masking Policies

```sql
USE DATABASE KMD_SCHOOLS;
USE SCHEMA SHARED;

-- Create CPR masking policy (Danish personal ID: DDMMYY-XXXX)
CREATE OR REPLACE MASKING POLICY CPR_MASK AS (val STRING) 
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'KMD_PII_ADMIN') 
            THEN val
        WHEN val IS NULL 
            THEN NULL
        ELSE 
            SUBSTRING(val, 1, 7) || 'XXXX'  -- Show DDMMYY- but mask last 4 digits
    END;

-- Create email masking policy
CREATE OR REPLACE MASKING POLICY EMAIL_MASK AS (val STRING) 
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'KMD_PII_ADMIN') 
            THEN val
        WHEN val IS NULL 
            THEN NULL
        ELSE 
            SUBSTRING(val, 1, 1) || '***@' || SPLIT_PART(val, '@', 2)
    END;
```

### Lab 4.2: Apply Masking to Demo Table

```sql
-- Create demo table with PII
CREATE OR REPLACE TABLE MASKING_DEMO (
    id INT,
    name VARCHAR(100),
    cpr_number VARCHAR(20),
    email VARCHAR(200)
);

INSERT INTO MASKING_DEMO VALUES
    (1, 'Magnus Jensen', '010190-1234', 'magnus.jensen@email.dk'),
    (2, 'Emma Nielsen', '150285-5678', 'emma.nielsen@school.dk'),
    (3, 'Oliver Andersen', '220398-9012', 'oliver.andersen@kmd.dk');

-- Apply masking policies
ALTER TABLE MASKING_DEMO 
    MODIFY COLUMN cpr_number SET MASKING POLICY CPR_MASK;

ALTER TABLE MASKING_DEMO 
    MODIFY COLUMN email SET MASKING POLICY EMAIL_MASK;
```

### Lab 4.3: Test Masking

```sql
-- Test as SYSADMIN (full access)
USE ROLE SYSADMIN;
SELECT * FROM MASKING_DEMO;
-- Expected: Full CPR and email visible

-- Create a test role to see masking in action
USE ROLE ACCOUNTADMIN;
CREATE ROLE IF NOT EXISTS KMD_ANALYST;
GRANT USAGE ON DATABASE KMD_SCHOOLS TO ROLE KMD_ANALYST;
GRANT USAGE ON SCHEMA KMD_SCHOOLS.SHARED TO ROLE KMD_ANALYST;
GRANT SELECT ON TABLE KMD_SCHOOLS.SHARED.MASKING_DEMO TO ROLE KMD_ANALYST;
GRANT USAGE ON WAREHOUSE KMD_WH TO ROLE KMD_ANALYST;

-- Test as KMD_ANALYST (masked)
USE ROLE KMD_ANALYST;
SELECT * FROM KMD_SCHOOLS.SHARED.MASKING_DEMO;
-- Expected: CPR shows as 010190-XXXX, email shows as m***@email.dk

-- Return to SYSADMIN
USE ROLE SYSADMIN;
SHOW MASKING POLICIES IN SCHEMA KMD_SCHOOLS.SHARED;
```

---

## Module 5: Dynamic Tables & dbt

### Learning Objectives
- Create dynamic tables with automatic refresh
- Understand target lag configuration
- Run dbt models on Snowflake

### Lab 5.1: Create Dynamic Tables

```sql
USE DATABASE KMD_ANALYTICS;
CREATE SCHEMA IF NOT EXISTS GOLD;
USE SCHEMA GOLD;

-- Create Dynamic Table for schools dimension
CREATE OR REPLACE DYNAMIC TABLE DT_DIM_SCHOOLS
    TARGET_LAG = '15 minutes'
    WAREHOUSE = KMD_WH
AS
SELECT 
    s.school_id,
    s.municipality_code,
    s.school_name,
    s.school_type,
    s.address,
    s.city,
    s.student_capacity,
    s.is_active,
    COUNT(DISTINCT st.student_id) as student_count,
    CURRENT_TIMESTAMP() AS refreshed_at
FROM KMD_STAGING.STAGING.DIM_SCHOOLS s
LEFT JOIN KMD_STAGING.RAW.STUDENTS_RAW st ON s.school_id = st.school_id
GROUP BY 1,2,3,4,5,6,7,8;

-- Check dynamic table status
SHOW DYNAMIC TABLES;

-- Monitor refresh history
SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    NAME => 'DT_DIM_SCHOOLS'
));

-- Query the dynamic table
SELECT * FROM DT_DIM_SCHOOLS LIMIT 10;
```

### Lab 5.2: dbt Project Setup

```bash
# Navigate to dbt project
cd 03_transformation/dbt_project

# Copy profile template and configure
cp profiles.yml.template ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with your Snowflake credentials

# Install dbt dependencies
dbt deps

# Test connection
dbt debug

# Load seed data (reference tables)
dbt seed

# Run staging models
dbt run --select staging

# Run all models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### Lab 5.3: Verify dbt Models

```sql
-- Check dbt-created objects
USE DATABASE KMD_STAGING;

-- Staging views
SELECT * FROM STAGING.STG_SCHOOLS LIMIT 5;
SELECT * FROM STAGING.STG_TEACHERS LIMIT 5;

-- Gold tables
SELECT * FROM GOLD.DIM_SCHOOLS LIMIT 5;
SELECT * FROM GOLD.DIM_STUDENTS LIMIT 5;
SELECT * FROM GOLD.FCT_GRADE_PERFORMANCE LIMIT 10;
```

---

## Module 6: Semantic Views & Cortex Analyst

### Learning Objectives
- Create semantic views for natural language queries
- Test Cortex Analyst functionality
- Build verified queries

### Lab 6.1: Create Analytics View

```sql
USE DATABASE KMD_ANALYTICS;
USE SCHEMA GOLD;

-- Create comprehensive analytics view
CREATE OR REPLACE VIEW V_SCHOOL_ANALYTICS AS
SELECT 
    s.school_id,
    s.school_name,
    s.school_type,
    s.municipality_code,
    COALESCE(sc.student_count, 0) as enrolled_students,
    COALESCE(tc.teacher_count, 0) as teacher_count,
    ROUND(COALESCE(sc.student_count, 0)::FLOAT / NULLIF(tc.teacher_count, 0), 1) as student_teacher_ratio
FROM KMD_STAGING.STAGING.DIM_SCHOOLS s
LEFT JOIN (
    SELECT school_id, COUNT(*) as student_count
    FROM KMD_STAGING.RAW.STUDENTS_RAW
    WHERE is_active = TRUE
    GROUP BY school_id
) sc ON s.school_id = sc.school_id
LEFT JOIN (
    SELECT school_id, COUNT(*) as teacher_count
    FROM KMD_STAGING.RAW.TEACHERS_RAW
    WHERE is_active = TRUE
    GROUP BY school_id
) tc ON s.school_id = tc.school_id;

SELECT * FROM V_SCHOOL_ANALYTICS LIMIT 10;
```

### Lab 6.2: Test Cortex Analyst (in Snowsight)

1. Open Snowsight → Data → Databases
2. Navigate to KMD_ANALYTICS.GOLD
3. Find the semantic model YAML (upload if needed)
4. Open Cortex Analyst
5. Try these questions:
   - "How many schools are in Copenhagen?"
   - "What is the average student capacity?"
   - "Show me schools with more than 500 students"
   - "Which municipality has the most schools?"

---

## Module 7: Streamlit Dashboard

### Learning Objectives
- Deploy Streamlit app to Snowflake
- Connect to analytics data
- Create interactive visualizations

### Lab 7.1: Setup Streamlit

```sql
USE DATABASE KMD_ANALYTICS;
CREATE SCHEMA IF NOT EXISTS STREAMLIT;
USE SCHEMA STREAMLIT;

-- Create stage for Streamlit files
CREATE OR REPLACE STAGE STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE);
```

### Lab 7.2: Deploy Streamlit App (in Snowsight)

1. Go to Snowsight → Projects → Streamlit
2. Click "+ Streamlit App"
3. Name: KMD_DASHBOARD
4. Database: KMD_ANALYTICS
5. Schema: STREAMLIT
6. Warehouse: KMD_WH
7. Copy the code from `04_ai_reporting/streamlit_apps/school_dashboard.py`
8. Click "Run"

---

## Cleanup (Optional)

```sql
-- Suspend tasks
ALTER TASK KMD_STAGING.RAW.PROCESS_SCHOOLS_CDC SUSPEND;

-- Remove all workshop resources
DROP DATABASE IF EXISTS KMD_SCHOOLS;
DROP DATABASE IF EXISTS KMD_STAGING;
DROP DATABASE IF EXISTS KMD_ANALYTICS;
DROP WAREHOUSE IF EXISTS KMD_WH;
DROP ROLE IF EXISTS KMD_ANALYST;
```

---

## Key Takeaways

1. **Storage Integration + External Stages**: Secure S3 access without credentials in code
2. **COPY INTO**: File-based loading with MATCH_BY_COLUMN_NAME for flexibility
3. **Schema Evolution**: Handle changing file structures automatically (except renames!)
4. **Streams & Tasks**: Native CDC for incremental processing
5. **Dynamic Masking**: Role-based PII protection without code changes
6. **Dynamic Tables**: Declarative materialized views with auto-refresh
7. **dbt**: Structured transformation with testing and documentation
8. **Cortex Analyst**: Natural language queries on structured data
9. **Streamlit**: Native dashboards within Snowflake

---

## Resources

- [Snowflake Documentation](https://docs.snowflake.com)
- [Storage Integrations](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration)
- [Schema Evolution](https://docs.snowflake.com/en/user-guide/data-load-schema-evolution)
- [dbt-snowflake](https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup)
- [Cortex Analyst](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst)
- [Streamlit in Snowflake](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)
