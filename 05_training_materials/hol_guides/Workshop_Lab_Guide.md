# KMD Denmark - Snowflake Onboarding Workshop
## Hands-on Lab Guide

---

## Workshop Overview

| Module | Topic | Duration |
|--------|-------|----------|
| 1 | Data Integration: External Stages & Snowpipe | 90 min |
| 2 | Data Integration: Streams & Tasks (CDC) | 45 min |
| 3 | Transformation: Dynamic Data Masking | 30 min |
| 4 | Transformation: Dynamic Tables & dbt | 60 min |
| 5 | AI & Reporting: Semantic Views & Cortex | 60 min |
| 6 | AI & Reporting: Streamlit Dashboard | 45 min |

---

## Prerequisites

- Snowflake account with SYSADMIN or ACCOUNTADMIN role
- Access to AWS S3 bucket: `s3://ubulut-iceberg-oregon/kmd/`
- Python 3.9+ installed (for data generation)
- dbt-snowflake installed (optional, for Module 4)

---

## Module 1: Data Integration - External Stages & Snowpipe

### Learning Objectives
- Create external stages connecting to S3
- Configure Snowpipe for automatic data ingestion
- Understand best practices for file-based data loading

### Lab 1.1: Setup Database Structure

```sql
-- Connect to Snowflake and run the setup script
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

### Lab 1.3: Create External Stage

```sql
USE DATABASE KMD_STAGING;
CREATE SCHEMA IF NOT EXISTS EXTERNAL_STAGES;
USE SCHEMA EXTERNAL_STAGES;

-- Create file format for CSV
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '');

-- Create internal stage for workshop (no AWS credentials needed)
CREATE OR REPLACE STAGE WORKSHOP_STAGE
    FILE_FORMAT = CSV_FORMAT;

-- List stage contents
LIST @WORKSHOP_STAGE;
```

### Lab 1.4: Upload Sample Data

```sql
-- In Snowsight, use the "Load Data" wizard or SnowSQL:
-- PUT file:///path/to/dim_schools.csv @WORKSHOP_STAGE/schools/;

-- For workshop, let's create sample data directly
USE DATABASE KMD_STAGING;
CREATE SCHEMA IF NOT EXISTS RAW;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE SCHOOLS_RAW (
    school_id VARCHAR(50),
    municipality_code VARCHAR(10),
    school_name VARCHAR(200),
    school_type VARCHAR(50),
    student_capacity INT,
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert sample data
INSERT INTO SCHOOLS_RAW (school_id, municipality_code, school_name, school_type, student_capacity, is_active)
VALUES 
    ('101-SCH-001', '101', 'Nordre Skole', 'Folkeskole', 450, TRUE),
    ('101-SCH-002', '101', 'Centrum Folkeskole', 'Folkeskole', 380, TRUE),
    ('751-SCH-001', '751', 'Aarhus Friskole', 'Friskole', 220, TRUE),
    ('461-SCH-001', '461', 'Odense Privatskole', 'Privatskole', 300, TRUE);

SELECT * FROM SCHOOLS_RAW;
```

### Lab 1.5: Create Snowpipe (Conceptual)

```sql
-- Note: Snowpipe requires S3 event notifications
-- This demonstrates the pipe definition

CREATE OR REPLACE PIPE SCHOOLS_PIPE
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest for school data'
AS
COPY INTO KMD_STAGING.RAW.SCHOOLS_RAW
FROM @KMD_STAGING.EXTERNAL_STAGES.WORKSHOP_STAGE/schools/
FILE_FORMAT = CSV_FORMAT;

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('SCHOOLS_PIPE');

-- View copy history
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'SCHOOLS_RAW',
    START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
));
```

### Best Practices Discussion
- Use separate pipes per table
- Implement error handling with ON_ERROR
- Monitor with COPY_HISTORY and PIPE_STATUS
- Set up alerts for failed loads

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
        student_capacity,
        COALESCE(is_active, TRUE) AS is_active
    FROM SCHOOLS_STREAM
    WHERE METADATA$ACTION = 'INSERT'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY school_id ORDER BY created_at DESC) = 1
) src
ON tgt.school_id = src.school_id
WHEN MATCHED THEN UPDATE SET
    municipality_code = src.municipality_code,
    school_name = src.school_name,
    school_type = src.school_type,
    student_capacity = src.student_capacity,
    is_active = src.is_active,
    updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    school_id, municipality_code, school_name, school_type, student_capacity, is_active
) VALUES (
    src.school_id, src.municipality_code, src.school_name, src.school_type, src.student_capacity, src.is_active
);

-- Enable the task
ALTER TASK PROCESS_SCHOOLS_CDC RESUME;
```

### Lab 2.4: Test CDC Pipeline

```sql
-- Insert new record
INSERT INTO KMD_STAGING.RAW.SCHOOLS_RAW 
(school_id, municipality_code, school_name, school_type, student_capacity, is_active)
VALUES ('851-SCH-001', '851', 'Aalborg Skole', 'Folkeskole', 280, TRUE);

-- Check stream has data
SELECT SYSTEM$STREAM_HAS_DATA('SCHOOLS_STREAM');

-- Manually execute task (for testing)
EXECUTE TASK PROCESS_SCHOOLS_CDC;

-- Verify data in staging
SELECT * FROM KMD_STAGING.STAGING.DIM_SCHOOLS;
```

---

## Module 3: Dynamic Data Masking

### Learning Objectives
- Understand masking policy concepts
- Create role-based masking policies
- Apply masking to PII columns

### Lab 3.1: Create Masking Policy

```sql
USE DATABASE KMD_SCHOOLS;
USE SCHEMA SHARED;

-- Create CPR masking policy (Danish personal ID)
CREATE OR REPLACE MASKING POLICY CPR_MASK AS (val STRING) 
RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN', 'KMD_PII_ADMIN') 
            THEN val
        WHEN val IS NULL 
            THEN NULL
        ELSE 
            SUBSTRING(val, 1, 7) || 'XXXX'
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

### Lab 3.2: Create Demo Table and Apply Masking

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
    (2, 'Emma Nielsen', '150285-5678', 'emma.nielsen@school.dk');

-- Apply masking policies
ALTER TABLE MASKING_DEMO 
    MODIFY COLUMN cpr_number SET MASKING POLICY CPR_MASK;

ALTER TABLE MASKING_DEMO 
    MODIFY COLUMN email SET MASKING POLICY EMAIL_MASK;

-- Test as SYSADMIN (full access)
USE ROLE SYSADMIN;
SELECT * FROM MASKING_DEMO;
-- Expected: Full CPR and email visible

-- Show masking policies
SHOW MASKING POLICIES;
```

---

## Module 4: Dynamic Tables & dbt

### Learning Objectives
- Create dynamic tables with automatic refresh
- Understand target lag configuration
- Run dbt models on Snowflake

### Lab 4.1: Create Dynamic Table

```sql
USE DATABASE KMD_ANALYTICS;
CREATE SCHEMA IF NOT EXISTS GOLD;
USE SCHEMA GOLD;

CREATE OR REPLACE DYNAMIC TABLE DIM_SCHOOLS
    TARGET_LAG = '15 minutes'
    WAREHOUSE = KMD_WH
AS
SELECT 
    s.school_id,
    s.municipality_code,
    s.school_name,
    s.school_type,
    s.student_capacity,
    s.is_active,
    CURRENT_TIMESTAMP() AS refreshed_at
FROM KMD_STAGING.STAGING.DIM_SCHOOLS s;

-- Check dynamic table status
SHOW DYNAMIC TABLES;

-- Monitor refresh history
SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    NAME => 'DIM_SCHOOLS'
));
```

### Lab 4.2: dbt Project Setup (Optional)

```bash
# Initialize dbt project
cd 03_transformation/dbt_project

# Install dependencies
dbt deps

# Test connection
dbt debug

# Run models
dbt run --select staging

# Run tests
dbt test
```

---

## Module 5: Semantic Views & Cortex Analyst

### Learning Objectives
- Create semantic views for natural language queries
- Test Cortex Analyst functionality
- Build verified queries

### Lab 5.1: Create Analytics Views

```sql
USE DATABASE KMD_ANALYTICS;
USE SCHEMA GOLD;

-- Create view for Cortex Analyst
CREATE OR REPLACE VIEW V_SCHOOL_ANALYTICS AS
SELECT 
    s.school_id,
    s.school_name,
    s.school_type,
    s.municipality_code,
    s.student_capacity,
    s.is_active
FROM DIM_SCHOOLS s
WHERE s.is_active = TRUE;

-- Query the view
SELECT * FROM V_SCHOOL_ANALYTICS;
```

### Lab 5.2: Test Natural Language Queries

In Snowsight, use Cortex Analyst:
1. Open "AI Features" > "Cortex Analyst"
2. Select the semantic model
3. Try questions like:
   - "How many schools are in Copenhagen?"
   - "What is the average student capacity?"
   - "List all Folkeskole schools"

---

## Module 6: Streamlit Dashboard

### Learning Objectives
- Deploy Streamlit app to Snowflake
- Connect to analytics data
- Create interactive visualizations

### Lab 6.1: Deploy Streamlit App

```sql
USE DATABASE KMD_ANALYTICS;
CREATE SCHEMA IF NOT EXISTS STREAMLIT;
USE SCHEMA STREAMLIT;

-- Create stage for Streamlit files
CREATE OR REPLACE STAGE STREAMLIT_STAGE;

-- Upload app file
-- PUT file:///path/to/school_dashboard.py @STREAMLIT_STAGE;

-- Create Streamlit app
CREATE OR REPLACE STREAMLIT KMD_DASHBOARD
    ROOT_LOCATION = '@KMD_ANALYTICS.STREAMLIT.STREAMLIT_STAGE'
    MAIN_FILE = 'school_dashboard.py'
    QUERY_WAREHOUSE = KMD_WH;
```

### Lab 6.2: Access Dashboard

1. Go to Snowsight > Streamlit
2. Find "KMD_DASHBOARD"
3. Click to open the interactive dashboard

---

## Cleanup (Optional)

```sql
-- Remove all workshop resources
DROP DATABASE IF EXISTS KMD_SCHOOLS;
DROP DATABASE IF EXISTS KMD_STAGING;
DROP DATABASE IF EXISTS KMD_ANALYTICS;
DROP WAREHOUSE IF EXISTS KMD_WH;
```

---

## Key Takeaways

1. **Multi-tenant Architecture**: Schema-per-tenant provides strong isolation
2. **Snowpipe**: Automated, serverless data ingestion from cloud storage
3. **Streams & Tasks**: Native CDC for incremental processing
4. **Dynamic Masking**: Role-based PII protection without code changes
5. **Dynamic Tables**: Declarative materialized views with auto-refresh
6. **Cortex Analyst**: Natural language queries on structured data
7. **Streamlit**: Native dashboards within Snowflake

---

## Resources

- [Snowflake Documentation](https://docs.snowflake.com)
- [dbt-snowflake](https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup)
- [Cortex Analyst](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst)
- [Streamlit in Snowflake](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)
