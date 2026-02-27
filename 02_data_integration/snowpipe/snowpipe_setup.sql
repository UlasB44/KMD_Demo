/*
=============================================================================
KMD Denmark - Snowpipe Auto-Ingest Setup
Automated data ingestion from S3 using Snowpipe
=============================================================================
BEST PRACTICES:
1. Use one pipe per table for easier management
2. Set appropriate error handling (ON_ERROR)
3. Monitor pipe status with PIPE_STATUS() and COPY_HISTORY()
4. Use notifications for alerting on failures
5. Implement retry logic for failed files
=============================================================================
*/

USE ROLE SYSADMIN;
USE DATABASE KMD_STAGING;
USE SCHEMA RAW;
USE WAREHOUSE KMD_WH;

-- ============================================================================
-- SECTION 1: Create Raw Landing Tables
-- ============================================================================

-- Raw schools table (landing zone)
CREATE OR REPLACE TABLE SCHOOLS_RAW (
    school_id VARCHAR(50),
    municipality_code VARCHAR(10),
    school_name VARCHAR(200),
    school_type VARCHAR(50),
    address VARCHAR(500),
    postal_code VARCHAR(10),
    city VARCHAR(100),
    phone VARCHAR(50),
    email VARCHAR(200),
    founded_year INT,
    student_capacity INT,
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    -- Metadata columns for tracking
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file VARCHAR(500),
    _row_number INT
);

-- Raw teachers table
CREATE OR REPLACE TABLE TEACHERS_RAW (
    teacher_id VARCHAR(50),
    school_id VARCHAR(50),
    municipality_code VARCHAR(10),
    cpr_number VARCHAR(20),
    cpr_masked VARCHAR(20),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    gender VARCHAR(1),
    birth_date DATE,
    email VARCHAR(200),
    phone VARCHAR(50),
    hire_date DATE,
    subjects VARCHAR(500),
    salary_band VARCHAR(10),
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file VARCHAR(500),
    _row_number INT
);

-- Raw students table
CREATE OR REPLACE TABLE STUDENTS_RAW (
    student_id VARCHAR(50),
    class_id VARCHAR(50),
    school_id VARCHAR(50),
    municipality_code VARCHAR(10),
    cpr_number VARCHAR(20),
    cpr_masked VARCHAR(20),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    gender VARCHAR(1),
    birth_date DATE,
    enrollment_date DATE,
    guardian_name VARCHAR(200),
    guardian_phone VARCHAR(50),
    guardian_email VARCHAR(200),
    address VARCHAR(500),
    postal_code VARCHAR(10),
    special_needs VARCHAR(100),
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file VARCHAR(500),
    _row_number INT
);

-- Raw classes table
CREATE OR REPLACE TABLE CLASSES_RAW (
    class_id VARCHAR(50),
    school_id VARCHAR(50),
    municipality_code VARCHAR(10),
    grade VARCHAR(10),
    section VARCHAR(5),
    class_name VARCHAR(50),
    academic_year VARCHAR(20),
    max_students INT,
    classroom_number VARCHAR(20),
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file VARCHAR(500),
    _row_number INT
);

-- Raw grades (fact) table
CREATE OR REPLACE TABLE GRADES_RAW (
    grade_record_id VARCHAR(50),
    student_id VARCHAR(50),
    class_id VARCHAR(50),
    school_id VARCHAR(50),
    municipality_code VARCHAR(10),
    subject VARCHAR(50),
    academic_year VARCHAR(20),
    term VARCHAR(10),
    grade_value VARCHAR(10),
    grade_date DATE,
    teacher_comment VARCHAR(500),
    is_final BOOLEAN,
    created_at TIMESTAMP_NTZ,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file VARCHAR(500),
    _row_number INT
);

-- Raw budgets (fact) table
CREATE OR REPLACE TABLE BUDGETS_RAW (
    budget_id VARCHAR(50),
    school_id VARCHAR(50),
    municipality_code VARCHAR(10),
    fiscal_year INT,
    category VARCHAR(100),
    budgeted_amount DECIMAL(15,2),
    spent_amount DECIMAL(15,2),
    currency VARCHAR(10),
    approved_date DATE,
    approved_by VARCHAR(200),
    notes VARCHAR(1000),
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file VARCHAR(500),
    _row_number INT
);

-- ============================================================================
-- SECTION 2: Create Snowpipes
-- ============================================================================

USE SCHEMA KMD_STAGING.EXTERNAL_STAGES;

-- Snowpipe for schools data
CREATE OR REPLACE PIPE KMD_SCHOOLS_PIPE
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest pipe for school master data'
AS
COPY INTO KMD_STAGING.RAW.SCHOOLS_RAW (
    school_id, municipality_code, school_name, school_type, address,
    postal_code, city, phone, email, founded_year, student_capacity,
    is_active, created_at, updated_at, _source_file, _row_number
)
FROM (
    SELECT 
        $1, $2, $3, $4, $5,
        $6, $7, $8, $9, $10, $11,
        $12, $13, $14,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @KMD_S3_STAGE/schools/
)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE';

-- Snowpipe for teachers data
CREATE OR REPLACE PIPE KMD_TEACHERS_PIPE
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest pipe for teacher data'
AS
COPY INTO KMD_STAGING.RAW.TEACHERS_RAW (
    teacher_id, school_id, municipality_code, cpr_number, cpr_masked,
    first_name, last_name, gender, birth_date, email, phone,
    hire_date, subjects, salary_band, is_active, created_at, updated_at,
    _source_file, _row_number
)
FROM (
    SELECT 
        $1, $2, $3, $4, $5,
        $6, $7, $8, $9, $10, $11,
        $12, $13, $14, $15, $16, $17,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @KMD_S3_STAGE/teachers/
)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE';

-- Snowpipe for students data
CREATE OR REPLACE PIPE KMD_STUDENTS_PIPE
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest pipe for student data'
AS
COPY INTO KMD_STAGING.RAW.STUDENTS_RAW (
    student_id, class_id, school_id, municipality_code, cpr_number, cpr_masked,
    first_name, last_name, gender, birth_date, enrollment_date,
    guardian_name, guardian_phone, guardian_email, address, postal_code,
    special_needs, is_active, created_at, updated_at,
    _source_file, _row_number
)
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6,
        $7, $8, $9, $10, $11,
        $12, $13, $14, $15, $16,
        $17, $18, $19, $20,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @KMD_S3_STAGE/students/
)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE';

-- Snowpipe for grades data
CREATE OR REPLACE PIPE KMD_GRADES_PIPE
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest pipe for grade records'
AS
COPY INTO KMD_STAGING.RAW.GRADES_RAW (
    grade_record_id, student_id, class_id, school_id, municipality_code,
    subject, academic_year, term, grade_value, grade_date,
    teacher_comment, is_final, created_at,
    _source_file, _row_number
)
FROM (
    SELECT 
        $1, $2, $3, $4, $5,
        $6, $7, $8, $9, $10,
        $11, $12, $13,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @KMD_S3_STAGE/grades/
)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE';

-- ============================================================================
-- SECTION 3: Get SQS Notification ARN (for S3 event setup)
-- ============================================================================

-- Get the notification channel for S3 bucket configuration
SHOW PIPES LIKE 'KMD%';

-- The notification_channel column contains the SQS queue ARN
-- You need to configure your S3 bucket to send events to this queue

/*
AWS S3 Event Configuration Steps:
1. Go to S3 bucket properties
2. Create event notification
3. Event types: s3:ObjectCreated:*
4. Destination: SQS Queue
5. Enter the SQS ARN from notification_channel

Or via AWS CLI:
aws s3api put-bucket-notification-configuration \
    --bucket ubulut-iceberg-oregon \
    --notification-configuration '{
        "QueueConfigurations": [{
            "QueueArn": "arn:aws:sqs:us-west-2:...:sf-snowpipe-...",
            "Events": ["s3:ObjectCreated:*"],
            "Filter": {
                "Key": {
                    "FilterRules": [{
                        "Name": "prefix",
                        "Value": "kmd/"
                    }]
                }
            }
        }]
    }'
*/

-- ============================================================================
-- SECTION 4: Snowpipe Monitoring Queries
-- ============================================================================

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('KMD_SCHOOLS_PIPE');

-- View copy history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'SCHOOLS_RAW',
    START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
));

-- Check for errors
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'SCHOOLS_RAW',
    START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
))
WHERE STATUS = 'LOAD_FAILED';

-- ============================================================================
-- SECTION 5: Manual Refresh (for testing)
-- ============================================================================

-- Force refresh a pipe (useful for testing)
-- ALTER PIPE KMD_SCHOOLS_PIPE REFRESH;

-- Pause/Resume pipes
-- ALTER PIPE KMD_SCHOOLS_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;
-- ALTER PIPE KMD_SCHOOLS_PIPE SET PIPE_EXECUTION_PAUSED = FALSE;

SELECT 'Snowpipe setup complete!' AS status;
SHOW PIPES LIKE 'KMD%';
