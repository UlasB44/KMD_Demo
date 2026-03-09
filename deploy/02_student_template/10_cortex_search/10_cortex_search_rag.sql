-- ============================================================================
-- STUDENT EXERCISE - STEP 10: CORTEX SEARCH - PDF DOCUMENT RAG
-- ============================================================================
-- Replace {MUNICIPALITY} with your assigned municipality (e.g., ESBJERG)
-- 
-- Municipality Reference:
--   COPENHAGEN  -> Code: 101
--   AARHUS      -> Code: 751
--   ODENSE      -> Code: 461
--   AALBORG     -> Code: 851
--   ESBJERG     -> Code: 561
-- ============================================================================
-- This exercise demonstrates how to:
-- 1. Create an internal stage for PDF storage
-- 2. Parse PDFs using Snowflake's built-in functions
-- 3. Chunk text for RAG (Retrieval Augmented Generation)
-- 4. Store chunks in a searchable table
-- 5. (Optional) Create a Cortex Search Service via Snowsight
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE {MUNICIPALITY}_DB;
USE WAREHOUSE KMD_WH;

-- ============================================================================
-- STEP 1: CREATE SCHEMA FOR DOCUMENT STORAGE
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS DOCUMENTS;
USE SCHEMA DOCUMENTS;

-- ============================================================================
-- STEP 2: CREATE INTERNAL STAGE FOR PDF FILES
-- ============================================================================
-- Internal stages store files within Snowflake
-- Files can be uploaded via Snowsight UI or SnowSQL

CREATE OR REPLACE STAGE PDF_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for municipality education policy PDF documents';

-- ============================================================================
-- STEP 3: UPLOAD YOUR PDFs
-- ============================================================================
-- Option A: Via Snowsight UI
--   1. Navigate to Data > Databases > {MUNICIPALITY}_DB > DOCUMENTS > Stages
--   2. Click on PDF_STAGE
--   3. Click "+ Files" button
--   4. Upload your municipality's PDF files from the pdfs/{municipality}/ folder
--
-- Option B: Via SnowSQL CLI
--   PUT file:///path/to/pdfs/{municipality}/*.pdf @PDF_STAGE AUTO_COMPRESS=FALSE;

-- Refresh directory metadata (required after upload):
ALTER STAGE PDF_STAGE REFRESH;

-- List uploaded files (run after uploading):
LIST @PDF_STAGE;

-- ============================================================================
-- STEP 4: CREATE TABLE FOR RAW PDF CONTENT
-- ============================================================================
-- Note: DIRECTORY() returns TIMESTAMP_TZ for LAST_MODIFIED

CREATE OR REPLACE TABLE PDF_RAW (
    file_name VARCHAR,
    file_path VARCHAR,
    file_size NUMBER,
    last_modified TIMESTAMP_TZ,
    raw_content VARIANT,
    municipality_code NUMBER DEFAULT {MUNICIPALITY_CODE}
);

-- ============================================================================
-- STEP 5: PARSE PDFs AND EXTRACT TEXT
-- ============================================================================
-- Snowflake's PARSE_DOCUMENT function extracts text from PDFs

INSERT INTO PDF_RAW (file_name, file_path, file_size, last_modified, raw_content)
SELECT 
    REGEXP_REPLACE(RELATIVE_PATH, '.*/', '') AS file_name,
    RELATIVE_PATH AS file_path,
    SIZE AS file_size,
    LAST_MODIFIED,
    SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
        @PDF_STAGE, 
        RELATIVE_PATH,
        {'mode': 'LAYOUT'}
    ) AS raw_content
FROM DIRECTORY(@PDF_STAGE)
WHERE RELATIVE_PATH LIKE '%.pdf';

-- Verify extracted content:
SELECT 
    file_name,
    file_size,
    SUBSTRING(raw_content:content::VARCHAR, 1, 500) AS extracted_text_preview
FROM PDF_RAW
LIMIT 5;

-- ============================================================================
-- STEP 6: CREATE CHUNKED DOCUMENTS TABLE FOR RAG
-- ============================================================================
-- For effective retrieval, we split documents into smaller chunks
-- Typical chunk size: 500-1000 characters with overlap

CREATE OR REPLACE TABLE PDF_CHUNKS (
    chunk_id NUMBER AUTOINCREMENT,
    document_id NUMBER,
    file_name VARCHAR,
    chunk_index NUMBER,
    chunk_text VARCHAR(16777216),
    chunk_size NUMBER,
    municipality_code NUMBER,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- STEP 7: CHUNK THE DOCUMENTS USING PURE SQL
-- ============================================================================
-- This uses a recursive CTE to split documents into overlapping chunks
-- CHUNK_SIZE = 800 characters, OVERLAP = 200 characters (step = 600)

INSERT INTO PDF_CHUNKS (document_id, file_name, chunk_index, chunk_text, chunk_size, municipality_code)
WITH 
-- Step 1: Number the documents and extract text
docs AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY file_name) AS doc_id,
        file_name,
        raw_content:content::VARCHAR AS full_text,
        municipality_code
    FROM PDF_RAW
    WHERE raw_content:content IS NOT NULL
),
-- Step 2: Generate chunk indices using a number generator
-- Max chunks per doc = ceil(text_length / step_size) 
numbers AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS n
    FROM TABLE(GENERATOR(ROWCOUNT => 100))  -- Support up to 100 chunks per document
),
-- Step 3: Create chunks with 800 char size, 600 char step (200 overlap)
chunked AS (
    SELECT 
        d.doc_id,
        d.file_name,
        n.n AS chunk_index,
        SUBSTRING(d.full_text, n.n * 600 + 1, 800) AS chunk_text,
        d.municipality_code
    FROM docs d
    CROSS JOIN numbers n
    WHERE n.n * 600 < LENGTH(d.full_text)
)
SELECT 
    doc_id,
    file_name,
    chunk_index,
    chunk_text,
    LENGTH(chunk_text) AS chunk_size,
    municipality_code
FROM chunked
WHERE LENGTH(chunk_text) > 0
ORDER BY doc_id, chunk_index;

-- Verify chunks:
SELECT 
    file_name,
    chunk_index,
    chunk_size,
    SUBSTRING(chunk_text, 1, 100) || '...' AS preview
FROM PDF_CHUNKS
ORDER BY file_name, chunk_index
LIMIT 10;

-- Summary statistics:
SELECT 
    file_name,
    COUNT(*) AS num_chunks,
    SUM(chunk_size) AS total_chars,
    ROUND(AVG(chunk_size), 0) AS avg_chunk_size
FROM PDF_CHUNKS
GROUP BY file_name
ORDER BY file_name;

-- ============================================================================
-- ALTERNATIVE: SQL CHUNKING EXAMPLE (COMMENTED OUT)
-- ============================================================================
-- This is a standalone example showing how chunking works with sample text.
-- Useful for understanding the recursive CTE approach.
/*
-- Pure SQL Text Chunking Example
-- Parameters: CHUNK_SIZE=50, STEP_SIZE=40 (overlap=10)
WITH sample_docs AS (
    SELECT 1 AS doc_id, 'doc1.pdf' AS file_name, 
           'This is a sample document text that we want to chunk into smaller pieces. Each chunk should have some overlap with the next one to preserve context.' AS full_text
    UNION ALL
    SELECT 2, 'doc2.pdf', 
           'Another example document showing how the chunking process works across multiple documents in a single query.'
),
numbers AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS n
    FROM TABLE(GENERATOR(ROWCOUNT => 20))
),
chunked AS (
    SELECT 
        d.doc_id,
        d.file_name,
        n.n AS chunk_index,
        SUBSTRING(d.full_text, n.n * 40 + 1, 50) AS chunk_text
    FROM sample_docs d
    CROSS JOIN numbers n
    WHERE n.n * 40 < LENGTH(d.full_text)
)
SELECT 
    doc_id,
    file_name,
    chunk_index,
    chunk_text,
    LENGTH(chunk_text) AS chunk_size
FROM chunked
WHERE LENGTH(chunk_text) > 0
ORDER BY doc_id, chunk_index;
*/

-- ============================================================================
-- STEP 8: CREATE FINAL SEARCH-READY TABLE
-- ============================================================================
-- This table is optimized for Cortex Search

CREATE OR REPLACE TABLE EDUCATION_DOCS_SEARCH (
    chunk_id NUMBER,
    document_name VARCHAR,
    document_type VARCHAR,
    chunk_text VARCHAR(16777216),
    municipality_code NUMBER,
    municipality_name VARCHAR,
    created_at TIMESTAMP_NTZ
);

INSERT INTO EDUCATION_DOCS_SEARCH
SELECT 
    chunk_id,
    REPLACE(REPLACE(file_name, '.pdf', ''), '_', ' ') AS document_name,
    CASE 
        WHEN file_name ILIKE '%safety%' OR file_name ILIKE '%emergency%' THEN 'Safety & Emergency'
        WHEN file_name ILIKE '%special_needs%' OR file_name ILIKE '%inclusion%' THEN 'Special Needs & Inclusion'
        WHEN file_name ILIKE '%teacher%' OR file_name ILIKE '%professional%' THEN 'Teacher Resources'
        WHEN file_name ILIKE '%parent%' OR file_name ILIKE '%family%' THEN 'Family Engagement'
        WHEN file_name ILIKE '%mental%' OR file_name ILIKE '%wellness%' THEN 'Student Wellness'
        WHEN file_name ILIKE '%stem%' OR file_name ILIKE '%digital%' THEN 'STEM & Technology'
        WHEN file_name ILIKE '%arts%' OR file_name ILIKE '%culture%' THEN 'Arts & Culture'
        WHEN file_name ILIKE '%environment%' OR file_name ILIKE '%green%' THEN 'Environmental Education'
        WHEN file_name ILIKE '%sport%' OR file_name ILIKE '%physical%' THEN 'Physical Education'
        WHEN file_name ILIKE '%maritime%' THEN 'Maritime Education'
        ELSE 'General Policy'
    END AS document_type,
    chunk_text,
    municipality_code,
    '{MUNICIPALITY}' AS municipality_name,
    created_at
FROM PDF_CHUNKS;

-- Verify final table:
SELECT document_type, COUNT(*) AS chunks
FROM EDUCATION_DOCS_SEARCH
GROUP BY document_type
ORDER BY chunks DESC;

-- ============================================================================
-- STEP 9: TEST SEMANTIC SEARCH (WITHOUT CORTEX SEARCH SERVICE)
-- ============================================================================
-- You can use CORTEX.EMBED_TEXT for similarity search without a formal service

-- Create embeddings table for vector search:
CREATE OR REPLACE TABLE EDUCATION_DOCS_EMBEDDINGS AS
SELECT 
    chunk_id,
    document_name,
    document_type,
    chunk_text,
    municipality_name,
    SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', chunk_text) AS embedding
FROM EDUCATION_DOCS_SEARCH;

-- Example: Find documents about ADHD support
SELECT 
    document_name,
    document_type,
    SUBSTRING(chunk_text, 1, 300) AS preview,
    VECTOR_COSINE_SIMILARITY(
        embedding, 
        SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 'How do we support students with ADHD?')
    ) AS similarity
FROM EDUCATION_DOCS_EMBEDDINGS
ORDER BY similarity DESC
LIMIT 5;

-- Example: Find safety procedures
SELECT 
    document_name,
    document_type,
    SUBSTRING(chunk_text, 1, 300) AS preview,
    VECTOR_COSINE_SIMILARITY(
        embedding, 
        SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 'What are the fire evacuation procedures?')
    ) AS similarity
FROM EDUCATION_DOCS_EMBEDDINGS
ORDER BY similarity DESC
LIMIT 5;

-- ============================================================================
-- STEP 10: CREATE CORTEX SEARCH SERVICE (VIA SNOWSIGHT UI)
-- ============================================================================
-- To create a Cortex Search Service:
-- 1. Navigate to AI/ML > Cortex Search in Snowsight
-- 2. Click "Create"
-- 3. Configure:
--    - Name: EDUCATION_SEARCH_SERVICE
--    - Source table: {MUNICIPALITY}_DB.DOCUMENTS.EDUCATION_DOCS_SEARCH
--    - Search column: CHUNK_TEXT
--    - Attribute columns: DOCUMENT_NAME, DOCUMENT_TYPE, MUNICIPALITY_NAME
--    - Warehouse: KMD_WH
-- 4. Click "Create"
--
-- Once created, you can query it with:
/*
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'EDUCATION_SEARCH_SERVICE',
    '{
        "query": "How do we support students with dyslexia?",
        "columns": ["document_name", "document_type", "chunk_text"],
        "limit": 5
    }'
);
*/

-- ============================================================================
-- VERIFY SETUP
-- ============================================================================
SHOW STAGES LIKE 'PDF_STAGE' IN SCHEMA {MUNICIPALITY}_DB.DOCUMENTS;
SHOW TABLES IN SCHEMA {MUNICIPALITY}_DB.DOCUMENTS;

SELECT 'PDF_RAW' AS table_name, COUNT(*) AS row_count FROM PDF_RAW
UNION ALL
SELECT 'PDF_CHUNKS', COUNT(*) FROM PDF_CHUNKS
UNION ALL  
SELECT 'EDUCATION_DOCS_SEARCH', COUNT(*) FROM EDUCATION_DOCS_SEARCH
UNION ALL
SELECT 'EDUCATION_DOCS_EMBEDDINGS', COUNT(*) FROM EDUCATION_DOCS_EMBEDDINGS;
