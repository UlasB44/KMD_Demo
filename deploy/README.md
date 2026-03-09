# KMD Snowflake Workshop

Snowflake onboarding workshop for KMD Denmark - building municipality data pipelines.

## Repository Structure

```
deploy/
├── 00_shared/              # Prerequisites (run once by instructor)
│   └── prerequisites.sql   # Storage integration, warehouse, file formats
│
├── 01_reference/           # Instructor's reference implementation
│   ├── 01_databases_schemas.sql
│   ├── 02_external_stages.sql
│   ├── 03_raw_tables_load.sql
│   ├── 04_security.sql
│   ├── 05_tenant_views.sql
│   ├── 06_streams_tasks.sql
│   ├── 07_dynamic_tables.sql
│   ├── 08_semantic_view.sql
│   └── README.md
│
├── 02_student_template/    # Generic template with {PLACEHOLDERS}
│   ├── 01_database.sql
│   ├── 02_raw_tables.sql
│   ├── 03_snowpipe.sql
│   ├── 04_streams_tasks.sql
│   ├── 05_security.sql
│   └── README.md
│
└── exercises/              # Pre-filled for each student
    ├── copenhagen/         # Student 1 (code: 101)
    ├── aarhus/             # Student 2 (code: 751)
    ├── odense/             # Student 3 (code: 461)
    ├── aalborg/            # Student 4 (code: 851)
    └── esbjerg/            # Spare (code: 561)

data/
└── generate_incremental.py # Script to generate test data files
```

## Workshop Flow

### Part 1: Instructor Demo (01_reference)
- Show complete pipeline with combined data
- Demonstrate RLS, masking, streams, tasks
- Explain architecture decisions

### Part 2: Student Exercise (exercises/)
Each student builds their own isolated pipeline:

| Student | Municipality | Database | S3 Folder |
|---------|--------------|----------|-----------|
| 1 | Copenhagen | COPENHAGEN_DB | `data/copenhagen/` |
| 2 | Aarhus | AARHUS_DB | `data/aarhus/` |
| 3 | Odense | ODENSE_DB | `data/odense/` |
| 4 | Aalborg | AALBORG_DB | `data/aalborg/` |

## Data Architecture

```
S3 Bucket: ubulut-iceberg-oregon
├── data/
│   ├── combined/           # Reference implementation data
│   │   ├── dim_schools_all.csv
│   │   ├── dim_teachers_all.csv
│   │   ├── dim_students_all.csv
│   │   └── dim_classes_all.csv
│   │
│   ├── copenhagen/         # Student 1 data
│   │   ├── dim_students_20260310.csv  (Day 1 - baseline)
│   │   ├── dim_students_20260311.csv  (Day 2 - +10 new)
│   │   └── dim_students_20260312.csv  (Day 3 - +10 more)
│   │
│   ├── aarhus/            # Student 2 data
│   ├── odense/            # Student 3 data
│   ├── aalborg/           # Student 4 data
│   └── esbjerg/           # Spare
```

## Features Covered

| Feature | Reference | Student Exercise |
|---------|-----------|------------------|
| Storage Integration | ✅ | Uses shared |
| External Stages | ✅ | ✅ |
| Snowpipe (AUTO_INGEST) | ✅ | ✅ |
| Streams (CDC) | ✅ | ✅ |
| Tasks (MERGE) | ✅ | ✅ |
| Row-Level Security | ✅ | Optional |
| Dynamic Masking | ✅ | ✅ |
| Schema-per-Tenant | ✅ | N/A (own DB) |
| Dynamic Tables | ✅ | Future |
| Semantic Views | ✅ | Future |

## Quick Start

```bash
# 1. Prerequisites (instructor only)
snowsql -f deploy/00_shared/prerequisites.sql

# 2. Reference implementation
cd deploy/01_reference
snowsql -f 01_databases_schemas.sql
# ... run all files in order

# 3. Student exercise (pick your municipality folder)
cd deploy/exercises/copenhagen
snowsql -f 01_database.sql
snowsql -f 02_raw_tables.sql
snowsql -f 03_snowpipe.sql
snowsql -f 04_streams_tasks.sql
snowsql -f 05_security.sql
```

## Snowflake Account

- Account: `BGYLLYD-AJ65411`
- Region: `us-west-2`
- Warehouse: `KMD_WH`
