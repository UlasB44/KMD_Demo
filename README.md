# KMD Denmark - Snowflake Onboarding Workshop

## Multi-tenant Snowflake Solution for Danish Municipalities

This repository contains all materials for the KMD Snowflake Onboarding Workshop demonstrating a multi-tenant architecture for Danish municipalities with a focus on Schools/Education data.

## Workshop Overview

| Topic | Duration | Description |
|-------|----------|-------------|
| Data Integration | 2.5 hours | External stages (S3), Snowpipe, Streams & Tasks, Schema Evolution |
| Transformation | 2 hours | Data masking, Dynamic Tables, dbt |
| AI & Reporting | 2 hours | Semantic Views, Cortex Analyst, Streamlit |
| Hands-on Labs | 1.5 hours | Guided exercises |

**Total: ~8 hours (full day workshop)**

## Architecture

```
                    +------------------+
                    |   AWS S3 Bucket  |
                    | s3://ubulut-iceberg-oregon/kmd/
                    +--------+---------+
                             |
                    +--------v---------+
                    | Storage Integration
                    |    (TRACKMAN)    |
                    +--------+---------+
                             |
              +--------------+--------------+
              |              |              |
    +---------v----+ +-------v------+ +----v---------+
    | COPENHAGEN   | | AARHUS       | | COMBINED     |
    | _STAGE       | | _STAGE       | | _STAGE       |
    +--------------+ +--------------+ +--------------+
              |              |              |
              +--------------+--------------+
                             |
                    +--------v---------+
                    |   RAW Schema     |
                    |   (Bronze)       |
                    +--------+---------+
                             |
                    +--------v---------+
                    |  Streams/Tasks   |
                    |     (CDC)        |
                    +--------+---------+
                             |
                    +--------v---------+
                    |  STAGING Schema  |
                    |   (Silver)       |
                    +--------+---------+
                             |
                    +--------v---------+
                    |  Dynamic Tables  |
                    |  + dbt Models    |
                    +--------+---------+
                             |
                    +--------v---------+
                    |  ANALYTICS Schema|
                    |    (Gold)        |
                    +--------+---------+
                             |
         +-------------------+-------------------+
         |                   |                   |
+--------v------+   +--------v--------+  +------v-------+
| Semantic View |   | Cortex Analyst  |  |  Streamlit   |
|               |   | (Text-to-SQL)   |  |  Dashboard   |
+---------------+   +-----------------+  +--------------+
```

## S3 Data Structure

```
s3://ubulut-iceberg-oregon/kmd/
├── combined/                 # All municipalities combined (35 schools, 993 teachers, 14,614 students)
│   ├── dim_schools_all.csv
│   ├── dim_teachers_all.csv
│   ├── dim_students_all.csv
│   ├── dim_classes_all.csv
│   ├── fact_budgets_all.csv
│   ├── fact_wellness_all.csv
│   └── fact_attendance_sample.csv
├── copenhagen/               # Municipality-specific data
├── aarhus/
├── odense/
├── aalborg/
└── esbjerg/
```

## Snowflake Objects

### External Stages (7 total)
| Stage | S3 Path |
|-------|---------|
| KMD_S3_STAGE | s3://ubulut-iceberg-oregon/kmd/ (root) |
| COPENHAGEN_STAGE | s3://ubulut-iceberg-oregon/kmd/copenhagen/ |
| AARHUS_STAGE | s3://ubulut-iceberg-oregon/kmd/aarhus/ |
| ODENSE_STAGE | s3://ubulut-iceberg-oregon/kmd/odense/ |
| AALBORG_STAGE | s3://ubulut-iceberg-oregon/kmd/aalborg/ |
| ESBJERG_STAGE | s3://ubulut-iceberg-oregon/kmd/esbjerg/ |
| COMBINED_STAGE | s3://ubulut-iceberg-oregon/kmd/combined/ |

### Databases
- **KMD_SCHOOLS**: Multi-tenant school data (schema-per-tenant)
- **KMD_STAGING**: Raw data landing zone and transformations
- **KMD_ANALYTICS**: Gold layer analytics

## Multi-Tenant Architecture (Schema-per-Tenant)

```
KMD_SCHOOLS (Database)
├── COPENHAGEN (Schema)     - Municipality 101
├── AARHUS (Schema)         - Municipality 751
├── ODENSE (Schema)         - Municipality 461
├── AALBORG (Schema)        - Municipality 851
├── ESBJERG (Schema)        - Municipality 561
└── SHARED (Schema)         - Reference data & masking policies
```

## Repository Structure

```
KMD_Demo/
├── 01_data_foundation/        # Synthetic data generation
│   ├── data/                  # Generated CSV files (by municipality)
│   │   ├── combined/          # All data merged
│   │   ├── copenhagen/
│   │   ├── aarhus/
│   │   └── ...
│   └── scripts/               # Data generation scripts
│
├── 02_data_integration/       # Ingestion patterns
│   ├── external_stages/       # External stage definitions (S3)
│   ├── snowpipe/              # Auto-ingest setup
│   ├── streams_tasks/         # CDC implementation
│   └── schema_evolution/      # Schema evolution examples
│
├── 03_transformation/         # Data transformation
│   ├── masking_policies/      # Dynamic data masking (CPR, email)
│   ├── dynamic_tables/        # Dynamic table definitions
│   └── dbt_project/           # Complete dbt project
│       ├── models/
│       │   ├── staging/       # stg_schools, stg_teachers, etc.
│       │   ├── intermediate/  # int_* models
│       │   └── marts/         # dim_*, fct_* tables
│       └── seeds/             # Reference data
│
├── 04_ai_reporting/           # AI & Analytics
│   ├── semantic_views/        # Cortex Analyst models
│   └── streamlit_apps/        # Dashboard apps
│
└── 05_training_materials/     # Workshop content
    └── hol_guides/            # Hands-on lab guides
```

## Quick Start

### 1. Setup Database Structure
```sql
-- Run the setup script
USE ROLE SYSADMIN;
!source 01_data_foundation/scripts/00_setup_databases.sql
```

### 2. Create External Stages (uses existing TRACKMAN integration)
```sql
!source 02_data_integration/external_stages/create_stages.sql
```

### 3. Load Data from S3
```sql
-- Load from external stage
COPY INTO KMD_STAGING.RAW.SCHOOLS_RAW
FROM @KMD_STAGING.EXTERNAL_STAGES.COMBINED_STAGE/dim_schools_all.csv
FILE_FORMAT = CSV_FORMAT;
```

### 4. Run the Workshop Labs
Follow the guides in `05_training_materials/hol_guides/Workshop_Lab_Guide.md`

## Data Summary

| Entity | Count |
|--------|-------|
| Schools | 35 |
| Teachers | 993 |
| Students | 14,614 |
| Classes | 699 |
| Municipalities | 5 |

## Prerequisites

- Snowflake Account with SYSADMIN role
- Storage Integration `TRACKMAN` (already configured)
- dbt-snowflake (for transformation demos)

## Key Features Demonstrated

- **External Stages**: S3 integration with storage integration
- **Schema Evolution**: Auto-add columns, handle missing columns
- **Dynamic Data Masking**: CPR numbers, emails (role-based)
- **Streams & Tasks**: CDC for incremental processing
- **Dynamic Tables**: Auto-refresh materialized views
- **dbt**: Full project with staging → intermediate → marts
- **Cortex Analyst**: Natural language to SQL

## License

Internal use only - KMD Denmark Workshop
