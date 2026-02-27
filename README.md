# KMD Denmark - Snowflake Onboarding Workshop

## Multi-tenant Snowflake Solution for Danish Municipalities

This repository contains all materials for the KMD Snowflake Onboarding Workshop demonstrating a multi-tenant architecture for Danish municipalities with a focus on Schools/Education data.

## Workshop Overview

| Topic | Duration | Description |
|-------|----------|-------------|
| Data Integration | 2 hours | External stages, Snowpipe, Streams & Tasks |
| Transformation | 2 hours | Data masking, Dynamic Tables, dbt |
| AI & Reporting | 2.5 hours | Semantic Views, Cortex Analyst, Streamlit |
| Hands-on Labs | 1.5 hours | Guided exercises |

## Architecture

```
                    +------------------+
                    |   AWS S3 Bucket  |
                    |  (CSV/Parquet)   |
                    +--------+---------+
                             |
                    +--------v---------+
                    |  External Stage  |
                    +--------+---------+
                             |
              +--------------+--------------+
              |                             |
    +---------v----------+       +----------v---------+
    |     Snowpipe       |       |   Manual COPY INTO |
    | (Auto-ingest)      |       |   (Batch loads)    |
    +---------+----------+       +----------+---------+
              |                             |
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

## Multi-Tenant Architecture (Schema-per-Tenant)

```
KMD_SCHOOLS (Database)
├── COPENHAGEN (Schema)     - Municipality 1
├── AARHUS (Schema)         - Municipality 2
├── ODENSE (Schema)         - Municipality 3
├── AALBORG (Schema)        - Municipality 4
├── ESBJERG (Schema)        - Municipality 5
├── SHARED (Schema)         - Shared reference data
└── ANALYTICS (Schema)      - Cross-tenant analytics (with RLS)
```

## Data Model - Schools Domain

### Core Entities
- **DIM_SCHOOLS**: School master data
- **DIM_TEACHERS**: Teacher information
- **DIM_STUDENTS**: Student data (CPR masked)
- **DIM_CLASSES**: Class definitions

### Academic Data
- **FACT_GRADES**: Student grades
- **FACT_ATTENDANCE**: Attendance records
- **FACT_TEST_SCORES**: Standardized test results

### Financial Data
- **FACT_BUDGETS**: School budgets
- **FACT_EXPENDITURES**: Spending records

### Student Welfare
- **FACT_WELLNESS**: Student wellness indicators
- **DIM_SPECIAL_NEEDS**: Special education categories

## Repository Structure

```
KMD_Demo/
├── 01_data_foundation/        # Synthetic data generation
│   ├── data/                  # Generated CSV files
│   └── scripts/               # Data generation scripts
│
├── 02_data_integration/       # Ingestion patterns
│   ├── external_stages/       # Stage definitions
│   ├── snowpipe/              # Auto-ingest setup
│   └── streams_tasks/         # CDC implementation
│
├── 03_transformation/         # Data transformation
│   ├── masking_policies/      # Dynamic data masking
│   ├── dynamic_tables/        # Dynamic table definitions
│   └── dbt_project/           # dbt models
│
├── 04_ai_reporting/           # AI & Analytics
│   ├── semantic_views/        # Cortex Analyst models
│   ├── cortex_agents/         # Agent definitions
│   └── streamlit_apps/        # Dashboard apps
│
└── 05_training_materials/     # Workshop content
    ├── slides/                # Presentation deck
    ├── hol_guides/            # Hands-on lab guides
    └── architecture_diagrams/ # Visual documentation
```

## Quick Start

### 1. Setup Database Structure
```sql
-- Run the setup script
!source 01_data_foundation/scripts/00_setup_databases.sql
```

### 2. Generate Sample Data
```bash
cd 01_data_foundation/scripts
python generate_school_data.py
```

### 3. Upload to S3 and Create Stages
```sql
!source 02_data_integration/external_stages/create_stages.sql
```

### 4. Run the Workshop Labs
Follow the guides in `05_training_materials/hol_guides/`

## Prerequisites

- Snowflake Account with ACCOUNTADMIN or SYSADMIN role
- AWS S3 bucket access (for external stages)
- Python 3.9+ (for data generation)
- dbt-snowflake (for transformation demos)

## Security Features Demonstrated

- Dynamic Data Masking (CPR numbers)
- Row-Level Security (tenant isolation)
- Role-Based Access Control (RBAC)
- Column-Level Security
- Audit logging

## License

Internal use only - KMD Denmark Workshop
