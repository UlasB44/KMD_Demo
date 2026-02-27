# KMD Schools - dbt Project

## Overview

This dbt project transforms raw school data into analytics-ready dimensions and facts following the Bronze → Silver → Gold pattern.

## Project Structure

```
dbt_project/
├── models/
│   ├── staging/           # Bronze → Silver (views)
│   │   ├── stg_schools.sql
│   │   ├── stg_teachers.sql
│   │   ├── stg_students.sql
│   │   ├── stg_classes.sql
│   │   ├── stg_grades.sql
│   │   ├── sources.yml
│   │   └── schema.yml
│   ├── intermediate/      # Ephemeral (not materialized)
│   │   ├── int_schools_with_teachers.sql
│   │   └── int_students_with_grades.sql
│   └── marts/             # Silver → Gold (tables)
│       ├── core/
│       │   ├── dim_schools.sql
│       │   ├── dim_teachers.sql
│       │   ├── dim_students.sql
│       │   └── schema.yml
│       └── analytics/
│           ├── fct_grades.sql
│           ├── fct_grade_performance.sql
│           └── schema.yml
├── seeds/
│   └── seed_municipalities.csv
├── tests/
├── macros/
├── dbt_project.yml
├── packages.yml
└── profiles.yml.template
```

## Setup

1. Copy `profiles.yml.template` to `~/.dbt/profiles.yml`
2. Fill in your Snowflake credentials
3. Install dependencies:
   ```bash
   dbt deps
   ```

## Usage

```bash
# Test connection
dbt debug

# Load seed data (municipalities reference)
dbt seed

# Run all models
dbt run

# Run only staging models
dbt run --select staging

# Run only marts
dbt run --select marts

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## Data Flow

```
RAW (KMD_STAGING.RAW)
    │
    ▼
STAGING (views)
    │  - Clean data
    │  - Deduplicate
    │  - Standardize
    ▼
INTERMEDIATE (ephemeral)
    │  - Join related entities
    │  - Calculate aggregates
    ▼
MARTS (tables in KMD_STAGING.GOLD)
    │  - dim_schools
    │  - dim_teachers  
    │  - dim_students
    │  - fct_grades
    │  - fct_grade_performance
```

## Danish Context

- **Grade Scale**: Danish 7-point scale (-3, 00, 02, 4, 7, 10, 12)
- **Passing Grade**: 02 or higher (we use 7+ for "passing" in metrics)
- **School Types**: Folkeskole, Friskole, Privatskole, Specialskole
- **Grade Levels**: 0-9 (Børnehaveklasse through 9th grade)
