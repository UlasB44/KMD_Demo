# ============================================================================
# DBT EXERCISE - Municipality Analytics Models
# ============================================================================
# This dbt project mirrors the Dynamic Tables from Step 7
# but uses dbt for transformation instead
# ============================================================================

## Prerequisites
- Complete Steps 01-04 (database, tables, snowpipe, streams/tasks)
- Have data loaded in your CLEAN schema

## Quick Start

### 1. Configure your profile
Create/edit `~/.dbt/profiles.yml`:
```yaml
kmd_municipality:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: BGYLLYD-AJ65411
      user: <your_username>
      authenticator: externalbrowser
      role: SYSADMIN
      warehouse: KMD_WH
      database: <MUNICIPALITY>_DB    # e.g., ESBJERG_DB
      schema: ANALYTICS
      threads: 4
```

### 2. Run with your municipality variables

**Option A: Override defaults via command line**
```bash
cd deploy/02_student_template/08_dbt

# For Esbjerg (default)
dbt run

# For Copenhagen
dbt run --vars '{"municipality": "COPENHAGEN", "municipality_code": 101, "municipality_name": "Copenhagen"}'

# For Aarhus
dbt run --vars '{"municipality": "AARHUS", "municipality_code": 751, "municipality_name": "Aarhus"}'

# For Odense
dbt run --vars '{"municipality": "ODENSE", "municipality_code": 461, "municipality_name": "Odense"}'

# For Aalborg
dbt run --vars '{"municipality": "AALBORG", "municipality_code": 851, "municipality_name": "Aalborg"}'
```

**Option B: Edit dbt_project.yml defaults**
Edit the `vars:` section in `dbt_project.yml` to match your municipality.

### 3. Run tests
```bash
dbt test
```

### 4. Generate documentation
```bash
dbt docs generate
dbt docs serve
```

## Models Created

| Model | Output Table | Description |
|-------|--------------|-------------|
| students_by_grade | DBT_STUDENTS_BY_GRADE | Student demographics per grade |
| class_enrollment | DBT_CLASS_ENROLLMENT | Enrollment vs capacity analysis |
| teacher_workload | DBT_TEACHER_WORKLOAD | Teacher student load metrics |
| municipality_overview | DBT_MUNICIPALITY_OVERVIEW | High-level KPIs |

## Dynamic Tables vs dbt

| Aspect | Dynamic Tables | dbt |
|--------|----------------|-----|
| Refresh | Automatic (TARGET_LAG) | Manual or scheduled |
| Version Control | SQL in Snowflake | Git-tracked models |
| Testing | Manual queries | Built-in test framework |
| Documentation | Comments | Auto-generated docs |
| Dependencies | Implicit | Explicit DAG |

## Municipality Codes Reference
- Copenhagen: 101
- Aarhus: 751
- Odense: 461
- Aalborg: 851
- Esbjerg: 561
