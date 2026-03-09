# KMD Workshop Deployment Guide

## Quick Deploy

```bash
# Prerequisites
# 1. Create storage integration (see deploy/00_prerequisites.sql)
# 2. Configure Snowflake CLI connection

# Deploy everything
./deploy/deploy_all.sh
```

## Manual Deploy (Step by Step)

Run SQL files in order:

| Step | File | Description |
|------|------|-------------|
| 0 | `00_prerequisites.sql` | Storage integration & warehouse (ACCOUNTADMIN) |
| 1 | `01_databases_schemas.sql` | Databases, schemas, file formats |
| 2 | `02_external_stages.sql` | 7 external stages pointing to S3 |
| 3 | `03_raw_tables_load.sql` | RAW tables + COPY INTO from S3 |
| 4 | `04_security.sql` | Roles, RLS policy, masking policies |
| 5 | `05_tenant_views.sql` | Municipality-specific views |
| 6 | `06_streams_tasks.sql` | CDC pipeline (streams + tasks) |
| 7 | `07_dynamic_tables.sql` | Analytics dynamic tables |
| 8 | `08_semantic_view.sql` | Cortex Analyst semantic view |

## What Gets Created

### Databases & Schemas
- **KMD_SCHOOLS**: COPENHAGEN, AARHUS, ODENSE, AALBORG, ESBJERG, SHARED
- **KMD_STAGING**: EXTERNAL_STAGES, RAW, CLEAN, CDC
- **KMD_ANALYTICS**: MARTS, SEMANTIC_MODELS

### External Stages (7)
| Stage | S3 Path |
|-------|---------|
| KMD_S3_STAGE | s3://ubulut-iceberg-oregon/data/ |
| COPENHAGEN_STAGE | s3://ubulut-iceberg-oregon/data/copenhagen/ |
| AARHUS_STAGE | s3://ubulut-iceberg-oregon/data/aarhus/ |
| ODENSE_STAGE | s3://ubulut-iceberg-oregon/data/odense/ |
| AALBORG_STAGE | s3://ubulut-iceberg-oregon/data/aalborg/ |
| ESBJERG_STAGE | s3://ubulut-iceberg-oregon/data/esbjerg/ |
| COMBINED_STAGE | s3://ubulut-iceberg-oregon/data/combined/ |

### Security
| Type | Name | Description |
|------|------|-------------|
| RLS Policy | MUNICIPALITY_RLS | Restricts data by municipality based on role |
| Masking | CPR_MASK | Masks Danish SSN (DDMMYY-XXXX) |
| Masking | EMAIL_MASK | Shows ***@domain.com |
| Masking | PHONE_MASK | Shows +45 60 ** ** ** |

### Roles
| Role | Access |
|------|--------|
| COPENHAGEN_ANALYST | Municipality 101 only |
| AARHUS_ANALYST | Municipality 751 only |
| ODENSE_ANALYST | Municipality 461 only |
| AALBORG_ANALYST | Municipality 851 only |
| ESBJERG_ANALYST | Municipality 561 only |
| KMD_DATA_ADMIN | All municipalities, full PII access |

### Data Loaded
| Table | Rows |
|-------|------|
| SCHOOLS_RAW | 35 |
| TEACHERS_RAW | 993 |
| STUDENTS_RAW | 14,614 |
| CLASSES_RAW | 699 |

## Testing Security

```sql
-- Test RLS as Copenhagen analyst
USE ROLE COPENHAGEN_ANALYST;
USE SECONDARY ROLES NONE;
USE WAREHOUSE KMD_WH;

-- Should only see municipality_code = 101
SELECT municipality_code, COUNT(*) 
FROM KMD_STAGING.RAW.STUDENTS_RAW 
GROUP BY municipality_code;

-- CPR should be masked
SELECT student_id, cpr_number, guardian_email 
FROM KMD_STAGING.RAW.STUDENTS_RAW LIMIT 5;
```

## Customization

To deploy to a different S3 bucket:
1. Update `00_prerequisites.sql` with your storage integration
2. Update URLs in `02_external_stages.sql`
