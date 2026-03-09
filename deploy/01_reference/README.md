# KMD Reference Implementation

This is the **instructor's reference implementation** showing all features with combined municipality data.

## Architecture

```
S3 (Combined Stage)
    ↓
┌─────────────────────────────────────────────────────────────┐
│  KMD_STAGING                                                 │
│  ├── EXTERNAL_STAGES (stages, file formats)                 │
│  ├── RAW (raw tables with RLS + masking)                    │
│  ├── CLEAN (deduplicated silver layer)                      │
│  └── CDC (streams, tasks)                                   │
├─────────────────────────────────────────────────────────────┤
│  KMD_SCHOOLS                                                 │
│  ├── COPENHAGEN (filtered views for municipality 101)       │
│  ├── AARHUS (filtered views for municipality 751)           │
│  ├── ODENSE (filtered views for municipality 461)           │
│  ├── AALBORG (filtered views for municipality 851)          │
│  └── ESBJERG (filtered views for municipality 561)          │
├─────────────────────────────────────────────────────────────┤
│  KMD_ANALYTICS                                               │
│  └── Dynamic Tables, Semantic Views                         │
└─────────────────────────────────────────────────────────────┘
```

## Deployment Order

```bash
# 1. Prerequisites (run from 00_shared first!)
# 2. Then run these in order:
snowsql -f 01_databases_schemas.sql
snowsql -f 02_external_stages.sql
snowsql -f 03_raw_tables_load.sql
snowsql -f 04_security.sql
snowsql -f 05_tenant_views.sql
snowsql -f 06_streams_tasks.sql
snowsql -f 07_dynamic_tables.sql
snowsql -f 08_semantic_view.sql
```

## Features Demonstrated

| Feature | File | Description |
|---------|------|-------------|
| Storage Integration | 02_external_stages.sql | S3 access via IAM role |
| Snowpipe | 06_streams_tasks.sql | Auto-ingest from S3 |
| Streams | 06_streams_tasks.sql | CDC on RAW tables |
| Tasks | 06_streams_tasks.sql | Scheduled MERGE to CLEAN |
| Row-Level Security | 04_security.sql | Municipality isolation |
| Dynamic Masking | 04_security.sql | CPR, email, phone masking |
| Schema-per-Tenant | 05_tenant_views.sql | Municipality-specific views |
| Dynamic Tables | 07_dynamic_tables.sql | Real-time analytics |
| Semantic Views | 08_semantic_view.sql | Cortex Analyst |

## Data Flow

```
S3 Event → Snowpipe → RAW (append) → Stream → Task (MERGE) → CLEAN
                         ↓
                    Municipality Views (RLS filtered)
                         ↓
                    Dynamic Tables → Semantic View
```
