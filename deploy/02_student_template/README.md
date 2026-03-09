# Student Exercise Template

Build your own municipality data pipeline from S3 to Snowflake.

## Your Assignment

| Municipality | Code | Student | S3 Path |
|--------------|------|---------|---------|
| COPENHAGEN | 101 | Student 1 | `s3://ubulut-iceberg-oregon/data/copenhagen/` |
| AARHUS | 751 | Student 2 | `s3://ubulut-iceberg-oregon/data/aarhus/` |
| ODENSE | 461 | Student 3 | `s3://ubulut-iceberg-oregon/data/odense/` |
| AALBORG | 851 | Student 4 | `s3://ubulut-iceberg-oregon/data/aalborg/` |
| ESBJERG | 561 | (Spare) | `s3://ubulut-iceberg-oregon/data/esbjerg/` |

## Before You Start

Make sure `00_shared/prerequisites.sql` has been run (instructor does this).

## Instructions

1. **Copy your municipality folder** from `exercises/{your_municipality}/`
2. **Run scripts in order** (01 → 02 → 03 → 04 → 05)
3. **Replace placeholders** as you go:
   - `{MUNICIPALITY}` → Your municipality name (UPPERCASE)
   - `{municipality}` → Your municipality name (lowercase)
   - `{CODE}` → Your municipality code

## Architecture You'll Build

```
S3: s3://bucket/data/{municipality}/
         │
         ▼
┌─────────────────────────────────────────┐
│  {MUNICIPALITY}_DB                      │
│  ├── RAW (Snowpipe lands data here)     │
│  │   ├── STUDENTS_RAW                   │
│  │   ├── TEACHERS_RAW                   │
│  │   └── CLASSES_RAW                    │
│  │                                      │
│  ├── CDC (Streams & Tasks)              │
│  │   ├── STUDENTS_STREAM → Task         │
│  │   ├── TEACHERS_STREAM → Task         │
│  │   └── CLASSES_STREAM → Task          │
│  │                                      │
│  └── CLEAN (Deduplicated data)          │
│      ├── STUDENTS                       │
│      ├── TEACHERS                       │
│      └── CLASSES                        │
└─────────────────────────────────────────┘
```

## Files to Run

| Step | File | What It Creates |
|------|------|-----------------|
| 1 | `01_database.sql` | Database + schemas |
| 2 | `02_raw_tables.sql` | Stage + RAW/CLEAN tables |
| 3 | `03_snowpipe.sql` | Auto-ingest pipes |
| 4 | `04_streams_tasks.sql` | CDC pipeline |
| 5 | `05_security.sql` | Masking policies |

## Data Flow (Full-Load Pattern)

```
Day 1: dim_students_20260310.csv (2900 records) → Snowpipe → RAW → Stream → Task → CLEAN (2900)
Day 2: dim_students_20260311.csv (2910 records) → Snowpipe → RAW → Stream → Task → CLEAN (2910)
                                                                    └─ MERGE deduplicates!
```

Each daily file contains ALL records. The MERGE task:
- **Inserts** new student_ids
- **Updates** existing student_ids
- Result: CLEAN always has latest deduplicated data

## Testing Your Pipeline

```sql
-- 1. Check RAW count after file upload
SELECT COUNT(*) FROM {MUNICIPALITY}_DB.RAW.STUDENTS_RAW;

-- 2. Check stream has captured changes
SELECT COUNT(*) FROM {MUNICIPALITY}_DB.CDC.STUDENTS_STREAM;

-- 3. Execute task manually (or wait 5 min)
EXECUTE TASK {MUNICIPALITY}_DB.CDC.PROCESS_STUDENTS_TASK;

-- 4. Check CLEAN count
SELECT COUNT(*) FROM {MUNICIPALITY}_DB.CLEAN.STUDENTS;

-- 5. Test masking (should see masked CPR, email, phone)
USE ROLE {MUNICIPALITY}_ANALYST;
SELECT * FROM {MUNICIPALITY}_DB.RAW.STUDENTS_RAW LIMIT 5;
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Pipe not loading | Check `SHOW PIPES` for notification_channel, verify S3 event notification |
| Stream empty | Data must be in RAW table first |
| Task not running | Check `SHOW TASKS` - must be STARTED not SUSPENDED |
| Masking not working | Verify policy attached with `SHOW MASKING POLICIES` |
