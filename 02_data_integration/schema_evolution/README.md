# Schema Evolution in Snowflake

## Overview

Schema evolution allows Snowflake tables to automatically adapt when source file structures change. This is critical for production data pipelines where upstream systems may add columns, remove fields, or reorganize data.

## Key Configuration

```sql
-- Table-level: Enable schema evolution
CREATE TABLE my_table (...)
ENABLE_SCHEMA_EVOLUTION = TRUE;

-- File Format: Required settings for CSV
CREATE FILE FORMAT csv_evolution
    TYPE = 'CSV'
    PARSE_HEADER = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- COPY command: Match by column name
COPY INTO my_table
FROM @stage
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

## Behavior Matrix

| Scenario | What Snowflake Does | Your Action |
|----------|---------------------|-------------|
| **New Column Added** | Auto-adds column, old rows get NULL | None - automatic |
| **Column Missing** | Drops NOT NULL, new rows get NULL | Monitor data quality |
| **Column Renamed** | Treats as NEW column | Manual: Use transformation |
| **Data Type Change** | COPY fails or truncates | Manual: Pre-process |
| **Column Order Changed** | Handles via MATCH_BY_COLUMN_NAME | None - automatic |
| **Case Difference** | CASE_INSENSITIVE matches | None - automatic |

## Critical Warning: Column Renames

Snowflake **cannot detect** column renames. If `school_name` becomes `skole_navn`:

1. Snowflake sees `skole_navn` as a NEW column
2. Snowflake sees `school_name` as MISSING
3. Result: Two columns, data split between them

**Solution**: Handle in COPY transformation or use COALESCE view:

```sql
CREATE VIEW v_unified AS
SELECT 
    COALESCE(school_name, skole_navn) AS school_name,
    ...
FROM table;
```

## Monitoring Schema Evolution

```sql
-- Check evolution history
SELECT 
    COLUMN_NAME,
    PARSE_JSON(SCHEMA_EVOLUTION_RECORD):evolutionType::VARCHAR AS type,
    PARSE_JSON(SCHEMA_EVOLUTION_RECORD):fileName::VARCHAR AS file,
    PARSE_JSON(SCHEMA_EVOLUTION_RECORD):triggeringTime::TIMESTAMP AS when
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'MY_TABLE'
AND SCHEMA_EVOLUTION_RECORD IS NOT NULL;
```

## Best Practices

1. **Always use MATCH_BY_COLUMN_NAME** - Never rely on column position
2. **Enable PARSE_HEADER for CSV** - Let Snowflake read column names
3. **Set ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE** - Required for CSV evolution
4. **Monitor schema evolution records** - Set up alerts for unexpected changes
5. **Have a strategy for renames** - Document expected column mappings
6. **Version your source files** - Track which schema version each file uses

## Files in This Module

- `schema_evolution_demo.sql` - Comprehensive demo with all scenarios
- `README.md` - This documentation
