# dbt for Snowsight - Municipality Setup

## Quick Start

### 1. Update `dbt_project.yml` for your municipality

Change these 3 values:
```yaml
vars:
  source_database: "COPENHAGEN_DB"    # Your database name
  municipality_code: 101              # Your municipality code  
  municipality_name: "Copenhagen"     # Your municipality name
```

### Municipality Codes
| Municipality | Code | Database |
|-------------|------|----------|
| Copenhagen | 101 | COPENHAGEN_DB |
| Aarhus | 751 | AARHUS_DB |
| Odense | 461 | ODENSE_DB |
| Aalborg | 851 | AALBORG_DB |
| Esbjerg | 561 | ESBJERG_DB |

### 2. profiles.yml (already configured)
The profiles.yml uses `KMD_WH` warehouse and `SYSADMIN` role - no changes needed.

### 3. Run in Snowsight
1. Go to Projects > dbt Projects
2. Sync your Git repository
3. Select the dbt project folder (`08_dbt`)
4. Click **Run** or **Build**

## Troubleshooting

### "No Profile" error
Make sure `profiles.yml` exists in the same folder as `dbt_project.yml`.

### "Invalid warehouse" error  
The profiles.yml must have `warehouse: KMD_WH` specified.

### Models show 0 rows
Your CLEAN tables may be empty. Check:
```sql
SELECT COUNT(*) FROM {YOUR_DB}.CLEAN.STUDENTS;
SELECT COUNT(*) FROM {YOUR_DB}.CLEAN.TEACHERS;
SELECT COUNT(*) FROM {YOUR_DB}.CLEAN.CLASSES;
```

If empty, run the Initial Load section from `04_streams_tasks.sql`.
