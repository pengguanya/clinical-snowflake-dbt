# Worksheets → GitLab repo mapping

Below are your worksheet names and the proposed target in this repo. Paste your SQL/Python into the placeholders or adopt the new dbt model.

| Worksheet | Keep/Adjust? | Target in repo | Notes |
|---|---|---|---|
| **Python: Show AE** | **Keep (convert)** | `src/analytics/ae_overview.py` | Replace ad‑hoc prints with a function and (optional) export to CSV/Parquet. |
| **Python: Count Categorical Data** | **Keep (convert)** | `src/analytics/category_counts.py` | Generic utility to count values for any column; used in KPIs. |
| **Python: Filter by STUDYID snowpark** | **Keep (convert)** | `src/queries/filter_by_studyid_snowpark.py` | Turn into reusable function; underpin row-access tests. |
| **Python: Filter by STUDYID with pandas** | **Keep (convert)** | `src/queries/filter_by_studyid_pandas.py` | For local/offline dev. |
| **Python: Print Table Name Match Pattern** | Adjust | `src/metadata/table_pattern_search.py` | Add regex + information_schema scan. |
| **SQL: Show partial date** | **Keep (convert)** | `transformations/dbt/macros/partial_date.sql` and used in `stg_dm.sql` | Macro to parse SDTM partial dates (YYYY-??-?? etc.). |
| **SQL: Add and rename columns** | **Keep (convert)** | dbt models `stg_*` | Use dbt `alias` & `select` with explicit casts; version control columns. |
| **Match Query Check / Filter Text / Refine** | Replace | dbt tests + GE checks | Convert text filtering checks into dbt tests (`accepted_values`, `relationships`). |
| **Row click action** | Drop | n/a | UI-specific; not needed in backend repo. |
| **TT Refresh / TT Refresh 2** | Replace | `flows/elt_flow.py` | Orchestration covers refresh; schedule in GitLab or Prefect. |
| **0000-AAA-Intellicheck_create_CO44657_TABLES** | Keep (rename)** | `transformations/dbt/models/staging/stg_intellicheck.sql` | If IntelliCheck is a source, keep as external source; otherwise remove. |
| **PDIL_DEV2-PD24_REGISTER_INFO_* / 2025-09-* timestamps** | Merge | `transformations/dbt/models/staging` | Group per domain; avoid timestamped names; use version control for history. |

> See `placeholders/worksheets/` for one-to-one template files where you can paste your current SQL/Python.
