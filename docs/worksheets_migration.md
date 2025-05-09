# Worksheets → GitLab repo mapping

Below are your worksheet names and the proposed target in this repo. Paste your SQL/Python into the placeholders or adopt the new dbt model.

| Worksheet | Keep/Adjust? | Target in repo | Notes |
|---|---|---|---|
| **Python: Show AE** | **Keep (convert)** | `src/analytics/ae_overview.py` | Replace ad‑hoc prints with a function and (optional) export to CSV/Parquet. |
| **Python: Count Categorical Data** | **Keep (convert)** | `src/analytics/category_counts.py` | Generic utility to count values for any column; used in KPIs. |
| **Python: Filter by STUDYID snowpark** | **Keep (convert)** | `src/queries/filter_by_studyid_snowpark.py` | Turn into reusable function; underpin row-access tests. |
| **Python: Filter by STUDYID with pandas** | **Keep (convert)** | `src/queries/filter_by_studyid_pandas.py` | For local/offline dev. |
