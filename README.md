# Clinical Data Management — Snowflake + dbt + Prefect

Clinical data management data product built on **Snowflake** with **dbt** and **Prefect**.

* **FAIR data products** on Snowflake with **ELT pipelines** (dbt), **data quality** (Great Expectations)
* **governance** (RBAC, masking, row access), **observability notes** (Monte Carlo / Ataccama), and **CI**.  


> **Scope**: Clinical DM entities (SDTM-like): **DM (demographics), AE (adverse events), CM (con meds)**.  
> Curated **gold** metrics: study data quality & AE summary KPIs.

## Quickstart (with uv)

```bash
# 1) Clone & enter
git clone <your-gitlab-project-url> clinical-data-mgmt-snowflake-dbt-demo
cd clinical-data-mgmt-snowflake-dbt-demo

# 2) Create environment & install
uv sync

# 3) Configure Snowflake
cp .env.example .env
# fill SNOWFLAKE_* variables

# 4) Bootstrap RBAC/DB/Policies
uv run scripts/bootstrap_snowflake.py

# 5) Run the ELT pipeline
prefect config set PREFECT_LOGGING_LEVEL=INFO
uv run flows/elt_flow.py

# 6) Optional: dbt alone
cd transformations/dbt
uv run dbt deps && uv run dbt build
```

### Overview
- **Data product (FAIR)**: `docs/data_product_contract.yml` and dbt metadata/tests.
- **Ingestion**: Python → Snowflake RAW (DM/AE/CM synthetic CSVs).
- **Transform**: dbt staging/silver/gold models with integrity checks and codelist constraints.
- **Governance**: RBAC roles; **masking** for USUBJID; **row access** policy by STUDYID.
- **DQ**: Great Expectations checks for DM/AE; freshness/valid values.
- **Observability**: Notes for **Monte Carlo** and **Ataccama** integration.
- **CI (GitLab)**: uv install, lint/tests, dbt compile/run (using protected variables).

### To map existing worksheets
See `docs/worksheets_migration.md`. Paste SQL/Python from Snowflake worksheets into the provided templates under `placeholders/worksheets/` **or** convert them into dbt models / Python tasks as suggested.

