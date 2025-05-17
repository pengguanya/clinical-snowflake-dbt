# Architecture — Clinical DM

- **Ingestion**: CSVs -> RAW via connector.
- **Transform**: dbt staging (SDTM-like) -> silver (conformed) -> gold (DM KPIs).
- **Governance**: RBAC, masking of USUBJID, row access policy by STUDYID.
- **Orchestration**: Prefect flow (ingest -> dbt -> DQ).
- **CI**: GitLab pipeline with uv.
