import os
def test_assets_exist():
    assert os.path.exists('transformations/dbt/models/gold/gold_study_quality_kpis.sql')
    assert os.path.exists('dq/run_ge_checks.py')
    assert os.path.exists('docs/worksheets_migration.md')
