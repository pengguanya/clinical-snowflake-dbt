from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

import os, subprocess
from prefect import flow, task
from src.load_csv_to_snowflake import load_raw_folder
from src.auth.wam_oauth import WAMPasswordTokenManager


def _set_oauth_token_from_wam():
    if os.getenv("SNOWFLAKE_AUTH_METHOD","password").lower() != "oauth":
        return None

    # choose one based on what you have:
    if os.getenv("WAM_USERNAME") and os.getenv("WAM_PASSWORD") and os.getenv("WAM_CLIENT_ID"):
        tm = WAMPasswordTokenManager(
            username=os.getenv("WAM_USERNAME"),
            password=os.getenv("WAM_PASSWORD"),
            client_id=os.getenv("WAM_CLIENT_ID"),
            scope=os.getenv("WAM_SCOPE","openid"),
        )
    elif os.getenv("WAM_CLIENT_ID") and os.getenv("WAM_CLIENT_SECRET"):
        tm = WAMClientCredsTokenManager(
            client_id=os.getenv("WAM_CLIENT_ID"),
            client_secret=os.getenv("WAM_CLIENT_SECRET"),
            scope=os.getenv("WAM_SCOPE","openid"),
        )
    else:
        raise RuntimeError("SNOWFLAKE_AUTH_METHOD=oauth but no WAM credentials provided (set either username/password+client_id OR client_id+client_secret).")

    token = tm.get_token()
    os.environ["SNOWFLAKE_OAUTH_TOKEN"] = token
    return token

@task
def ingest_raw():
    load_raw_folder("data/raw")

@task
def run_dbt():
    repo_root = Path(__file__).resolve().parents[1]
    dbt_dir = repo_root / "dbt"

    # Start with current env, then overlay values from .env
    env = os.environ.copy()
    env.update({k: v for k, v in dotenv_values(repo_root / ".env").items() if v is not None})

    # If you fetched an OAuth token earlier, make sure it’s in env, e.g.:
    # env["SNOWFLAKE_OAUTH_TOKEN"] = token

    subprocess.run(["uv", "run", "dbt", "debug", "--profiles-dir", "."], cwd=dbt_dir, env=env, check=True)
    subprocess.run(["uv", "run", "dbt", "build", "--profiles-dir", "."], cwd=dbt_dir, env=env, check=True)

@task
def run_ge():
    subprocess.run(["uv", "run", "python", "dq/run_ge_checks.py"], check=True)

@flow(name="clinical_cdm_elt_flow")
def main():
    _set_oauth_token_from_wam()   # <-- fetch and export token once
    ingest_raw()
    run_dbt()
    run_ge()

if __name__ == "__main__":
    main()
