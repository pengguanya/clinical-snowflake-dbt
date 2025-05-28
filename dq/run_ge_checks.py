import pandas as pd
from great_expectations.dataset import PandasDataset

def check_dm(path="data/raw/DM.csv"):
    df = pd.read_csv(path)
    ds = PandasDataset(df)
    ds.expect_column_values_to_not_be_null("USUBJID")
    ds.expect_column_values_to_be_in_set("SEX", ["M","F"])  # extend per SDTM
    ds.expect_column_values_to_match_regex("BRTHDTC", r"^\d{4}(-\d{2}|-\?\?)?(-\d{2}|-\?\?)?$")  # allow partial dates
    return ds.validate()["success"]

def check_ae(path="data/raw/AE.csv"):
    df = pd.read_csv(path)
    ds = PandasDataset(df)
    ds.expect_column_values_to_not_be_null("USUBJID")
    ds.expect_column_values_to_be_in_set("AESEV", ["MILD","MODERATE","SEVERE"])
    return ds.validate()["success"]

if __name__ == "__main__":
    ok = check_dm() and check_ae()
    print("GE checks passed?", ok)
    if not ok: raise SystemExit(1)
