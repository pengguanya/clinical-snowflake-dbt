import pandas as pd
def filter_dm_by_studyid_local(studyid: str, path='data/raw/DM.csv'):
    df = pd.read_csv(path)
    return df[df['STUDYID'] == studyid]
