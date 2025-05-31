import pandas as pd

def ae_overview(ae_csv='data/raw/AE.csv'):
    df = pd.read_csv(ae_csv)
    return (df.groupby(['STUDYID','AEDECOD','AESEV'])
              .size()
              .reset_index(name='n')
              .sort_values(['STUDYID','n'], ascending=[True, False]))

if __name__ == '__main__':
    print(ae_overview().head(20))
