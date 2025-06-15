import pandas as pd

def category_counts(csv_path, column):
    df = pd.read_csv(csv_path)
    return df[column].value_counts().reset_index(names=[column, 'n'])

if __name__ == '__main__':
    print(category_counts('data/raw/DM.csv', 'SEX'))
