import os
import pandas as pd


def check_data(df):
    print(f"Initial shape: {df.shape}")

    for column in df.columns:
        if df[column].isnull().any():
            print(f"Column '{column}' has null values.")
        else:
            print(f"Column '{column}' has no null values.")

    df.dropna(inplace=True)
    print(f"Shape after dropping nulls: {df.shape}")

    for column in df.columns:
        if 'date' in column.lower() or 'time' in column.lower():
            df[column] = pd.to_datetime(df[column], errors='coerce')

    return df


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, '..', '..'))
    csv_path = os.path.join(project_root, 'data', 'raw', 'police_reports.csv')
    df = pd.read_csv(csv_path)
    cleaned_df = check_data(df)
    cleaned_df.to_csv('data/processed/police_reports.csv',
                      index=False, encoding='utf-8')
