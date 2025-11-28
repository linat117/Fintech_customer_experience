import pandas as pd
import glob
import os

DATADIR = os.path.join(os.path.dirname(__file__), '..', 'data')
OUTFILE = os.path.join(DATADIR, 'clean_reviews.csv')

def load_all_raw():
    files = glob.glob(os.path.join(DATADIR, 'raw_*.csv'))
    dfs = [pd.read_csv(f,dtype={'review': str, 'rating': float, 'date': str}) for f in files]
    if not dfs:
        raise FileNotFoundError("No raw CSV files found")
    return pd.concat(dfs, ignore_index=True)

def clean_df(df):
    # strip whitespace
    df['review'] = df['review'].astype(str).str.strip()
    # drop empty reviews
    df = df[df['review'].str.len() > 0].copy()
    # standardize rating to int
    df['rating'] = df['rating'].astype(int)
    # normalize date
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    # drop duplicates
    df = df.drop_duplicates(subset=['review', 'rating', 'date', 'bank'])
    # add source if missing
    if 'source' not in df.columns:
        df['source'] = 'Google Play'
    return df[['review', 'rating', 'date', 'bank', 'source']]

def main():
    df = load_all_raw()
    print("Raw total:", len(df))
    df_clean = clean_df(df)
    print("After clean:", len(df_clean))
    df_clean.to_csv(OUTFILE, index=False)
    print("Saved clean CSV:", OUTFILE)

if __name__ == "__main__":
    main()