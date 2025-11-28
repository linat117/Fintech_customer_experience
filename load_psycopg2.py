# load_psycopg2.py
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# CONFIG - update or use environment variables
DB_HOST = os.getenv('DB_HOST','localhost')
DB_PORT = os.getenv('DB_PORT','5432')
DB_NAME = os.getenv('DB_NAME','app_reviews')
DB_USER = os.getenv('DB_USER','postgres')
DB_PASS = os.getenv('DB_PASS','12345678')   # set if needed

CSV_PATH = "data/clean_reviews.csv"

def get_conn():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    return conn

def main():
    print("Reading CSV:", CSV_PATH)
    df = pd.read_csv(CSV_PATH, dtype={'review':str, 'rating':float, 'date':str, 'app_name':str, 'source':str, 'country':str})
    # minimal safe cleaning
    df = df.dropna(subset=['review'])
    df['rating'] = df['rating'].fillna(0).astype(int)
    df['app_name'] = df.get('app_name', 'Unknown')
    df['source'] = df.get('source', 'Google Play')
    df['country'] = df.get('country', 'US')
    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d')
    df = df.dropna(subset=['date'])

    records = df[['review','rating','date','app_name','source','country']].apply(lambda row: tuple(row), axis=1).tolist()

    insert_sql = """
    INSERT INTO reviews (review, rating, date, app_name, source, country)
    VALUES %s
    ON CONFLICT ON CONSTRAINT unique_review_row
    DO NOTHING;
    """

    conn = get_conn()
    cur = conn.cursor()
    try:
        # chunked insertion to avoid huge single statement
        chunk_size = 1000
        for i in range(0, len(records), chunk_size):
            batch = records[i:i+chunk_size]
            execute_values(cur, insert_sql, batch, template=None, page_size=100)
            conn.commit()
            print(f"Inserted batch {i}..{i+len(batch)}")
    except Exception as e:
        conn.rollback()
        print("ERROR:", e)
        raise
    finally:
        cur.close()
        conn.close()

    print("Done. Attempted to insert total rows:", len(records))

if __name__ == "__main__":
    main()
