# load_psycopg2.py
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# CONFIG - environment variables or defaults
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'app_reviews')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASS', '12345678')

CSV_PATH = "data/clean_reviews.csv"
SCHEMA_FILE = "src/schema.sql"  # contains banks + reviews table creation

BANKS_LIST = ["Bank A", "Bank B", "Bank C"]  # replace with actual bank names

def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def main():
    conn = get_conn()
    cur = conn.cursor()

    # --- 1. Execute schema.sql ---
    print("Executing schema file:", SCHEMA_FILE)
    with open(SCHEMA_FILE, 'r') as f:
        cur.execute(f.read())
    conn.commit()

    # --- 2. Insert banks ---
    for bank in BANKS_LIST:
        cur.execute("INSERT INTO banks (name) VALUES (%s) ON CONFLICT (name) DO NOTHING", (bank,))
    conn.commit()

    # --- 3. Read CSV ---
    print("Reading CSV:", CSV_PATH)
    df = pd.read_csv(CSV_PATH, dtype=str)
    df = df.dropna(subset=['review'])
    df['rating'] = df['rating'].fillna(0).astype(int)
    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d')
    df = df.dropna(subset=['date'])

    # Map bank names to bank_id
    cur.execute("SELECT bank_id, name FROM banks")
    bank_map = {name: bank_id for bank_id, name in cur.fetchall()}

    df = df[df['app_name'].isin(bank_map.keys())]  # keep only known banks
    df['bank_id'] = df['app_name'].map(bank_map)

    records = df[['bank_id', 'review', 'rating', 'date', 'source']].apply(lambda row: tuple(row), axis=1).tolist()

    # --- 4. Insert reviews in batches ---
    insert_sql = """
        INSERT INTO reviews (bank_id, review, rating, review_date, source)
        VALUES %s
        ON CONFLICT DO NOTHING;
    """

    chunk_size = 1000
    for i in range(0, len(records), chunk_size):
        batch = records[i:i+chunk_size]
        execute_values(cur, insert_sql, batch)
        conn.commit()
        print(f"Inserted batch {i}..{i+len(batch)}")

    # --- 5. Verification queries ---
    print("\nVerification queries:")
    cur.execute("SELECT COUNT(*) FROM reviews")
    print("Total reviews:", cur.fetchone()[0])

    cur.execute("""
        SELECT b.name, COUNT(r.review_id)
        FROM reviews r
        JOIN banks b ON r.bank_id = b.bank_id
        GROUP BY b.name
    """)
    print("Reviews per bank:", cur.fetchall())

    cur.execute("""
        SELECT b.name, AVG(r.rating) as avg_rating
        FROM reviews r
        JOIN banks b ON r.bank_id = b.bank_id
        GROUP BY b.name
    """)
    print("Average rating per bank:", cur.fetchall())

    # --- 6. Close connection ---
    cur.close()
    conn.close()
    print("Done. Attempted to insert total rows:", len(records))

if __name__ == "__main__":
    main()
