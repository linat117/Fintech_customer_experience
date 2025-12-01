import pandas as pd
import psycopg2

DB_NAME = "bank_reviews"
DB_USER = "postgres"
DB_PASS = "12345678"
DB_HOST = "localhost"
DB_PORT = "5432"

def main():
    df = pd.read_csv("data/reviews_cleaned.csv")

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO reviews 
        (bank_id, review_text, rating, review_date,  
         sentiment_label, sentiment_score,  source)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    for _, row in df.iterrows():
        cur.execute(insert_sql, (
            row["bank_id"],
            row["review"],
            row["rating"],
            row["date"],
            row["sentiment_label"],
            row["sentiment_score"],
            row["source"],
        ))

    conn.commit()
    cur.close()
    conn.close()

    print("✔ All reviews inserted successfully!")

if __name__ == "__main__":
    main()
