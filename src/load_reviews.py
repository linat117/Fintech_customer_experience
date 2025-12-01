import psycopg2
import pandas as pd

# Load the cleaned CSV file
df = pd.read_csv("data/reviews_cleaned.csv")

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    database="bank_reviews",
    user="postgres",
    password="12345678"
)

cur = conn.cursor()

# Map app_name to bank_id (replace with your actual IDs)
bank_mapping = {
    "App1": 1,
    "App2": 2,
    "App3": 3
}

# Insert reviews
for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO reviews (bank_id, review_text, rating, review_date, sentiment_label, sentiment_score, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            bank_mapping[row["bank"]],   # foreign key
            row["cleaned_review"],           # text
            row["rating"],                   # int
            row["date"],                     # date
            row["sentiment_label"],          # label
            row["sentiment_score"],          # score
            "Google Play"                    # source
        )
    )

conn.commit()
cur.close()
conn.close()

print("Data inserted successfully!")
