# analysis.py

import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# -----------------------------
# STEP 1: Load Cleaned CSV
# -----------------------------

def load_clean_data():
    df = pd.read_csv("data/task2_reviews_analysis.csv")
    print(f"Loaded {len(df)} cleaned rows.")
    return df


# -----------------------------
# STEP 2: Sentiment Analysis
# -----------------------------

def apply_sentiment(df):
    analyzer = SentimentIntensityAnalyzer()

    def get_score(text):
        return analyzer.polarity_scores(str(text))['compound']

    df['sentiment_score'] = df['review'].apply(get_score)

    def get_label(score):
        if score > 0.05:
            return "positive"
        elif score < -0.05:
            return "negative"
        else:
            return "neutral"

    df['sentiment_label'] = df['sentiment_score'].apply(get_label)

    print("Sentiment analysis applied.")
    return df


# -----------------------------
# STEP 3: Map app_name → bank_id
# -----------------------------

def map_bank_ids(df):
    bank_map = {
        "CBE": 1,
        "BOA": 2,
        "Dashen": 3
    }

    df['bank_id'] = df['bank'].map(bank_map)

    print("Bank IDs mapped.")
    return df


# -----------------------------
# STEP 4: Save enriched dataset
# -----------------------------

def save_enriched(df):
    output_path = "data/reviews_cleaned.csv"
    df.to_csv(output_path, index=False)
    print(f"Saved enriched dataset to {output_path}")


# -----------------------------
# MAIN EXECUTION
# -----------------------------

def main():
    print("Starting analysis...")

    df = load_clean_data()
    df = apply_sentiment(df)
    df = map_bank_ids(df)
    
    save_enriched(df)

    print("Analysis completed successfully!")


if __name__ == "__main__":
    main()
