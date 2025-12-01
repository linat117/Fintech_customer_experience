# src/task4_analysis.py
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from collections import Counter
import re
from sqlalchemy import create_engine
from sklearn.feature_extraction.text import TfidfVectorizer
from wordcloud import WordCloud
import nltk
from nltk.corpus import stopwords
nltk.download('stopwords', quiet=True)

# CONFIG — update DB_URL if needed
DB_URL = "postgresql+psycopg2://postgres:12345678@localhost:5432/bank_reviews"
CSV_FALLBACK = "data/reviews_cleaned.csv"

OUTDIR = "outputs/figures"
os.makedirs(OUTDIR, exist_ok=True)

STOP = set(stopwords.words('english'))

# =========================
# 1. LOAD DATA
# =========================
def load_data():
    try:
        engine = create_engine(DB_URL)
        df = pd.read_sql("SELECT * FROM reviews", engine)
        print("Loaded from Postgres:", len(df), "rows")
    except Exception as e:
        print("DB load failed:", e)
        print("Falling back to CSV:", CSV_FALLBACK)
        df = pd.read_csv(CSV_FALLBACK)
        print("Loaded from CSV:", len(df), "rows")

    df.columns = [c.lower() for c in df.columns]

    # Determine date field
    if 'review_date' in df.columns:
        df['review_date'] = pd.to_datetime(df['review_date'])
    elif 'date' in df.columns:
        df['review_date'] = pd.to_datetime(df['date'])
    else:
        raise ValueError("Missing review_date column")

    # Determine bank column
    if 'app_name' in df.columns:
        df['bank_col'] = df['app_name']
    elif 'bank' in df.columns:
        df['bank_col'] = df['bank']
    elif 'bank_id' in df.columns:
        df['bank_col'] = df['bank_id']
    else:
        raise ValueError("No bank column found (expected app_name, bank, or bank_id)")

    # If cleaned_text missing, fallback to review_text
    if 'cleaned_text' not in df.columns:
        df['cleaned_text'] = df.get('review_text', df.get('review', "")).astype(str)

    return df


# =========================
# 2. TOKENIZE UTIL
# =========================
def tokenize(text):
    tokens = re.findall(r'\w+', str(text).lower())
    tokens = [t for t in tokens if t not in STOP and len(t) > 2]
    return tokens


# =========================
# 3. SENTIMENT TREND PLOT
# =========================
def plot_sentiment_trend(df):
    df2 = df.copy()
    df2['score'] = df2['sentiment_score'].astype(float)

    plt.figure(figsize=(10, 6))
    for bank, group in df2.groupby('bank_col'):
        ts = (
            group.set_index('review_date')
            .resample('D')['score']
            .mean()
            .ffill()
        )
        ts_ma = ts.rolling(7, min_periods=1).mean()
        plt.plot(ts_ma.index, ts_ma.values, label=str(bank))

    plt.title("Sentiment Trend (7-day Moving Average)")
    plt.xlabel("Date")
    plt.ylabel("Sentiment Score")
    plt.legend()
    fname = os.path.join(OUTDIR, "sentiment_trend.png")
    plt.tight_layout()
    plt.savefig(fname, dpi=150)
    plt.close()
    print("Saved", fname)


# =========================
# 4. RATING DISTRIBUTION
# =========================
def plot_rating_distribution(df):
    banks = df['bank_col'].unique()
    fig, axes = plt.subplots(
        1, len(banks), figsize=(5 * len(banks), 4), sharey=True
    )

    if len(banks) == 1:
        axes = [axes]

    for ax, bank in zip(axes, banks):
        subset = df[df['bank_col'] == bank]
        counts = subset['rating'].value_counts().sort_index()

        ax.bar(counts.index.astype(int), counts.values)
        ax.set_title(f"Rating Distribution: {bank}")
        ax.set_xlabel("Rating")
        ax.set_xticks([1, 2, 3, 4, 5])
        ax.set_ylabel("Count")

    plt.tight_layout()
    fname = os.path.join(OUTDIR, "rating_distribution.png")
    plt.savefig(fname, dpi=150)
    plt.close()
    print("Saved", fname)


# =========================
# 5. TOP KEYWORDS PER BANK
# =========================
def top_keywords_by_bank(df, top_n=15):
    results = {}

    for bank, group in df.groupby('bank_col'):
        tokens = []
        for text in group['cleaned_text'].astype(str):
            tokens.extend(tokenize(text))

        counter = Counter(tokens)
        top = counter.most_common(top_n)
        results[bank] = top

        # Save CSV
        pd.DataFrame(top, columns=["token", "count"]).to_csv(
            f"{OUTDIR}/top_tokens_{bank}.csv", index=False
        )

    return results


def plot_top_keywords(results):
    for bank, top in results.items():
        terms, counts = zip(*top)

        plt.figure(figsize=(8, 5))
        plt.barh(terms[::-1], counts[::-1])
        plt.title(f"Top Keywords - {bank}")
        plt.xlabel("Count")
        plt.tight_layout()

        fname = os.path.join(OUTDIR, f"top_keywords_{bank}.png")
        plt.savefig(fname, dpi=150)
        plt.close()
        print("Saved", fname)


# =========================
# 6. WORDCLOUDS
# =========================
def wordcloud_per_bank(df):
    for bank, group in df.groupby('bank_col'):
        text = " ".join(group['cleaned_text'].astype(str))
        if not text.strip():
            continue

        wc = WordCloud(
            width=900, height=450,
            background_color="white",
            stopwords=STOP
        ).generate(text)

        plt.figure(figsize=(10, 5))
        plt.imshow(wc, interpolation="bilinear")
        plt.axis("off")

        fname = os.path.join(OUTDIR, f"wordcloud_{bank}.png")
        plt.savefig(fname, dpi=150, bbox_inches="tight")
        plt.close()
        print("Saved", fname)


# =========================
# 7. DRIVERS & PAIN POINTS
# =========================
def extract_drivers_painpoints(df, top_n=10):
    summary = []

    for bank, group in df.groupby('bank_col'):
        total = len(group)

        # tokenize all
        tokens_all = Counter(
            t for text in group['cleaned_text'].astype(str)
            for t in tokenize(text)
        )

        # negative-only
        neg = group[group['sentiment_label'] == 'negative']
        tokens_neg = Counter(
            t for text in neg['cleaned_text'].astype(str)
            for t in tokenize(text)
        )

        # positive-only
        pos = group[group['sentiment_label'] == 'positive']
        tokens_pos = Counter(
            t for text in pos['cleaned_text'].astype(str)
            for t in tokenize(text)
        )

        drivers = [t for t, _ in tokens_pos.most_common(5)]
        pain = [t for t, _ in tokens_neg.most_common(5)]
        examples = neg['cleaned_text'].astype(str).head(3).tolist()

        summary.append({
            'bank': bank,
            'total_reviews': total,
            'drivers': drivers,
            'pain_points': pain,
            'neg_examples': examples
        })

    return summary


def save_summary(summary):
    rows = []
    for s in summary:
        rows.append({
            'bank': s['bank'],
            'total_reviews': s['total_reviews'],
            'drivers': ", ".join(s['drivers']),
            'pain_points': ", ".join(s['pain_points']),
            'neg_examples': " || ".join(s['neg_examples'])
        })

    out = "outputs/drivers_painpoints_summary.csv"
    pd.DataFrame(rows).to_csv(out, index=False)
    print("Saved", out)


# =========================
# MAIN
# =========================
def main():
    df = load_data()

    print("\nGenerating plots and insights...\n")

    plot_sentiment_trend(df)
    plot_rating_distribution(df)

    kw = top_keywords_by_bank(df)
    plot_top_keywords(kw)

    try:
        wordcloud_per_bank(df)
    except Exception as e:
        print("Wordcloud generation skipped:", e)

    summary = extract_drivers_painpoints(df)
    save_summary(summary)

    print("\nTask 4 complete! Check outputs/figures/ and outputs/drivers_painpoints_summary.csv\n")


if __name__ == "__main__":
    main()
