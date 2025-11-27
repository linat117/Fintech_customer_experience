from google_play_scraper import Sort , reviews
import pandas as pd 
import time 
import json 
import os

OUTDIR = os.path.join(os.path.dirname(__file__), '..', 'data')
os.makedirs(OUTDIR, exist_ok=True)

app_map = {
    "CBE" : "com.combanketh.mobilebanking",
    "BOA" : "com.boa.boaMobileBanking",
    "Dashen" : "com.dashen.dashensuperapp"
}


TARGET = 600
SLEEP= 1.0

def fetch_app_reviews(app_id, target = 600):
    all_reviews = []
    count = 0
    token = None
    while len(all_reviews) < target:
        result, token = reviews(
            app_id,
            lang='en',
            country='us',
            sort = Sort.NEWEST,
            count=200,
            continuation_token=token
        )
        if not result:
            break
        all_reviews.extend(result)
        print(f"Fetched { len(all_reviews)} for {app_id}")
        if token is None:
            break
        time.sleep(SLEEP)
    return all_reviews[:target]

def main():
    for bank, app_id in app_map.items():
        print(f"Scraping {bank} ({app_id}) ...")
        raw = fetch_app_reviews(app_id, target=TARGET)
        # save raw JSON
        raw_path = os.path.join(OUTDIR, f"raw_{bank}.json")
        with open(raw_path, 'w', encoding='utf-8') as f:
            json.dump(raw, f, default=str, ensure_ascii=False)
        # convert to dataframe and basic columns
        df = pd.DataFrame(raw)
        # rename common fields
        df = df.rename(columns={'content': 'review', 'score': 'rating', 'at': 'date'})
        df = df[['review', 'rating', 'date']].copy()
        df['bank'] = bank
        df['source'] = 'Google Play'
        # save raw CSV for preprocessing
        df.to_csv(os.path.join(OUTDIR, f"raw_{bank}.csv"), index=False)
        print(f"Saved raw_{bank}.csv with {len(df)} rows")

if __name__ == "__main__":
    main()