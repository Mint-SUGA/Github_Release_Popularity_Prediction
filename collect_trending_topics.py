import os
import pandas as pd
import requests
import time
from dotenv import load_dotenv

load_dotenv()

INPUT_CSV = "data/trending_history.csv"
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN', '')

df = pd.read_csv(INPUT_CSV)
if 'topics' not in df.columns:
    df['topics'] = [[] for _ in range(len(df))]

HEADERS = {
    'Authorization': 'token ' + GITHUB_TOKEN,
    'Accept': 'application/vnd.github.v3+json'
}

for i, row in df.iterrows():
    if i > 600:
        repo_name = row['repo_name']
        if pd.isna(repo_name):
            continue
        # print(f"Processing {i+1}/{len(df)}: {repo_name}")
        try:
            response = requests.get(
                f"https://api.github.com/repos/{repo_name}",
                headers=HEADERS,
                timeout=10
            )
            if response.status_code == 200:
                topics = response.json().get('topics', [])
                df.at[i, 'topics'] = topics
                # print(f"  Topics: {topics}")
            else:
                print(f"  Error: {response.status_code}")
                df.at[i, 'topics'] = []
        except Exception as e:
            print(f"  Error: {e}")
            df.at[i, 'topics'] = []
        if (i + 1) % 50 == 0:
            df.to_csv(INPUT_CSV.replace('.csv', '_with_topics1.csv'), index=False)
        if (i + 1) % 200 == 0:
            print(f"{i+1} rows done")
        time.sleep(0.8 if GITHUB_TOKEN else 2)

df.to_csv(INPUT_CSV, index=False)
print(f"Topics all done! Written into {INPUT_CSV}")