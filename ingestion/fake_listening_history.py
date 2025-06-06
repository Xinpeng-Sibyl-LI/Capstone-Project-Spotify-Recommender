"""
fake_listening_history.py
----------------------------------
Writes seed_listening_history.csv with the schema above.
"""

import csv, uuid, random, os
from datetime import datetime, timedelta
import pandas as pd
from faker import Faker
from dotenv import load_dotenv
import snowflake.connector as sf
from load_to_snowflake import load_df_to_snowflake
from pathlib import Path


fake = Faker()
Faker.seed(42)
random.seed(42)

load_dotenv()                        # pull your Snowflake creds

# ─── 1. Pull real track + artist IDs + genres from Snowflake ────────────────
conn = sf.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
)
sql = """
SELECT
    t.id          AS track_id,
    a.id          AS artist_id,
    a.genres      AS artist_genres
FROM raw_top_tracks t
JOIN raw_top_artists a
  ON a.id = t.artist_id
"""
track_df = pd.read_sql(sql, conn)
conn.close()

# ─── 2. Build fake plays ────────────────────────────────────────────────────
N_PLAYS = 25_000
DEVICE_POOL = ["Mobile", "Web", "Smart Speaker", "Car"]
LANG_POOL   = ["en", "es", "fr", "de", "pt", "it"]

user_id = str(uuid.uuid4())          # single listener; change if you want many
rows = []

for _ in range(N_PLAYS):
    track_row = track_df.sample(1).iloc[0]

    # ── robust genres guard ───────────────────────────────────────────────
    genres_raw = track_row["ARTIST_GENRES"]

    if pd.isna(genres_raw):                          # None / NaN
        genres_list = []
    elif isinstance(genres_raw, list):               # already a list
        genres_list = genres_raw
    else:                                            # string like '["pop","rock"]'
        try:
            genres_list = json.loads(genres_raw)
        except Exception:
            genres_list = []

    # ── build the row; now use genres_list (safe) ─────────────────────────
    rows.append({
        "play_id":   str(uuid.uuid4()),
        "user_id":   "xp",
        "track_id":  track_row["TRACK_ID"],
        "artist_id": track_row["ARTIST_ID"],
        "play_ts":   fake.date_time_between(start_date="-60d", end_date="now"),
        "genres":    random.sample(genres_list, k=min(2, len(genres_list)))
                     if genres_list else [],      # ← genres_list, not raw column
        "language":  random.choice(LANG_POOL),
        "device":    random.choice(DEVICE_POOL),
    })

plays_df = pd.DataFrame(rows)

# ─── 3. Save to a seed file ────────────────────────────────────────────────
out_path = "dbt/seeds/seed_listening_history.csv"
os.makedirs(os.path.dirname(out_path), exist_ok=True)
plays_df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
print(f"✅ wrote {len(plays_df):,} fake plays → {out_path}")

# Push the DataFrame straight to Snowflake
load_df_to_snowflake(plays_df, "RAW_LISTENING_HISTORY") 