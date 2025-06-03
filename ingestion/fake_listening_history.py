"""
generate_fake_listening_history.py
---------------------------------
Create a CSV (or directly load to Snowflake) that references real track_ids.
"""

import os, random, uuid, csv
import pandas as pd
from faker import Faker
from dotenv import load_dotenv
import snowflake.connector as sf  # optional direct load

fake = Faker()
Faker.seed(42)
random.seed(42)

# ─── 1. Read real track_ids (choose ONE of the options) ──────────────────────
# Option A: from local CSV exported previously
# tracks_df = pd.read_csv("seed_raw_top_tracks.csv")

# Option B: pull live from Snowflake
load_dotenv()
conn = sf.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
)
tracks_df = pd.read_sql("SELECT id FROM raw_top_tracks", conn)
conn.close()

track_ids = tracks_df["ID"].tolist()
user_ids  = [fake.uuid4() for _ in range(200)]

N_PLAYS   = 25_000

rows = []
for _ in range(N_PLAYS):
    rows.append(
        dict(
            PLAY_ID=str(uuid.uuid4()),
            USER_ID=random.choice(user_ids),
            TRACK_ID=random.choice(track_ids),
            PLAY_TS=fake.date_time_between(start_date="-30d", end_date="now"),
            DEVICE=random.choice(["Mobile", "Web", "Smart Speaker", "Car"]),
        )
    )

plays_df = pd.DataFrame(rows)

# ─── 2. Write to CSV seed file ───────────────────────────────────────────────
plays_df.to_csv("seed_listening_history.csv", index=False, quoting=csv.QUOTE_MINIMAL)
print("✅  Wrote", len(plays_df), "fake plays to seed_listening_history.csv")
