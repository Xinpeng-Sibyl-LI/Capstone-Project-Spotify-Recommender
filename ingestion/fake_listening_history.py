import csv
import uuid
import random
import os
import json
import pandas as pd
from faker import Faker
from dotenv import load_dotenv
import snowflake.connector as sf
from datetime import datetime

fake = Faker()
Faker.seed(42)
random.seed(42)
load_dotenv()

def generate_fake_listening_history(n_plays=25000):
    # Connect to Snowflake
    conn = sf.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    )
    
    # Get tracks from Snowflake
    sql = """
    SELECT 
        ID as track_id,
        ARTIST_ID,
        TRACK_LANGUAGE
    FROM RAW_TOP_TRACKS
    """
    
    track_df = pd.read_sql(sql, conn)
    conn.close()
    
    if track_df.empty:
        print("⚠️ No tracks found")
        return pd.DataFrame()
    
    print(f"📊 Loaded {len(track_df)} tracks from Snowflake")
    
    # Generate fake plays
    DEVICE_POOL = ["Mobile", "Web", "Smart Speaker", "Car"]
    
    rows = []
    for i in range(n_plays):
        track_row = track_df.sample(1).iloc[0]
        
        # Use actual track language or default to 'und'
        track_language = track_row.get("TRACK_LANGUAGE")
        if pd.isna(track_language):
            track_language = "und"

        # Generate fake datetime and convert to string format that Snowflake likes
        fake_datetime = fake.date_time_between(start_date="-60d", end_date="now")
        play_ts_str = fake_datetime.strftime('%Y-%m-%d %H:%M:%S')

        rows.append({
            "play_id": str(uuid.uuid4()),
            "user_id": "xp", 
            "track_id": track_row["TRACK_ID"],
            "artist_id": track_row["ARTIST_ID"],

            "play_ts": play_ts_str,  # Now as string in YYYY-MM-DD HH:MM:SS format
            "track_language": track_language,
            "device": random.choice(DEVICE_POOL),
            "play_duration_seconds": random.randint(30, 300),
            "skipped": random.choice([True, False]) if random.random() < 0.3 else False
        })
    
    plays_df = pd.DataFrame(rows)
    print(f"✅ Generated {len(plays_df):,} listening records")
    
    return plays_df

def save_listening_history_to_csv(plays_df, output_path="data/raw_listening_history.csv"):
    if plays_df.empty:
        return False
        
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Check if file exists to determine if we need headers
    file_exists = os.path.exists(output_path)
    
    # Append mode if file exists, write mode if new file
    mode = 'a' if file_exists else 'w'
    header = not file_exists  # Only write header if file doesn't exist
    
    plays_df.to_csv(output_path, index=False, mode=mode, header=header, quoting=csv.QUOTE_MINIMAL)
    
    action = "Appended" if file_exists else "Saved"
    print(f"💾 {action} {len(plays_df):,} records to {output_path}")
    return True

def main():
    plays_df = generate_fake_listening_history(n_plays=250)
    
    if not plays_df.empty:
        save_listening_history_to_csv(plays_df)
        return plays_df
    
    return pd.DataFrame()

if __name__ == "__main__":
    df = main()
    
    # Optional: load directly to Snowflake
    if not df.empty:
        try:
            from load_to_snowflake import load_df_to_snowflake
            load_df_to_snowflake(df, "RAW_LISTENING_HISTORY")
        except ImportError:
            print("ℹ️ Skipping direct Snowflake load")