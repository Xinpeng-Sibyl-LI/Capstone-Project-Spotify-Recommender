import os
import json
import sys
from pathlib import Path
from dotenv import load_dotenv

import snowflake.connector

# Load environment variables from the .env file
env_path = Path(__file__).resolve().parents[1] / 'ingestion' / '.env'
load_dotenv(dotenv_path=env_path)

# Add the ingestion directory to sys.path to import crawl.py
ingestion_path = Path(__file__).resolve().parents[1] / 'ingestion'
sys.path.append(str(ingestion_path))

# Import the main function from crawl.py
from crawl import main as crawl_spotify_data

# Fetch data from Spotify API
def load_spotify_data():
    data = crawl_spotify_data()
    top_artists = data.get('top_artists', [])
    top_tracks = data.get('top_tracks', [])

    # Connect to Snowflake
    conn = connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    cursor = conn.cursor()

    cursor.execute("SELECT CURRENT_VERSION()")
    version = cursor.fetchone()
    print(version)


    # Insert top artists into RAW_TOP_ARTISTS table
    for artist in top_artists:
        try:
            cursor.execute("""
                INSERT INTO RAW_TOP_ARTISTS (id, name, followers, popularity)
                VALUES (%s, %s, %s, %s)
            """, (
                artist['id'],
                artist['name'],
                artist['followers']['total'],
                artist['popularity']
            ))
            print(f"Inserted artist: {artist['name']}")
        except Exception as e:
            print(f"Error inserting artist {artist['name']}: {e}")

    # Insert top tracks into RAW_TOP_TRACKS table
    for track in top_tracks:
        try:
            cursor.execute("""
                INSERT INTO RAW_TOP_TRACKS (id, name, artist_id, popularity)
                VALUES (%s, %s, %s, %s)
            """, (
                track['id'],
                track['name'],
                track['artists'][0]['id'],
                track['popularity']
            ))
            print(f"Inserted track: {track['name']}")
        except Exception as e:
            print(f"Error inserting track {track['name']}: {e}")

    # Close the cursor and connection
    cursor.close()
    conn.close()
    print("✅ Data successfully loaded into Snowflake.")
