import os
import json
import sys
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector as sf
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from typing import Sequence


# Setup paths and imports
env_path = Path(__file__).resolve().parents[1] / 'ingestion' / '.env'
load_dotenv(dotenv_path=env_path)

ingestion_path = Path(__file__).resolve().parents[1] / 'ingestion'
sys.path.append(str(ingestion_path))

from crawl import main as crawl_spotify_data


def _get_conn():
    return sf.connect(
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
        database  = os.getenv("SNOWFLAKE_DATABASE"),
        schema    = os.getenv("SNOWFLAKE_SCHEMA"),
    )


def load_df_to_snowflake(df: pd.DataFrame, table_name: str):
    """Insert DataFrame into Snowflake table."""
    if df.empty:
        print(f"⚠️  {table_name}: No data to load")
        return False

    with _get_conn() as conn:
        result = write_pandas(
            conn,
            df,
            table_name       = table_name,
            quote_identifiers= False
        )

        success = result[0]

        # Connector 3.13 → (success, nchunks, nrows)           ↴ int
        # Connector 3.0  → (success, nrows)                    ↴ int
        # Connector 2.x  → (success, nchunks, put_results[])   ↴ list
        third = result[-1]
        nrows = third if isinstance(third, int) else len(df)

        if success:
            print(f"✅  {table_name}: {nrows:,} rows loaded")
            return True
        else:
            print(f"❌  {table_name}: write_pandas reported failure")
            return False



def load_spotify_data():
    """Load Spotify artists and tracks."""
    print("📡 Fetching Spotify data...")
    data = crawl_spotify_data()
    
    top_artists = data.get('top_artists', [])
    top_tracks = data.get('top_tracks', [])
    
    if not top_artists or not top_tracks:
        print("❌ No Spotify data retrieved")
        return False
    
    # Process artists
    artists_df = pd.DataFrame([{
        'id': a['id'],
        'name': a['name'],
        'followers': a['followers'],
        'popularity': a['popularity'],
        'genres': json.dumps(a.get('genres', [])),
        'json_data': json.dumps(a.get('json_data', {}))
    } for a in top_artists])
    
    # Process tracks
    tracks_df = pd.DataFrame([{
        'id': t['id'],
        'name': t['name'],
        'artist_id': t['artists'][0]['id'],
        'popularity': t['popularity'],
        'duration_ms': t.get('duration_ms'),
        'explicit': t.get('explicit', False),
        'preview_url': t.get('preview_url'),
        'json_data': json.dumps(t)
    } for t in top_tracks])
    
    # Load to Snowflake
    artists_ok = load_df_to_snowflake(artists_df, 'RAW_TOP_ARTISTS')
    tracks_ok = load_df_to_snowflake(tracks_df, 'RAW_TOP_TRACKS')
    
    return artists_ok and tracks_ok


def load_listening_history(csv_path="dbt/seeds/seed_listening_history.csv"):
    """Load fake listening history from CSV."""
    if not os.path.exists(csv_path):
        print(f"⚠️  Listening history not found: {csv_path}")
        print("   Run fake_listening_history.py first")
        return False
    
    print("📖 Loading listening history...")
    df = pd.read_csv(csv_path)
    return load_df_to_snowflake(df, 'RAW_LISTENING_HISTORY')


def main(include_listening_history=True):
    """Load all Spotify data to Snowflake."""
    print("🎵 Starting Spotify data pipeline...")
    
    results = {}
    results['spotify'] = load_spotify_data()
    
    if include_listening_history:
        results['listening'] = load_listening_history()
    
    # Summary
    print("\n" + "="*40)
    success_count = sum(results.values())
    total_count = len(results)
    
    if success_count == total_count:
        print("🎉 All data loaded successfully!")
    else:
        print(f"⚠️  {success_count}/{total_count} datasets loaded")
    print("="*40)
    
    return success_count == total_count


if __name__ == "__main__":
    # Load everything
    main(include_listening_history=True)
    
    # Or load only Spotify data
    # main(include_listening_history=False)