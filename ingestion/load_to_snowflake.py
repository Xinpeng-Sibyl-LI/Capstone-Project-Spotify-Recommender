import os
import json
import sys
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector as sf
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime

# Setup paths and imports
load_dotenv()

# Add current directory to path so we can import crawl
sys.path.append(os.path.dirname(__file__))
from crawl import main as crawl_spotify_data

def _get_conn():
    return sf.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

def load_df_to_snowflake(df: pd.DataFrame, table_name: str, truncate_first=True):
    """Insert DataFrame into Snowflake table."""
    if df.empty:
        print(f"⚠️  {table_name}: No data to load")
        return False

    # Add ingested_at timestamp as string in format Snowflake expects
    df_with_timestamp = df.copy()
    df_with_timestamp['ingested_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Convert column names to uppercase for Snowflake
    df_with_timestamp.columns = [col.upper() for col in df_with_timestamp.columns]

    with _get_conn() as conn:
        try:
            # Truncate table first if requested (overwrite mode)
            if truncate_first:
                cursor = conn.cursor()
                cursor.execute(f"TRUNCATE TABLE {table_name}")
                cursor.close()
                print(f"🗑️  {table_name}: Table truncated")

            result = write_pandas(
                conn,
                df_with_timestamp,
                table_name=table_name,
                quote_identifiers=False,
                auto_create_table=False,
                overwrite=False
            )

            success = result[0]
            nrows = result[-1] if isinstance(result[-1], int) else len(df)

            if success:
                print(f"✅  {table_name}: {nrows:,} rows loaded")
                return True
            else:
                print(f"❌  {table_name}: write_pandas reported failure")
                return False
                
        except Exception as e:
            print(f"❌  {table_name}: Error - {e}")
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
    
    print(f"📊 Processing {len(top_artists)} artists and {len(top_tracks)} tracks")
    
    # Process artists
    artists_data = []
    for a in top_artists:
        artists_data.append({
            'id': a.get('id'),
            'name': a.get('name'),
            'followers': a.get('followers', 0),
            'popularity': a.get('popularity', 0),
            'genres': json.dumps(a.get('genres', [])),
            'json_data': json.dumps(a)
        })
    
    artists_df = pd.DataFrame(artists_data)
    
    # Process tracks
    tracks_data = []
    for t in top_tracks:
        # Get first artist ID safely
        artist_id = None
        if t.get('artists') and len(t['artists']) > 0:
            artist_id = t['artists'][0].get('id')
            
        tracks_data.append({
            'id': t.get('id'),
            'name': t.get('name'),
            'artist_id': artist_id,
            'popularity': t.get('popularity', 0),
            'duration_ms': t.get('duration_ms'),
            'explicit': t.get('explicit', False),
            'track_language': t.get('language', 'und'),
            'json_data': json.dumps(t)
        })
    
    tracks_df = pd.DataFrame(tracks_data)
    
    # Load to Snowflake - always overwrite artists and tracks
    artists_ok = load_df_to_snowflake(artists_df, 'RAW_TOP_ARTISTS', truncate_first=True)
    tracks_ok = load_df_to_snowflake(tracks_df, 'RAW_TOP_TRACKS', truncate_first=True)
    
    return artists_ok and tracks_ok

def load_listening_history():
    """Generate and load fake listening history."""
    print("🎭 Generating fake listening history...")
    
    try:
        from fake_listening_history import generate_fake_listening_history
        

        plays_df = generate_fake_listening_history(n_plays=250)
        
        if plays_df.empty:
            print("❌ No listening history data generated")
            return False
        
        # Load to Snowflake - append mode (no truncate)
        return load_df_to_snowflake(plays_df, 'RAW_LISTENING_HISTORY', truncate_first=False)
        
    except ImportError as e:
        print(f"❌ Could not import fake_listening_history: {e}")
        return False
    except Exception as e:
        print(f"❌ Error generating listening history: {e}")
        return False

def main(include_listening_history=True):
    """Load all Spotify data to Snowflake."""
    print("🎵 Starting Spotify data pipeline...")
    
    results = {}
    
    # Load Spotify data first
    results['spotify'] = load_spotify_data()
    
    if include_listening_history and results['spotify']:
        results['listening'] = load_listening_history()
    elif include_listening_history:
        print("⚠️ Skipping listening history (Spotify data load failed)")
        results['listening'] = False
    
    # Summary
    print("\n" + "="*50)
    success_count = sum(results.values())
    total_count = len(results)
    
    print(f"📊 Results:")
    for dataset, success in results.items():
        status = "✅" if success else "❌"
        print(f"  {status} {dataset.title()}: {'Success' if success else 'Failed'}")
    
    if success_count == total_count:
        print("\n🎉 All data loaded successfully!")
    else:
        print(f"\n⚠️  {success_count}/{total_count} datasets loaded successfully")
    print("="*50)
    
    return success_count == total_count

if __name__ == "__main__":
    success = main(include_listening_history=True)
    sys.exit(0 if success else 1)