from dotenv import load_dotenv
import os
import base64
from requests import post, get
import json
import time
import pandas as pd
from langdetect import detect, DetectorFactory, LangDetectException

# Make language detection deterministic
DetectorFactory.seed = 0

load_dotenv()

client_id = os.getenv("SPOTIFY_CLIENT_ID")
client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

def get_token():
    auth_string = client_id + ":" + client_secret
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}
    result = post(url, headers=headers, data=data)
    json_result = json.loads(result.content)
    return json_result['access_token']

def get_auth_header(token):
    return {"Authorization": "Bearer " + token}

def detect_language(text):
    """Return ISO-639-1 language code or 'und' if undetermined."""
    try:
        return detect(text)
    except LangDetectException:
        return "und"

def get_top_tracks_for_artists(token, artist_ids):
    headers = get_auth_header(token)
    track_data = []

    for artist_id in artist_ids:
        url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks?country=US"
        res = get(url, headers=headers)
        tracks = json.loads(res.content).get("tracks", [])
        
        for track in tracks:
            # Add language detection
            text_for_lang = f"{track['name']} {track['artists'][0]['name']}"
            track["language"] = detect_language(text_for_lang)
            
        track_data.extend(tracks)
        time.sleep(0.05)  # Rate limiting
        
    return track_data

TOP_ARTISTS = [
    "Bruno Mars", "The Weeknd", "Lady Gaga", "Billie Eilish", "Rihanna",
    "Coldplay", "Ed Sheeran", "Kendrick Lamar", "Bad Bunny", "Taylor Swift",
    "Justin Bieber", "Drake", "Ariana Grande", "David Guetta", "SZA",
    "Calvin Harris", "Maroon 5", "Post Malone", "Dua Lipa", "Eminem",
    "J Balvin", "Katy Perry", "Shakira", "Sia", "Travis Scott",
    "Pitbull", "Sabrina Carpenter", "Kanye West", "Miley Cyrus", "Lana Del Rey",
    "Beyoncé", "Chris Brown", "Imagine Dragons", "Adele", "Benson Boone",
    "Daddy Yankee", "Tate McRae", "Arctic Monkeys", "Future", "Karol G",
    "Doja Cat", "OneRepublic", "Marshmello", "Sam Smith", "Linkin Park",
    "Black Eyed Peas", "Alex Warren", "Khalid", "Playboi Carti", "Selena Gomez"
]

def get_top_artists_us(token):
    headers = get_auth_header(token)
    url = "https://api.spotify.com/v1/search"
    artist_data = []

    for name in TOP_ARTISTS:
        query = f"?q={name}&type=artist&limit=1"
        res = get(url + query, headers=headers)
        items = json.loads(res.content).get("artists", {}).get("items", [])
        if items:
            a = items[0]
            artist_data.append({
                "id": a["id"],
                "name": a["name"],
                "followers": a["followers"]["total"],
                "popularity": a["popularity"],
                "genres": a["genres"]
            })
        time.sleep(0.05)  # Rate limiting
        
    return artist_data

def save_to_local_csv(data, artists_filename="data/raw_artists.csv", tracks_filename="data/raw_tracks.csv"):
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    
    # Save artists to CSV
    artists_path = os.path.join(base_dir, artists_filename)
    os.makedirs(os.path.dirname(artists_path), exist_ok=True)
    
    artists_df = pd.DataFrame(data["top_artists"])
    artists_df.to_csv(artists_path, index=False)
    print(f"✅ Saved {len(artists_df)} artists to {artists_path}")
    
    # Save tracks to CSV
    tracks_path = os.path.join(base_dir, tracks_filename)
    os.makedirs(os.path.dirname(tracks_path), exist_ok=True)
    
    tracks_df = pd.DataFrame(data["top_tracks"])
    tracks_df.to_csv(tracks_path, index=False)
    print(f"✅ Saved {len(tracks_df)} tracks to {tracks_path}")

def save_to_local_json(data, filename="data/raw_artists_tracks.json"):
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    full_path = os.path.join(base_dir, filename)
    
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    
    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def main():
    token = get_token()
    
    top_artists = get_top_artists_us(token)
    artist_ids = [artist["id"] for artist in top_artists]
    top_tracks = get_top_tracks_for_artists(token, artist_ids)

    return {
        "top_artists": top_artists,
        "top_tracks": top_tracks
    }

def fetch_artists_and_tracks():
    return main()

if __name__ == "__main__":
    data = main()
    save_to_local_csv(data)  # Changed from save_to_local_json to save_to_local_csv
    print(f"✅ fetched {len(data['top_artists'])} artists and {len(data['top_tracks'])} tracks")