# file: ingestion/crawl_ds.py

from dotenv import load_dotenv       # for loading environment variables
import os                             # for env vars, paths
import base64                         # for encoding client credentials
from requests import post, get        # to call Spotify endpoints
import json                           # for JSON dumps (optional)
import pandas as pd                   # for CSV output
import csv                            # for quoting constants

# ─── 1. load environment variables ─────────────────────────────────────────────
# Make sure your .env lives at the same level (or above) so load_dotenv() finds it.
load_dotenv()

# Spotify credentials must be in your .env exactly as:
#   SPOTIFY_CLIENT_ID=...
#   SPOTIFY_CLIENT_SECRET=...
client_id = os.getenv("client_id")
client_secret = os.getenv("client_secret")

if not client_id or not client_secret:
    raise RuntimeError("Missing client_id or client_secret in environment.")


# ─── 2. get_token() ─────────────────────────────────────────────────────────────
def get_token() -> str:
    """
    Exchange client credentials for a Bearer token.
    """
    auth_string = f"{client_id}:{client_secret}"
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = base64.b64encode(auth_bytes).decode("utf-8")

    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": f"Basic {auth_base64}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}

    response = post(url, headers=headers, data=data)
    response.raise_for_status()
    token = response.json().get("access_token")
    if not token:
        raise RuntimeError("Failed to obtain Spotify access token.")
    return token


# ─── 3. helper: attach Bearer token ─────────────────────────────────────────────
def get_auth_header(token: str) -> dict:
    """
    Given a valid Spotify access token, return the Authorization header dict.
    """
    return {"Authorization": f"Bearer {token}"}


# ─── 4. get_playlist_tracks() ───────────────────────────────────────────────────
def get_playlist_tracks(token: str, playlist_id: str) -> list[dict]:
    """
    Given an access token and a playlist ID, return a list of track objects.
    """
    headers = get_auth_header(token)
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
    params = {"fields": "items(track(id,name,artists)),next", "limit": 100}
    tracks: list[dict] = []
    while url:
        res = get(url, headers=headers, params=params)
        res.raise_for_status()
        payload = res.json()
        for item in payload.get("items", []):
            if item.get("track") and item["track"].get("artists"):
                tracks.append(item["track"])
        # Move to next page, if present
        url = payload.get("next")
        params = None
    return tracks


# ─── 5. get_top_artists_from_playlist() ────────────────────────────────────────
from collections import Counter

def get_top_artists_from_playlist(token: str, playlist_id: str, top_n: int = 10) -> list[str]:
    """
    Fetches all tracks in the given playlist, extracts each track's primary artist ID,
    and returns the top N artist IDs by frequency.
    """
    playlist_tracks = get_playlist_tracks(token, playlist_id)
    artist_counter = Counter()
    for track in playlist_tracks:
        primary_artist = track["artists"][0]["id"]
        artist_counter[primary_artist] += 1
    top_artists = [artist_id for artist_id, _ in artist_counter.most_common(top_n)]
    return top_artists


# ─── 6. get_artists_metadata() ─────────────────────────────────────────────────
def get_artists_metadata(token: str, artist_ids: list[str]) -> list[dict]:
    """
    Given a list of artist IDs (up to 50), call the "Get Several Artists" endpoint
    and return a list of artist‐info dicts (including genres, followers, popularity, etc.)
    """
    if not artist_ids:
        return []

    headers = get_auth_header(token)
    ids_param = ",".join(artist_ids[:50])
    url = f"https://api.spotify.com/v1/artists"
    params = {"ids": ids_param}

    res = get(url, headers=headers, params=params)
    res.raise_for_status()
    return res.json().get("artists", [])


# ─── 7. get_top_tracks_for_artists() ────────────────────────────────────────────
def get_top_tracks_for_artists(token: str, artist_ids: list[str]) -> list[dict]:
    """
    Given a list of Spotify artist IDs, fetch each artist’s top tracks (country=US)
    and return a combined list of track dictionaries.
    """
    headers = get_auth_header(token)
    track_data: list[dict] = []
    for artist_id in artist_ids:
        url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks?country=US"
        res = get(url, headers=headers)
        res.raise_for_status()
        tracks = res.json().get("tracks", [])
        track_data.extend(tracks)
    return track_data


# ─── 8. get_top_artists_us() (legacy) ───────────────────────────────────────────
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


def get_top_artists_us(token: str) -> list[dict]:
    """
    (Legacy) Hard-coded top 10 US artist names → fetch metadata for each.
    Returns a list of artist metadata dicts.
    """
    headers = get_auth_header(token)
    url = "https://api.spotify.com/v1/search"
    artist_data: list[dict] = []
    for name in TOP_ARTISTS:
        q = f"?q={name}&type=artist&limit=1"
        res = get(url + q, headers=headers)
        res.raise_for_status()
        items = res.json().get("artists", {}).get("items", [])
        if items:
            artist_data.append(items[0])
    return artist_data


# ─── 9. save_to_local_json() (optional) ─────────────────────────────────────────
def save_to_local_json(data: dict, filename: str = "data/raw_artists_tracks.json"):
    """
    Save the given dict (with keys 'top_artists', 'top_tracks') into JSON on disk.
    """
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    full_path = os.path.join(base_dir, filename)
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


# ─── 10. MAIN() ─────────────────────────────────────────────────────────────────
def main() -> dict:
    """
    1) Get Spotify token
    2) Fetch either hard-coded top artists or “live” top artists
    3) Fetch top tracks for those artists
    4) Write out CSV files and return the combined dict
    """
    token = get_token()
    print(f"DEBUG: Retrieved token (length {len(token)})")

    # ── (A) Option 1: hard-coded top 10 US names ──────────────────────────────
    top_artists_metadata = get_top_artists_us(token)

    # If you want “live” top artists from the Global Top 50 playlist instead,
    # comment out the two lines above and uncomment below:
    #
    # PLAYLIST_ID = "37i9dQZEVXbMDoHDwVN2tF"  # Spotify’s “Global Top 50” ID
    # top_artist_ids = get_top_artists_from_playlist(token, PLAYLIST_ID, top_n=10)
    # top_artists_metadata = get_artists_metadata(token, top_artist_ids)

    # 2b. Fetch each artist’s top tracks (country=US)
    artist_ids = [artist["id"] for artist in top_artists_metadata]
    top_tracks = get_top_tracks_for_artists(token, artist_ids)

    # 3. Convert lists of dicts into pandas DataFrames and write CSV
    # ────────────────────────────────────────────────────────────────────────────

    # 3a. Top Artists → DataFrame
    artists_df = pd.json_normalize(top_artists_metadata)
    if not artists_df.empty:
        # Flatten nested 'followers.total' to its own column
        if "followers.total" in artists_df.columns:
            artists_df["followers_total"] = artists_df["followers.total"]
        artists_out = artists_df[[
            "id", "name", "followers_total", "popularity", "genres"
        ]].rename(columns={
            "id": "artist_id",
            "name": "artist_name",
            "popularity": "artist_popularity"
        })
    else:
        artists_out = pd.DataFrame(
            columns=["artist_id", "artist_name", "followers_total", "artist_popularity", "genres"]
        )

    # 3b. Top Tracks → DataFrame
    tracks_df = pd.json_normalize(top_tracks)
    if not tracks_df.empty:
        columns_to_keep = [
            "id", "name", "artists[0].id", "popularity", "album.name", "duration_ms", "explicit"
        ]
        for col in columns_to_keep:
            if col not in tracks_df.columns:
                tracks_df[col] = None  # ensure column exists even if missing
        tracks_out = tracks_df[columns_to_keep].rename(columns={
            "id": "track_id",
            "name": "track_name",
            "artists[0].id": "primary_artist_id",
            "popularity": "track_popularity",
            "album.name": "album_name",
            "duration_ms": "duration_ms",
            "explicit": "is_explicit"
        })
    else:
        tracks_out = pd.DataFrame(
            columns=[
                "track_id", "track_name", "primary_artist_id",
                "track_popularity", "album_name", "duration_ms", "is_explicit"
            ]
        )

    # 3c. Make sure the output 'data/' folder exists at project root
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    data_dir = os.path.join(base_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    # 3d. Write Artists CSV
    artists_csv_path = os.path.join(data_dir, "top_artists.csv")
    artists_out.to_csv(
        artists_csv_path,
        index=False,
        quoting=csv.QUOTE_MINIMAL
    )
    print(f"✅ Wrote {len(artists_out):,} artists → {artists_csv_path}")

    # 3e. Write Tracks CSV
    tracks_csv_path = os.path.join(data_dir, "top_tracks.csv")
    tracks_out.to_csv(
        tracks_csv_path,
        index=False,
        quoting=csv.QUOTE_MINIMAL
    )
    print(f"✅ Wrote {len(tracks_out):,} tracks  → {tracks_csv_path}")

    # 4. Optionally save JSON as well (legacy behavior)
    # save_to_local_json({"top_artists": top_artists_metadata, "top_tracks": top_tracks})

    return {
        "top_artists": top_artists_metadata,
        "top_tracks": top_tracks
    }


# ─── 11. Convenience function for other modules ────────────────────────────────
def fetch_artists_and_tracks() -> dict:
    """
    If you want to import this module elsewhere, call:
        from crawl_ds import fetch_artists_and_tracks
        data = fetch_artists_and_tracks()
    It will fetch and return the same dict without writing CSV.
    """
    return main()


# ─── 12. If run as a script, kick off main() ───────────────────────────────────
if __name__ == "__main__":
    data = main()
    print(
        "✅ fetched",
        len(data["top_artists"]),
        "artists and",
        len(data["top_tracks"]),
        "tracks"
    )
