from dotenv import load_dotenv   #For loading environment variables from a .env file
import os   # OS-level operations (paths, env vars)
import base64   # For encoding credentials
from requests import post, get   #To make HTTP requests
import json    # For working with JSON data


# Loadd environment variables from .env.
load_dotenv()

# Retrieve Spotify API credentials from environment variables
client_id = os.getenv("client_id")
client_secret = os.getenv("client_secret")

# Function to retrieve an access token from Spotify API
def get_token():
    # Prepare credentials for encoding
    auth_string = client_id + ":" + client_secret
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

    # Make POST request to get token
    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}
    result = post(url, headers = headers, data = data)
    json_result = json.loads(result.content)
    print("Spotify token response:", json_result)
    
    token = json_result['access_token']
    return token


# Function to create an Authorization header using the token
def get_auth_header(token):
    return{"Authorization": "Bearer "+ token}


# Search Spotify for an artist by name
def search_for_artist(token, artist_name):
    url = "https://api.spotify.com/v1/search"
    headers = get_auth_header(token)
    query = f"?q={artist_name}&type=artist&limit=1"

    query_url = url + query
    result = get(query_url, headers=headers)
    json_result = json.loads(result.content)["artists"]["items"]

    # If no results found, return None
    if len(json_result) == 0:
        print("No artist with this name exists...")
        return None
    
    # Return the first matching artist's metadata
    return json_result[0]

# Given a list of artist IDs, fetch their top tracks
def get_top_tracks_for_artists(token, artist_ids):
    headers = get_auth_header(token)
    track_data = []

    for artist_id in artist_ids:
        url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks?country=US"
        res = get(url, headers=headers)
        tracks = json.loads(res.content).get("tracks", [])
        track_data.extend(tracks)
    return track_data

# Hardcoded list of top US artist names to query
TOP_US_ARTISTS = [
    "Drake", "Taylor Swift", "Morgan Wallen", "SZA", "The Weeknd",
    "Bad Bunny", "Metro Boomin", "Doja Cat", "Luke Combs", "Olivia Rodrigo"
]


# Query Spotify to fetch metadata for the top artists
def get_top_artists_us(token):
    headers = get_auth_header(token)
    url = "https://api.spotify.com/v1/search"
    artist_data = []

    for name in TOP_US_ARTISTS:
        query = f"?q={name}&type=artist&limit=1"
        res = get(url + query, headers=headers)
        items = json.loads(res.content).get("artists", {}).get("items", [])
        if items:
            artist_data.append(items[0])    # Add first match to list
    return artist_data

# Save JSON data to a file in the /data directory
def save_to_local_json(data, filename="data/raw_artists_tracks.json"):
    # Resolve the full path to the data directory (same level as ingestion folder)
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    full_path = os.path.join(base_dir, filename)

    # Make sure the directory exists
    os.makedirs(os.path.dirname(full_path), exist_ok=True)

    # Write the data to the specified JSON file
    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

# Main function to control the script workflow
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
    """Convenience wrapper so other modules can import fresh data."""
    return main()

if __name__ == "__main__":
    data = main()                 # run the workflow
    save_to_local_json(data)      # persist it (optional)
    print("✅ fetched", len(data["top_artists"]), "artists and",
          len(data["top_tracks"]), "tracks")
