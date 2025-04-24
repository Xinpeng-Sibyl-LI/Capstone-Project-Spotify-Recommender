
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
import os
import base64
from requests import post, get
import json

# take environment variables from .env.
load_dotenv()

client_id = os.getenv("client_id")
client_secret = os.getenv("client_secret")

# get authorizatio token
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
    result = post(url, headers = headers, data = data)
    json_result = json.loads(result.content)
    token = json_result['access_token']
    return token

def get_auth_header(token):
    return{"Authorization": "Bearer "+ token}


# Write a function to allow searching artist and get the artist's top tracks
def search_for_artist(token, artist_name):
    url = "https://api.spotify.com/v1/search"
    headers = get_auth_header(token)
    query = f"?q={artist_name}&type=artist&limit=1"

    query_url = url + query
    result = get(query_url, headers=headers)
    json_result = json.loads(result.content)["artists"]["items"]
    if len(json_result) == 0:
        print("No artist with this name exists...")
        return None
    
    return json_result[0]

def get_top_tracks_for_artists(token, artist_ids):
    headers = get_auth_header(token)
    track_data = []

    for artist_id in artist_ids:
        url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks?country=US"
        res = get(url, headers=headers)
        tracks = json.loads(res.content).get("tracks", [])
        track_data.extend(tracks)
    return track_data

TOP_US_ARTISTS = [
    "Drake", "Taylor Swift", "Morgan Wallen", "SZA", "The Weeknd",
    "Bad Bunny", "Metro Boomin", "Doja Cat", "Luke Combs", "Olivia Rodrigo"
]

def get_top_artists_us(token):
    headers = get_auth_header(token)
    url = "https://api.spotify.com/v1/search"
    artist_data = []

    for name in TOP_US_ARTISTS:
        query = f"?q={name}&type=artist&limit=1"
        res = get(url + query, headers=headers)
        items = json.loads(res.content).get("artists", {}).get("items", [])
        if items:
            artist_data.append(items[0])
    return artist_data

#  Add a function to save results to JSON
def save_to_local_json(data, filename="data/raw_artists_tracks.json"):
    # Get the parent directory of the current file's directory
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    full_path = os.path.join(base_dir, filename)

    # Make sure the directory exists
    os.makedirs(os.path.dirname(full_path), exist_ok=True)

    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

# Orchestrate the flow
def main():
    token = get_token()
    top_artists = get_top_artists_us(token)
    artist_ids = [artist["id"] for artist in top_artists]
    top_tracks = get_top_tracks_for_artists(token, artist_ids)
    
    combined = {
        "top_artists": top_artists,
        "top_tracks": top_tracks
    }

    save_to_local_json(combined, filename="data/raw_artists_tracks.json")

if __name__ == "__main__":
    main()
