import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import os

load_dotenv()

sp=spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=os.getenv("SPOTIFY_CLIENT_ID"),
    client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
    redirect_uri="http://127.0.0.1:8888/callback",
    scope="user-read-recently-played user-read-currently-playing"
))

results=sp.current_user_recently_played(limit=10)
for item in results['items']:
    print(item['track']['name'],'-',item['track']['artists'][0]['name'])