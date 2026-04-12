import spotipy
from spotipy.oauth2 import SpotifyOAuth
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
import json
import time
from groq import Groq

load_dotenv()

groq_client = Groq(api_key=os.getenv("GROQ_API_KEY"))


# Spotify connection
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=os.getenv("SPOTIFY_CLIENT_ID"),
    client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
    redirect_uri="http://127.0.0.1:8888/callback",
    scope="user-read-recently-played user-read-currently-playing"
))

# Kafka connection
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

def get_audio_features(track_name, artist_name):
    try:
        response = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {
                    "role": "user",
                    "content": f"""Estimate the mood of this song and return ONLY a JSON object, nothing else. No explanation, no markdown, no backticks.
                    Song: {track_name}
                    Artist: {artist_name}

Return exactly this format:
{{"valence": 0.0, "energy": 0.0, "danceability": 0.0, "tempo": 0.0}}

valence: 0.0 (very sad) to 1.0 (very happy)
energy: 0.0 (very calm) to 1.0 (very intense)
danceability: 0.0 (not danceable) to 1.0 (very danceable)
tempo: beats per minute as a float between 60.0 and 180.0"""
                }
            ],
            temperature=0.1
        )
        text = response.choices[0].message.content.strip()
        features = json.loads(text)
        print(f"Mood estimated → valence: {features['valence']} energy: {features['energy']}")
        return features
    except Exception as e:
        print(f"Error estimating mood: {e}")
        return {"valence": 0.5, "energy": 0.5, "danceability": 0.5, "tempo": 100.0}


def get_currently_playing():
    try:
        result = sp.currently_playing()
        if result and result["is_playing"]:
            track = result["item"]
            return {
                "track_id": track["id"],
                "track_name": track["name"],
                "artist": track["artists"][0]["name"],
                "album": track["album"]["name"],
                "duration_ms": track["duration_ms"],
                "timestamp": time.time(),
                "source": "currently_playing"
            }
    except Exception as e:
        print(f"Error fetching currently playing: {e}")
    return None

def get_recently_played():
    try:
        results = sp.current_user_recently_played(limit=1)
        if results["items"]:
            item = results["items"][0]
            track = item["track"]
            
            # Check if this track was played in the last 10 minutes
            played_at = item["played_at"]  # format: "2026-04-08T10:30:00.000Z"
            from datetime import datetime, timezone
            played_time = datetime.strptime(played_at, "%Y-%m-%dT%H:%M:%S.%fZ")
            played_time = played_time.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            minutes_ago = (now - played_time).total_seconds() / 60

            if minutes_ago > 10:
                print(f"Recently played track is {minutes_ago:.1f} min old — skipping")
                return None

            return {
                "track_id": track["id"],
                "track_name": track["name"],
                "artist": track["artists"][0]["name"],
                "album": track["album"]["name"],
                "duration_ms": track["duration_ms"],
                "timestamp": time.time(),
                "source": "recently_played"
            }
    except Exception as e:
        print(f"Error fetching recently played: {e}")
    return None

def publish_to_kafka(event):
    producer.send("spotify-plays", value=event)
    producer.flush()
    print(f"Published: {event['track_name']} by {event['artist']}")

print("Producer started. Polling Spotify every 30 seconds...")

def load_last_track_id():
    try:
        with open(".last_track", "r") as f:
            return f.read().strip()
    except:
        return None

def save_last_track_id(track_id):
    with open(".last_track", "w") as f:
        f.write(track_id)

last_track_id = load_last_track_id()

while True:
    event = get_currently_playing()

    if event is None:
        event = get_recently_played()

    if event and event["track_id"] != last_track_id:
        audio = get_audio_features(event["track_name"], event["artist"])
        event.update(audio)
        publish_to_kafka(event)
        last_track_id = event["track_id"]
        save_last_track_id(event["track_id"])
    else:
        print("No new track detected.")

    time.sleep(30)