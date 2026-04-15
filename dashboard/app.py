import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Spotify Listening Intelligence",
    page_icon="🎵",
    layout="wide"
)

def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        database=os.getenv("POSTGRES_DB", "spotify_db"),
        user=os.getenv("POSTGRES_USER", "spotify_user"),
        password=os.getenv("POSTGRES_PASS", "spotify_pass")
    )

def query(sql):
    try:
        conn = get_connection()
        df = pd.read_sql(sql, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()

st.title("🎵 Spotify Listening Intelligence")
st.caption(f"Live pipeline · Last refreshed: {datetime.now().strftime('%H:%M:%S')} IST")

latest = query("""
    SELECT track_name, artist, album, mood_label, valence, energy, played_at
    FROM tracks
    ORDER BY played_at DESC
    LIMIT 1
""")

if not latest.empty:
    row = latest.iloc[0]
    mood_colors = {
        "Happy": "🟡", "Positive": "🟢",
        "Neutral": "🔵", "Melancholic": "🟠", "Sad": "🔴"
    }
    emoji = mood_colors.get(row["mood_label"], "⚪")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Last Played", row["track_name"][:30])
    with col2:
        st.metric("Artist", row["artist"][:25])
    with col3:
        st.metric("Mood", f"{emoji} {row['mood_label']}")
    with col4:
        st.metric("Valence", f"{row['valence']:.2f}")

st.divider()

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Mood over time")
    mood_df = query("""
        SELECT played_at, valence, energy, track_name, artist, mood_label
        FROM tracks
        ORDER BY played_at ASC
    """)
    if not mood_df.empty:
        mood_df["played_at"] = pd.to_datetime(mood_df["played_at"]).dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=mood_df["played_at"], y=mood_df["valence"],
            mode="lines+markers", name="Valence (happiness)",
            line=dict(color="#1DB954", width=2),
            hovertemplate="<b>%{customdata[0]}</b><br>%{customdata[1]}<br>Valence: %{y:.2f}<extra></extra>",
            customdata=mood_df[["track_name", "artist"]].values
        ))
        fig.add_trace(go.Scatter(
            x=mood_df["played_at"], y=mood_df["energy"],
            mode="lines+markers", name="Energy",
            line=dict(color="#FF6B6B", width=2, dash="dot"),
            hovertemplate="Energy: %{y:.2f}<extra></extra>"
        ))
        fig.add_hrect(y0=0.7, y1=1.0, fillcolor="#1DB954",
                      opacity=0.05, line_width=0, annotation_text="Happy zone")
        fig.add_hrect(y0=0.0, y1=0.3, fillcolor="#FF6B6B",
                      opacity=0.05, line_width=0, annotation_text="Sad zone")
        fig.update_layout(
            height=300, margin=dict(l=0, r=0, t=10, b=0),
            legend=dict(orientation="h", y=1.1),
            xaxis_title=None, yaxis_title="Score (0-1)",
            yaxis=dict(range=[0, 1])
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data yet — play some music!")

with col2:
    st.subheader("Top artists")
    artists_df = query("""
        SELECT artist, COUNT(*) as plays
        FROM tracks
        GROUP BY artist
        ORDER BY plays DESC
        LIMIT 8
    """)
    if not artists_df.empty:
        fig = px.bar(
            artists_df, x="plays", y="artist",
            orientation="h", color="plays",
            color_continuous_scale="Greens"
        )
        fig.update_layout(
            height=300, margin=dict(l=0, r=0, t=10, b=0),
            showlegend=False, coloraxis_showscale=False,
            xaxis_title="Plays", yaxis_title=None
        )
        fig.update_yaxes(autorange="reversed")
        st.plotly_chart(fig, use_container_width=True)

col1, col2 = st.columns(2)

with col1:
    st.subheader("Mood distribution")
    mood_dist = query("""
        SELECT mood_label, COUNT(*) as count
        FROM tracks
        GROUP BY mood_label
        ORDER BY count DESC
    """)
    if not mood_dist.empty:
        color_map = {
            "Happy": "#FFD700", "Positive": "#1DB954",
            "Neutral": "#4A90D9", "Melancholic": "#FF8C00", "Sad": "#E74C3C"
        }
        fig = px.pie(
            mood_dist, values="count", names="mood_label",
            color="mood_label", color_discrete_map=color_map,
            hole=0.4
        )
        fig.update_layout(height=280, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Peak listening hours (IST)")
    hours_df = query("""
        SELECT EXTRACT(HOUR FROM played_at + INTERVAL '5 hours 30 minutes') AS hour,
               COUNT(*) as plays,
               AVG(valence) as avg_mood
        FROM tracks
        GROUP BY hour
        ORDER BY hour
    """)
    if not hours_df.empty:
        hours_df["hour"] = hours_df["hour"].astype(int)
        fig = px.bar(
            hours_df, x="hour", y="plays",
            color="avg_mood", color_continuous_scale="RdYlGn",
            labels={"hour": "Hour of day (IST)", "plays": "Tracks played",
                    "avg_mood": "Avg mood"}
        )
        fig.update_layout(height=280, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, use_container_width=True)

st.subheader("Recent tracks")
recent = query("""
    SELECT played_at, track_name, artist, album,
           mood_label, valence, energy, source
    FROM tracks
    ORDER BY played_at DESC
    LIMIT 20
""")
if not recent.empty:
    recent["played_at"] = pd.to_datetime(recent["played_at"]).dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata').dt.strftime("%d %b %H:%M IST")
    recent["valence"] = recent["valence"].round(2)
    recent["energy"] = recent["energy"].round(2)
    st.dataframe(
        recent.rename(columns={
            "played_at": "Time (IST)", "track_name": "Track",
            "artist": "Artist", "album": "Album",
            "mood_label": "Mood", "valence": "Valence",
            "energy": "Energy", "source": "Source"
        }),
        use_container_width=True,
        hide_index=True
    )

st.divider()
stats = query("""
    SELECT COUNT(*) as total_tracks,
           COUNT(DISTINCT artist) as unique_artists,
           AVG(valence) as avg_valence,
           AVG(energy) as avg_energy
    FROM tracks
""")
if not stats.empty:
    s = stats.iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total tracks logged", int(s["total_tracks"]))
    c2.metric("Unique artists", int(s["unique_artists"]))
    c3.metric("Average mood", f"{s['avg_valence']:.2f}")
    c4.metric("Average energy", f"{s['avg_energy']:.2f}")

time.sleep(30)
st.rerun()