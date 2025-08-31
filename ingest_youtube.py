#!/usr/bin/env python3
import os
import sys
import time
import argparse
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from googleapiclient.discovery import build

def get_youtube():
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        sys.exit("ERROR: YOUTUBE_API_KEY not set (in env or .env).")
    return build("youtube", "v3", developerKey=api_key)

def resolve_channel_id(youtube, channel_id: str | None, channel_query: str | None) -> str:
    if channel_id:
        return channel_id
    if not channel_query:
        sys.exit("ERROR: Provide either --channel-id or --channel-query.")
    resp = youtube.search().list(
        q=channel_query, part="snippet", type="channel", maxResults=1
    ).execute()
    items = resp.get("items", [])
    if not items:
        sys.exit(f"ERROR: No channel found for query: {channel_query}")
    return items[0]["snippet"]["channelId"]

def list_recent_video_ids(youtube, channel_id: str, max_results: int = 25) -> list[str]:
    video_ids: list[str] = []
    page_token = None
    while len(video_ids) < max_results:
        req = youtube.search().list(
            part="id",
            channelId=channel_id,
            type="video",
            order="date",
            maxResults=min(50, max_results - len(video_ids)),
            pageToken=page_token,
        )
        resp = req.execute()
        for it in resp.get("items", []):
            vid = it.get("id", {}).get("videoId")
            if vid:
                video_ids.append(vid)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
        time.sleep(0.15)  # gentle pacing
    return video_ids

def fetch_videos_batch(youtube, video_ids: list[str]) -> list[dict]:
    rows: list[dict] = []
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i+50]
        resp = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(chunk),
            maxResults=50
        ).execute()
        for it in resp.get("items", []):
            snip = it.get("snippet", {})
            stats = it.get("statistics", {})
            cdet = it.get("contentDetails", {})
            rows.append({
                "video_id": it.get("id"),
                "channel_id": snip.get("channelId"),
                "channel_title": snip.get("channelTitle"),
                "title": snip.get("title"),
                "description": snip.get("description"),
                "published_at": snip.get("publishedAt"),
                "duration": cdet.get("duration"),  # ISO8601, parse in dbt later
                "tags": "|".join(snip.get("tags", [])) if snip.get("tags") else None,
                "default_language": snip.get("defaultLanguage"),
                "view_count": _to_int(stats.get("viewCount")),
                "like_count": _to_int(stats.get("likeCount")),
                "comment_count": _to_int(stats.get("commentCount")),
            })
        time.sleep(0.15)
    return rows

def _to_int(v):
    try:
        return int(v) if v is not None else None
    except Exception:
        return None

def to_dataframe(rows: list[dict]) -> pd.DataFrame:
    cols = [
        "video_id","channel_id","channel_title","title","description",
        "published_at","duration","tags","default_language",
        "view_count","like_count","comment_count"
    ]
    df = pd.DataFrame(rows)
    return df.reindex(columns=cols)

def save_csv(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"✅ Saved {len(df)} rows → {path}")

# ---------- Main ----------
def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="Ingest recent YouTube videos → CSV")
    parser.add_argument("--channel-id", default=os.getenv("CHANNEL_ID"))
    parser.add_argument("--channel-query", default=None, help="Search phrase to find channel if ID unknown")
    parser.add_argument("--max-results", type=int, default=int(os.getenv("MAX_RESULTS", "25")))
    parser.add_argument("--out", default="data/raw/youtube_videos.csv")
    args = parser.parse_args()

    yt = get_youtube()
    ch_id = resolve_channel_id(yt, args.channel_id, args.channel_query)
    print(f"ℹ️ Channel ID: {ch_id}")

    vids = list_recent_video_ids(yt, ch_id, args.max_results)
    if not vids:
        sys.exit("No videos found.")
    print(f"ℹ️ Fetching metadata for {len(vids)} videos...")

    rows = fetch_videos_batch(yt, vids)
    df = to_dataframe(rows)
    save_csv(df, Path(args.out))
    print("🎉 Ingestion complete.")

if __name__ == "__main__":
    main()
