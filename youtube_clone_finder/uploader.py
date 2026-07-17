#!/usr/bin/env python3
"""
YouTube upload & scheduling
===========================
Uploads a finished video to YouTube with title/description/tags, an optional
custom thumbnail, and an optional scheduled publish time. This is the
"Upload & Scheduling" deliverable: videos go up as private and flip to
public at the scheduled time.

One-time OAuth setup (per channel):
  1. console.cloud.google.com -> your project -> enable "YouTube Data API v3".
  2. Credentials -> Create OAuth client ID -> type "Web application" ->
     add https://developers.google.com/oauthplayground to redirect URIs.
  3. Go to https://developers.google.com/oauthplayground -> gear icon ->
     "Use your own OAuth credentials" -> paste client ID + secret.
  4. Authorize scope: https://www.googleapis.com/auth/youtube.upload
     (sign in with the CHANNEL's Google account) -> "Exchange authorization
     code for tokens" -> copy the refresh token.
  5. export YT_CLIENT_ID=... YT_CLIENT_SECRET=... YT_REFRESH_TOKEN=...

Quota note: each upload costs ~1600 of the 10,000 free daily units, so
plan at most ~6 uploads/day per API project.

Usage:
  python uploader.py --video final.mp4 --title "5 AI Side Hustles That Pay" \\
      --description-file deliverables/<slug>/metadata.json \\
      --thumbnail thumb.png --publish-at 2026-07-20T15:00:00Z
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import os
import sys
from pathlib import Path

import requests

TOKEN_URL = "https://oauth2.googleapis.com/token"
UPLOAD_URL = "https://www.googleapis.com/upload/youtube/v3/videos"
THUMB_URL = "https://www.googleapis.com/upload/youtube/v3/thumbnails/set"
CHUNK_SIZE = 8 * 1024 * 1024


def get_access_token() -> str:
    client_id = os.environ.get("YT_CLIENT_ID")
    client_secret = os.environ.get("YT_CLIENT_SECRET")
    refresh_token = os.environ.get("YT_REFRESH_TOKEN")
    if not all([client_id, client_secret, refresh_token]):
        sys.exit(
            "Missing YT_CLIENT_ID / YT_CLIENT_SECRET / YT_REFRESH_TOKEN.\n"
            "See the one-time OAuth setup steps at the top of this file."
        )
    resp = requests.post(
        TOKEN_URL,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    if resp.status_code != 200:
        sys.exit(f"Token refresh failed ({resp.status_code}): {resp.text}\nRe-do the OAuth setup.")
    return resp.json()["access_token"]


def load_metadata(args: argparse.Namespace) -> tuple[str, list[str]]:
    """Description and tags, either from flags or from a producer metadata.json."""
    description, tags = args.description or "", args.tags.split(",") if args.tags else []
    if args.description_file:
        path = Path(args.description_file)
        if path.suffix == ".json":
            meta = json.loads(path.read_text(encoding="utf-8"))
            description = meta.get("description", description)
            tags = meta.get("tags", tags)
        else:
            description = path.read_text(encoding="utf-8")
    return description, tags


def upload_video(token: str, args: argparse.Namespace, description: str, tags: list[str]) -> str:
    status: dict = {"privacyStatus": "private" if args.publish_at else args.privacy}
    if args.publish_at:
        status["publishAt"] = args.publish_at
    status["selfDeclaredMadeForKids"] = args.made_for_kids
    body = {
        "snippet": {
            "title": args.title,
            "description": description,
            "tags": tags,
            "categoryId": args.category,
        },
        "status": status,
    }
    video_path = Path(args.video)
    size = video_path.stat().st_size

    start = requests.post(
        f"{UPLOAD_URL}?uploadType=resumable&part=snippet,status",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=UTF-8",
            "X-Upload-Content-Length": str(size),
            "X-Upload-Content-Type": "video/*",
        },
        json=body,
        timeout=60,
    )
    if start.status_code != 200:
        sys.exit(f"Upload session failed ({start.status_code}): {start.text}")
    session_url = start.headers["Location"]

    with video_path.open("rb") as fh:
        sent = 0
        while sent < size:
            chunk = fh.read(CHUNK_SIZE)
            end = sent + len(chunk) - 1
            resp = requests.put(
                session_url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Length": str(len(chunk)),
                    "Content-Range": f"bytes {sent}-{end}/{size}",
                },
                data=chunk,
                timeout=300,
            )
            sent = end + 1
            print(f"  uploaded {sent / size:.0%}", flush=True)
            if resp.status_code in (200, 201):
                return resp.json()["id"]
            if resp.status_code != 308:
                sys.exit(f"Chunk upload failed ({resp.status_code}): {resp.text}")
    sys.exit("Upload ended without a completed response.")


def set_thumbnail(token: str, video_id: str, thumb_path: str) -> None:
    mime = mimetypes.guess_type(thumb_path)[0] or "image/png"
    resp = requests.post(
        f"{THUMB_URL}?videoId={video_id}",
        headers={"Authorization": f"Bearer {token}", "Content-Type": mime},
        data=Path(thumb_path).read_bytes(),
        timeout=120,
    )
    if resp.status_code != 200:
        print(f"  thumbnail failed ({resp.status_code}): {resp.text} — set it manually in Studio")


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload and schedule a YouTube video.")
    parser.add_argument("--video", required=True, help="path to the video file")
    parser.add_argument("--title", required=True)
    parser.add_argument("--description", default="")
    parser.add_argument("--description-file", help="metadata.json from producer.py, or a plain text file")
    parser.add_argument("--tags", default="", help="comma-separated (ignored if metadata.json has tags)")
    parser.add_argument("--thumbnail", help="path to a thumbnail image (1280x720)")
    parser.add_argument("--publish-at", help="RFC3339 UTC time, e.g. 2026-07-20T15:00:00Z (schedules the video)")
    parser.add_argument("--privacy", choices=["public", "private", "unlisted"], default="private")
    parser.add_argument("--category", default="27", help="27=Education, 22=People&Blogs, 28=Sci&Tech")
    parser.add_argument("--made-for-kids", action="store_true", help="REQUIRED for kids content (COPPA)")
    args = parser.parse_args()

    token = get_access_token()
    description, tags = load_metadata(args)
    print(f"Uploading {args.video} ...")
    video_id = upload_video(token, args, description, tags)
    print(f"Uploaded: https://youtube.com/watch?v={video_id}")
    if args.thumbnail:
        set_thumbnail(token, video_id, args.thumbnail)
        print("Thumbnail set.")
    if args.publish_at:
        print(f"Scheduled to go public at {args.publish_at} (UTC).")


if __name__ == "__main__":
    main()
