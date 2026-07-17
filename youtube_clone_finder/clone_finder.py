#!/usr/bin/env python3
"""
YouTube Clone Finder
====================
Finds popular, recently-published YouTube videos whose FORMAT you can remake
with AI tools, ranks them by "clone-ability", and shows the revenue math
toward a $10k/month target.

The core signal is the OUTLIER RATIO: a video with 40x more views than its
channel has subscribers proves the topic/format is pulling clicks on its own
merit — not the channel's brand. Those are the formats worth remaking as
original AI-produced videos.

IMPORTANT — what "clone" means here:
  Remake the format and topic with your own original script, voiceover, and
  visuals. Re-uploading or closely copying someone's video violates copyright
  and YouTube's reused-content policy and will get a channel demonetized.
  YouTube's inauthentic-content rules also demonetize mass-produced AI slop,
  so every video still needs an original angle and real production quality.

Setup:
  1. Get a free API key: https://console.cloud.google.com/ -> create project
     -> enable "YouTube Data API v3" -> Credentials -> API key.
     Free quota is 10,000 units/day; one full 4-niche run uses ~2,800 units.
  2. export YOUTUBE_API_KEY="your-key"
  3. pip install requests
  4. python clone_finder.py --niche all

Usage examples:
  python clone_finder.py --niche finance
  python clone_finder.py --niche all --days 60 --top 15
  python clone_finder.py --niche kids_rhymes --min-views 500000
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

API_BASE = "https://www.googleapis.com/youtube/v3"
MONTHLY_TARGET_USD = 10_000

# Fraction of a proven video's traffic a competent remake typically captures.
# Deliberately conservative; used only for the planning math, not the ranking.
CAPTURE_RATE = 0.10

# Title words that suggest a faceless, remakeable format.
FACELESS_POSITIVE = re.compile(
    r"\b(top \d+|best|how to|explained|guide|tips|facts|story|stories|history|"
    r"compilation|mix|songs?|rhymes?|ranked|vs\.?|documentary|motivation|"
    r"rules?|lessons?|ideas|ways to|tutorial|review)\b",
    re.IGNORECASE,
)

# Title words that suggest personality-driven content an AI remake can't replace.
FACELESS_NEGATIVE = re.compile(
    r"\b(vlog|reacts?|reaction|podcast|interview|i tried|i tested|i spent|"
    r"my journey|day in the life|q&a|behind the scenes|face reveal|irl)\b",
    re.IGNORECASE,
)


@dataclass
class Video:
    video_id: str
    title: str
    channel_id: str
    channel_title: str
    niche_key: str
    matched_query: str
    published_at: datetime
    views: int = 0
    likes: int = 0
    comments: int = 0
    duration_seconds: int = 0
    subscribers: int = 0
    # computed
    age_days: float = 0.0
    views_per_day: float = 0.0
    outlier_ratio: float = 0.0
    engagement_pct: float = 0.0
    clone_score: float = 0.0
    est_monthly_rev_low: float = 0.0
    est_monthly_rev_high: float = 0.0

    @property
    def url(self) -> str:
        return f"https://youtube.com/watch?v={self.video_id}"


@dataclass
class NicheResult:
    key: str
    label: str
    rpm_low: float
    rpm_high: float
    notes: str
    videos: list[Video] = field(default_factory=list)


class QuotaTracker:
    """YouTube API quota costs: search.list = 100, videos/channels.list = 1."""

    def __init__(self) -> None:
        self.used = 0

    def charge(self, units: int) -> None:
        self.used += units


def api_get(endpoint: str, params: dict, api_key: str, quota: QuotaTracker, cost: int) -> dict:
    params = {**params, "key": api_key}
    resp = requests.get(f"{API_BASE}/{endpoint}", params=params, timeout=30)
    quota.charge(cost)
    if resp.status_code == 403:
        detail = resp.json().get("error", {}).get("message", "")
        sys.exit(
            f"YouTube API refused the request (403): {detail}\n"
            "Most likely the daily quota is exhausted or the key lacks "
            "YouTube Data API v3 access. Fix it in the Google Cloud console."
        )
    resp.raise_for_status()
    return resp.json()


def parse_iso_duration(duration: str) -> int:
    """PT1H2M30S -> seconds. Live/upcoming videos report P0D -> 0."""
    match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration or "")
    if not match:
        return 0
    hours, minutes, seconds = (int(g) if g else 0 for g in match.groups())
    return hours * 3600 + minutes * 60 + seconds


def search_niche(
    niche_key: str,
    seed_queries: list[str],
    published_after: datetime,
    per_query: int,
    api_key: str,
    quota: QuotaTracker,
) -> dict[str, Video]:
    """Run every seed query and return unique videos keyed by video id."""
    found: dict[str, Video] = {}
    for query in seed_queries:
        data = api_get(
            "search",
            {
                "part": "snippet",
                "q": query,
                "type": "video",
                "order": "viewCount",
                "publishedAfter": published_after.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "maxResults": per_query,
                "relevanceLanguage": "en",
                "safeSearch": "none",
            },
            api_key,
            quota,
            cost=100,
        )
        for item in data.get("items", []):
            vid = item["id"].get("videoId")
            if not vid or vid in found:
                continue
            snippet = item["snippet"]
            found[vid] = Video(
                video_id=vid,
                title=snippet["title"],
                channel_id=snippet["channelId"],
                channel_title=snippet["channelTitle"],
                niche_key=niche_key,
                matched_query=query,
                published_at=datetime.fromisoformat(
                    snippet["publishedAt"].replace("Z", "+00:00")
                ),
            )
    return found


def hydrate_stats(videos: dict[str, Video], api_key: str, quota: QuotaTracker) -> None:
    """Fill in view/like/comment counts, duration, then channel subscriber counts."""
    ids = list(videos)
    for i in range(0, len(ids), 50):
        batch = ids[i : i + 50]
        data = api_get(
            "videos",
            {"part": "statistics,contentDetails", "id": ",".join(batch)},
            api_key,
            quota,
            cost=1,
        )
        for item in data.get("items", []):
            video = videos[item["id"]]
            stats = item.get("statistics", {})
            video.views = int(stats.get("viewCount", 0))
            video.likes = int(stats.get("likeCount", 0))
            video.comments = int(stats.get("commentCount", 0))
            video.duration_seconds = parse_iso_duration(
                item.get("contentDetails", {}).get("duration", "")
            )

    channel_ids = list({v.channel_id for v in videos.values()})
    subs: dict[str, int] = {}
    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i : i + 50]
        data = api_get(
            "channels",
            {"part": "statistics", "id": ",".join(batch)},
            api_key,
            quota,
            cost=1,
        )
        for item in data.get("items", []):
            subs[item["id"]] = int(item.get("statistics", {}).get("subscriberCount", 0))
    for video in videos.values():
        video.subscribers = subs.get(video.channel_id, 0)


def score_video(video: Video, rpm_low: float, rpm_high: float) -> None:
    """Compute the clone-ability score (0-100) and the revenue estimates."""
    now = datetime.now(timezone.utc)
    video.age_days = max((now - video.published_at).total_seconds() / 86400, 1.0)
    video.views_per_day = video.views / video.age_days
    # Floor tiny channels at 1k subs so brand-new channels don't produce
    # absurd ratios from a few hundred views.
    video.outlier_ratio = video.views / max(video.subscribers, 1000)
    video.engagement_pct = (
        100 * (video.likes + 5 * video.comments) / video.views if video.views else 0.0
    )

    # --- clone score components, each 0-100 ---
    # Outlier: 10x subs -> ~66, 100x -> 100. log scale, saturates.
    outlier_pts = min(100.0, 33.3 * math.log10(max(video.outlier_ratio, 0.1) * 10))
    # Momentum: 10k views/day -> ~80. log scale.
    momentum_pts = min(100.0, 20.0 * math.log10(max(video.views_per_day, 1)))
    # Format: does the title read like a faceless, remakeable format?
    format_pts = 50.0
    if FACELESS_POSITIVE.search(video.title):
        format_pts += 35.0
    if FACELESS_NEGATIVE.search(video.title):
        format_pts -= 40.0
    # Duration: 3-20 min is the monetization sweet spot (mid-roll eligible,
    # cheap to produce). Shorts and 1h+ epics score lower.
    if 180 <= video.duration_seconds <= 1200:
        duration_pts = 100.0
    elif video.duration_seconds < 60:
        duration_pts = 30.0
    else:
        duration_pts = 60.0
    engagement_pts = min(100.0, video.engagement_pct * 20)

    video.clone_score = round(
        0.35 * outlier_pts
        + 0.25 * momentum_pts
        + 0.20 * max(format_pts, 0.0)
        + 0.10 * duration_pts
        + 0.10 * engagement_pts,
        1,
    )

    # What a competent remake could earn per month, at the niche's RPM,
    # capturing CAPTURE_RATE of the original's current daily traffic.
    est_monthly_views = video.views_per_day * 30 * CAPTURE_RATE
    video.est_monthly_rev_low = est_monthly_views / 1000 * rpm_low
    video.est_monthly_rev_high = est_monthly_views / 1000 * rpm_high


def videos_needed_for_target(niche: NicheResult, top: list[Video]) -> tuple[int, float]:
    """How many clones like the niche's top performers reach $10k/mo."""
    if not top:
        return 0, 0.0
    sample = top[: min(5, len(top))]
    avg_mid = sum((v.est_monthly_rev_low + v.est_monthly_rev_high) / 2 for v in sample) / len(sample)
    if avg_mid <= 0:
        return 0, 0.0
    return math.ceil(MONTHLY_TARGET_USD / avg_mid), avg_mid


def fmt_num(n: float) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.0f}k"
    return f"{n:.0f}"


def write_csv(results: list[NicheResult], path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "niche", "clone_score", "title", "url", "channel", "views",
                "views_per_day", "subscribers", "outlier_ratio", "engagement_pct",
                "duration_min", "published", "est_monthly_rev_low_usd",
                "est_monthly_rev_high_usd", "matched_query",
            ]
        )
        for niche in results:
            for v in niche.videos:
                writer.writerow(
                    [
                        niche.key, v.clone_score, v.title, v.url, v.channel_title,
                        v.views, round(v.views_per_day), v.subscribers,
                        round(v.outlier_ratio, 2), round(v.engagement_pct, 2),
                        round(v.duration_seconds / 60, 1),
                        v.published_at.date().isoformat(),
                        round(v.est_monthly_rev_low, 2),
                        round(v.est_monthly_rev_high, 2), v.matched_query,
                    ]
                )


def write_markdown(results: list[NicheResult], path: Path, days: int, top_n: int) -> None:
    lines = [
        "# YouTube Clone Finder — report",
        f"Generated {datetime.now(timezone.utc).date().isoformat()} · window: last {days} days · "
        f"capture assumption: {CAPTURE_RATE:.0%} of the original's daily traffic",
        "",
        "**Clone score** blends outlier ratio (views vs channel subscribers), daily momentum, "
        "faceless-format fit, monetizable duration, and engagement. Higher = the format itself "
        "is pulling clicks and an original AI remake can compete for the same demand.",
        "",
        "> Remake the format with an original script/voice/visuals. Never re-upload or closely "
        "> copy — reused and mass-produced content gets demonetized.",
        "",
    ]
    for niche in results:
        needed, avg_mid = videos_needed_for_target(niche, niche.videos)
        lines += [f"## {niche.label}", "", niche.notes, ""]
        if needed:
            lines.append(
                f"**Path to $10k/mo:** top clone candidates here average "
                f"~${avg_mid:,.0f}/mo each (RPM ${niche.rpm_low:.0f}–${niche.rpm_high:.0f}, "
                f"{CAPTURE_RATE:.0%} capture) → roughly **{needed} performing videos** to reach "
                f"${MONTHLY_TARGET_USD:,}/mo from ads alone. Affiliate/sponsor revenue "
                "(strongest in finance and AI tools) lowers that number substantially."
            )
        lines += [
            "",
            "| Score | Video | Views | Views/day | Subs | Outlier | Est. $/mo (clone) |",
            "|---|---|---|---|---|---|---|",
        ]
        for v in niche.videos[:top_n]:
            lines.append(
                f"| {v.clone_score} | [{v.title[:70]}]({v.url}) | {fmt_num(v.views)} "
                f"| {fmt_num(v.views_per_day)} | {fmt_num(v.subscribers)} "
                f"| {v.outlier_ratio:.1f}x | ${v.est_monthly_rev_low:,.0f}–"
                f"${v.est_monthly_rev_high:,.0f} |"
            )
        lines.append("")
    lines += [
        "## Before you publish",
        "- Monetization requires the YouTube Partner Program: 1,000 subscribers + 4,000 public "
        "watch hours (or 10M Shorts views) — budget 2–4 months of consistent uploads first.",
        "- YouTube's inauthentic-content policy demonetizes mass-produced/repetitious AI content. "
        "Every video needs an original script, angle, and real editing polish.",
        "- Kids content is 'Made for Kids' under COPPA: personalized ads and comments are "
        "disabled, so RPM collapses to ~$0.50–2. It only works at enormous view volume.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def print_console(results: list[NicheResult], top_n: int) -> None:
    for niche in results:
        needed, avg_mid = videos_needed_for_target(niche, niche.videos)
        print(f"\n{'=' * 78}\n{niche.label}  (RPM ${niche.rpm_low:.0f}–${niche.rpm_high:.0f})")
        if needed:
            print(
                f"  ~${avg_mid:,.0f}/mo per clone → ~{needed} videos for "
                f"${MONTHLY_TARGET_USD:,}/mo (ads only)"
            )
        print("=" * 78)
        for i, v in enumerate(niche.videos[:top_n], 1):
            print(
                f"{i:2}. [{v.clone_score:5.1f}] {v.title[:62]}\n"
                f"      {fmt_num(v.views)} views · {fmt_num(v.views_per_day)}/day · "
                f"{v.outlier_ratio:.1f}x channel subs · "
                f"est ${v.est_monthly_rev_low:,.0f}–${v.est_monthly_rev_high:,.0f}/mo\n"
                f"      {v.url}"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Find AI-clonable popular YouTube videos.")
    parser.add_argument("--niche", default="all", help="niche key from niches.json, or 'all'")
    parser.add_argument("--days", type=int, default=30, help="only videos published in the last N days")
    parser.add_argument("--per-query", type=int, default=15, help="results fetched per seed query (max 50)")
    parser.add_argument("--min-views", type=int, default=50_000, help="ignore videos below this view count")
    parser.add_argument("--top", type=int, default=10, help="rows shown per niche in console/markdown")
    parser.add_argument("--out", default="clone_report", help="output file basename (.csv + .md)")
    args = parser.parse_args()

    api_key = os.environ.get("YOUTUBE_API_KEY")
    if not api_key:
        sys.exit(
            "YOUTUBE_API_KEY is not set.\n"
            "Get a free key: console.cloud.google.com -> new project -> enable "
            "'YouTube Data API v3' -> Credentials -> API key, then:\n"
            '  export YOUTUBE_API_KEY="your-key"'
        )

    niches_path = Path(__file__).parent / "niches.json"
    all_niches = json.loads(niches_path.read_text(encoding="utf-8"))
    if args.niche != "all" and args.niche not in all_niches:
        sys.exit(f"Unknown niche '{args.niche}'. Available: {', '.join(all_niches)}, all")
    selected = all_niches if args.niche == "all" else {args.niche: all_niches[args.niche]}

    quota = QuotaTracker()
    published_after = datetime.now(timezone.utc) - timedelta(days=args.days)
    results: list[NicheResult] = []

    for key, cfg in selected.items():
        print(f"Scanning {cfg['label']} ({len(cfg['seed_queries'])} queries)...", flush=True)
        videos = search_niche(key, cfg["seed_queries"], published_after, args.per_query, api_key, quota)
        if videos:
            hydrate_stats(videos, api_key, quota)
        kept = [v for v in videos.values() if v.views >= args.min_views]
        for v in kept:
            score_video(v, cfg["rpm_low"], cfg["rpm_high"])
        kept.sort(key=lambda v: v.clone_score, reverse=True)
        results.append(
            NicheResult(key, cfg["label"], cfg["rpm_low"], cfg["rpm_high"], cfg["notes"], kept)
        )
        print(f"  {len(videos)} found, {len(kept)} above {fmt_num(args.min_views)} views")

    print_console(results, args.top)
    csv_path = Path(f"{args.out}.csv")
    md_path = Path(f"{args.out}.md")
    write_csv(results, csv_path)
    write_markdown(results, md_path, args.days, args.top)
    print(f"\nSaved: {csv_path} and {md_path}")
    print(f"API quota used: ~{quota.used} of 10,000 daily units")


if __name__ == "__main__":
    main()
