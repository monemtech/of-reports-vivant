#!/usr/bin/env python3
"""
Depositphotos stock image sourcing
==================================
Pulls brand-safe stock images for a video package: searches Depositphotos,
downloads watermarked previews for review, writes a picks sheet with page
links, and (optionally) licenses full-resolution files with your account.

Setup:
  1. DP_API_KEY — Depositphotos issues API keys on request (they are not
     self-serve): log in, then ask via https://depositphotos.com/api-suite.html
     or support. Search + previews work with the key alone.
  2. DP_USERNAME / DP_PASSWORD — only needed for --license (uses your
     account's subscription/on-demand downloads).

Usage:
  # Source images for a whole package produced by producer.py
  python stock_images.py --package deliverables/5-ai-side-hustles

  # Ad-hoc search
  python stock_images.py --query "small business owner laptop" --limit 8

  # License chosen images full-res into the package
  python stock_images.py --package deliverables/5-ai-side-hustles --license 12345678 87654321
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

import requests

API_URL = "https://api.depositphotos.com"

# Never pull images that create brand or privacy problems for a client:
# competitor logos/brands and identifiable-person close-ups are filtered by
# title keywords, and previews are for internal review only until licensed.
DEFAULT_EXCLUDE = ["logo", "trademark", "editorial"]

PREVIEW_KEYS = ["url_big", "url_max_qa", "url2", "thumb_max", "thumbnail", "thumb"]


def api_call(params: dict) -> dict:
    api_key = os.environ.get("DP_API_KEY")
    if not api_key:
        sys.exit(
            "DP_API_KEY is not set. Depositphotos issues API keys on request —\n"
            "ask via https://depositphotos.com/api-suite.html with your account,\n"
            "then: export DP_API_KEY=..."
        )
    resp = requests.post(API_URL, data={"dp_apikey": api_key, **params}, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if data.get("type") == "error":
        sys.exit(f"Depositphotos API error on {params.get('dp_command')}: {data}")
    return data


def search(query: str, limit: int, exclude: list[str]) -> list[dict]:
    data = api_call(
        {
            "dp_command": "search",
            "dp_search_query": query,
            "dp_search_photo": "true",
            "dp_search_limit": limit * 2,  # headroom for the exclude filter
            "dp_search_offset": 0,
        }
    )
    items = data.get("result", data.get("items", []))
    kept = []
    for item in items:
        title = (item.get("title") or "").lower()
        if any(term.lower() in title for term in exclude):
            continue
        kept.append(item)
        if len(kept) >= limit:
            break
    return kept


def preview_url(item: dict) -> str | None:
    for key in PREVIEW_KEYS:
        if item.get(key):
            return item[key]
    return None


def page_url(item: dict) -> str:
    return item.get("itemurl") or f"https://depositphotos.com/{item.get('id')}/stock-photo.html"


def download(url: str, dest: Path) -> bool:
    resp = requests.get(url, timeout=120)
    if resp.status_code != 200:
        return False
    dest.write_bytes(resp.content)
    return True


def fetch_for_queries(plans: list[dict], out_dir: Path, per_query: int, exclude: list[str]) -> None:
    """plans: [{section, search_query, placement}] -> previews + picks.md"""
    preview_dir = out_dir / "preview"
    preview_dir.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Image picks — review before licensing",
        "",
        "Previews are watermarked and for internal review only. License your",
        "picks with `--license <id> <id> ...` (uses your Depositphotos account)",
        "or by opening the page links with your account.",
        "",
    ]
    for plan in plans:
        query = plan["search_query"]
        print(f"Searching: {query}")
        items = search(query, per_query, exclude)
        lines += [f"## {plan.get('section', query)}", f"*Query:* `{query}` · *Placement:* {plan.get('placement', '-')}", ""]
        if not items:
            lines += ["No results — try a broader query.", ""]
            continue
        for item in items:
            media_id = item.get("id")
            url = preview_url(item)
            saved = ""
            if url:
                slug = re.sub(r"[^a-z0-9]+", "-", plan.get("section", "img").lower())[:30]
                dest = preview_dir / f"{slug}-{media_id}{Path(url).suffix or '.jpg'}"
                if download(url, dest):
                    saved = f" · preview: `{dest.name}`"
            lines.append(f"- **{media_id}** — {item.get('title', 'untitled')} — [page]({page_url(item)}){saved}")
        lines.append("")
    (out_dir / "picks.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {out_dir / 'picks.md'} and previews to {preview_dir}/")


def license_items(media_ids: list[str], out_dir: Path, option: str) -> None:
    user = os.environ.get("DP_USERNAME")
    password = os.environ.get("DP_PASSWORD")
    if not (user and password):
        sys.exit("--license needs DP_USERNAME and DP_PASSWORD set (your Depositphotos account).")
    session = api_call({"dp_command": "login", "dp_login_user": user, "dp_login_password": password})
    session_id = session.get("sessionid")
    licensed_dir = out_dir / "licensed"
    licensed_dir.mkdir(parents=True, exist_ok=True)
    for media_id in media_ids:
        data = api_call(
            {
                "dp_command": "getMedia",
                "dp_session_id": session_id,
                "dp_media_id": media_id,
                "dp_media_option": option,
                "dp_media_license": "standard",
            }
        )
        url = data.get("downloadLink") or data.get("url") or (data.get("result") or {}).get("downloadLink")
        if not url:
            print(f"  {media_id}: no download link in response — raw: {json.dumps(data)[:300]}")
            continue
        dest = licensed_dir / f"{media_id}.jpg"
        if download(url, dest):
            print(f"  licensed {media_id} -> {dest}")
        else:
            print(f"  {media_id}: download failed from {url}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Source brand-safe stock images from Depositphotos.")
    parser.add_argument("--package", help="a deliverables/<slug> folder from producer.py")
    parser.add_argument("--query", help="ad-hoc search instead of a package image plan")
    parser.add_argument("--limit", type=int, default=5, help="images per query")
    parser.add_argument("--exclude", default=",".join(DEFAULT_EXCLUDE), help="comma-separated title terms to skip")
    parser.add_argument("--license", nargs="*", metavar="MEDIA_ID", help="license these image IDs full-res")
    parser.add_argument("--size", default="xl", help="license size option (s/m/l/xl)")
    args = parser.parse_args()

    exclude = [t for t in args.exclude.split(",") if t.strip()]

    if args.package:
        pkg = Path(args.package)
        out_dir = pkg / "images"
        if args.license:
            license_items(args.license, out_dir, args.size)
            return
        plan_path = pkg / "image_plan.json"
        if not plan_path.exists():
            sys.exit(f"{plan_path} not found — regenerate the package with the current producer.py")
        plans = json.loads(plan_path.read_text(encoding="utf-8"))
        fetch_for_queries(plans, out_dir, args.limit, exclude)
    elif args.query:
        out_dir = Path("stock_search")
        if args.license:
            license_items(args.license, out_dir, args.size)
            return
        fetch_for_queries([{"section": args.query, "search_query": args.query, "placement": "-"}], out_dir, args.limit, exclude)
    else:
        sys.exit("Pass --package deliverables/<slug> or --query '...'")


if __name__ == "__main__":
    main()
