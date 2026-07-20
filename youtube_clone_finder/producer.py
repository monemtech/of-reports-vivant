#!/usr/bin/env python3
"""
Video package + channel brief producer
======================================
Generates the sellable deliverables of a YouTube automation service using
the Claude API:

  --mode package  (default)  A complete video package for one topic:
      hook-based high-retention script, 3 title options, SEO description,
      tags, a CTR-focused thumbnail brief, and a ready-to-paste Invideo
      production prompt.

  --mode channel  A channel launch kit for a niche:
      channel name options, positioning statement, banner/logo briefs,
      upload schedule, 30 video ideas mapped to search queries, and a
      monetization plan (the "growth blueprint" deliverable).

Setup:
  pip install anthropic
  export ANTHROPIC_API_KEY="sk-ant-..."   (or `ant auth login`)

Usage:
  python producer.py --topic "5 AI side hustles that actually pay" --niche finance
  python producer.py --mode channel --niche kids_rhymes
  python producer.py --topic "best credit cards 2026" --niche finance --minutes 10
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import anthropic

MODEL = "claude-opus-4-8"

PACKAGE_SCHEMA = {
    "type": "object",
    "properties": {
        "script": {
            "type": "string",
            "description": "Full narration script in markdown. Hook in first "
            "2 sentences, open loops for retention, concrete numbers, "
            "worst-to-best or curiosity structure, subscribe CTA that tees "
            "up the next video.",
        },
        "titles": {
            "type": "array",
            "items": {"type": "string"},
            "description": "3 title options under 60 chars, curiosity-driven, no clickbait lies",
        },
        "description": {
            "type": "string",
            "description": "YouTube description: 2-line hook, chapter timestamps, 3-5 hashtags",
        },
        "tags": {"type": "array", "items": {"type": "string"}},
        "thumbnail_brief": {
            "type": "object",
            "properties": {
                "text_overlay": {"type": "string", "description": "3-5 words max"},
                "visual": {"type": "string"},
                "style_notes": {"type": "string"},
            },
            "required": ["text_overlay", "visual", "style_notes"],
            "additionalProperties": False,
        },
        "invideo_prompt": {
            "type": "string",
            "description": "One-paragraph production brief for an AI video "
            "generator: visual style, pacing, voiceover tone, b-roll guidance",
        },
        "image_plan": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "section": {"type": "string", "description": "Which part of the script this covers"},
                    "search_query": {
                        "type": "string",
                        "description": "Stock-photo search query. Brand-safe: "
                        "generic concepts only, no brand names, no competitor "
                        "products, no identifiable individuals",
                    },
                    "placement": {"type": "string", "description": "How/where the image is used on screen"},
                },
                "required": ["section", "search_query", "placement"],
                "additionalProperties": False,
            },
            "description": "5-8 stock image searches covering the script's sections plus one for the thumbnail",
        },
    },
    "required": [
        "script", "titles", "description", "tags", "thumbnail_brief",
        "invideo_prompt", "image_plan",
    ],
    "additionalProperties": False,
}

CHANNEL_SCHEMA = {
    "type": "object",
    "properties": {
        "channel_names": {"type": "array", "items": {"type": "string"}},
        "positioning": {"type": "string", "description": "One paragraph: who it's for, the angle, why subscribe"},
        "banner_brief": {"type": "string"},
        "logo_brief": {"type": "string"},
        "upload_schedule": {"type": "string"},
        "video_ideas": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "format": {"type": "string"},
                    "target_query": {"type": "string", "description": "The search demand this video captures"},
                },
                "required": ["title", "format", "target_query"],
                "additionalProperties": False,
            },
            "description": "30 video ideas for the first 2 months",
        },
        "monetization_plan": {
            "type": "string",
            "description": "Realistic path: YPP requirements timeline, ad RPM "
            "expectations for this niche, affiliate/sponsor opportunities, "
            "and honest milestones - never guarantee revenue",
        },
    },
    "required": [
        "channel_names", "positioning", "banner_brief", "logo_brief",
        "upload_schedule", "video_ideas", "monetization_plan",
    ],
    "additionalProperties": False,
}

SYSTEM = """You produce deliverables for a professional YouTube channel
production service. Non-negotiable rules:

- Everything you write is ORIGINAL. You study what performs in a niche and
  create new material in proven formats - never copy or closely paraphrase
  an existing video.
- Quality clears YouTube's inauthentic-content bar: a real point of view,
  specific numbers and examples, no filler, no generic AI-sounding prose.
- Scripts are written for retention: a hook in the first two sentences that
  creates an open loop, payoffs distributed through the video, pattern
  interrupts every 60-90 seconds, and a CTA that tees up a follow-up video.
- Kids content must be genuinely safe, gentle, and developmentally
  appropriate - COPPA applies and quality expectations are high.
- Never promise or guarantee revenue figures. Give honest ranges with the
  assumptions stated.
- Assume ~150 spoken words per minute when a target length is given."""


def load_niche(niche_key: str) -> dict:
    niches = json.loads((Path(__file__).parent / "niches.json").read_text(encoding="utf-8"))
    if niche_key not in niches:
        sys.exit(f"Unknown niche '{niche_key}'. Available: {', '.join(niches)}")
    return niches[niche_key]


def generate(prompt: str, schema: dict) -> dict:
    client = anthropic.Anthropic()
    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=16000,
            thinking={"type": "adaptive"},
            system=SYSTEM,
            output_config={"format": {"type": "json_schema", "schema": schema}},
            messages=[{"role": "user", "content": prompt}],
        )
    except (anthropic.AuthenticationError, TypeError):
        # TypeError is what the SDK raises when no credential source exists at all
        sys.exit(
            "No Claude API credentials found.\n"
            'Set ANTHROPIC_API_KEY (console.anthropic.com -> API keys) or run "ant auth login".'
        )
    if response.stop_reason == "refusal":
        sys.exit("The model declined this topic. Pick a different one.")
    text = next(b.text for b in response.content if b.type == "text")
    return json.loads(text)


def slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug[:60] or "untitled"


def write_package(result: dict, topic: str, out_root: Path) -> Path:
    out = out_root / slugify(topic)
    out.mkdir(parents=True, exist_ok=True)
    (out / "script.md").write_text(result["script"], encoding="utf-8")
    (out / "metadata.json").write_text(
        json.dumps(
            {"titles": result["titles"], "description": result["description"], "tags": result["tags"]},
            indent=2,
        ),
        encoding="utf-8",
    )
    thumb = result["thumbnail_brief"]
    (out / "thumbnail_brief.md").write_text(
        f"# Thumbnail brief\n\n**Text overlay:** {thumb['text_overlay']}\n\n"
        f"**Visual:** {thumb['visual']}\n\n**Style:** {thumb['style_notes']}\n",
        encoding="utf-8",
    )
    (out / "invideo_prompt.txt").write_text(result["invideo_prompt"], encoding="utf-8")
    (out / "image_plan.json").write_text(json.dumps(result["image_plan"], indent=2), encoding="utf-8")
    return out


def write_channel_kit(result: dict, niche_label: str, out_root: Path) -> Path:
    out = out_root / f"channel-kit-{slugify(niche_label)}"
    out.mkdir(parents=True, exist_ok=True)
    ideas = "\n".join(
        f"| {i + 1} | {v['title']} | {v['format']} | {v['target_query']} |"
        for i, v in enumerate(result["video_ideas"])
    )
    (out / "channel_kit.md").write_text(
        f"# Channel launch kit — {niche_label}\n\n"
        f"## Name options\n" + "\n".join(f"- {n}" for n in result["channel_names"]) + "\n\n"
        f"## Positioning\n{result['positioning']}\n\n"
        f"## Banner brief\n{result['banner_brief']}\n\n"
        f"## Logo brief\n{result['logo_brief']}\n\n"
        f"## Upload schedule\n{result['upload_schedule']}\n\n"
        f"## First 30 videos\n\n| # | Title | Format | Captures demand for |\n|---|---|---|---|\n{ideas}\n\n"
        f"## Monetization plan\n{result['monetization_plan']}\n",
        encoding="utf-8",
    )
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate video packages and channel kits.")
    parser.add_argument("--mode", choices=["package", "channel"], default="package")
    parser.add_argument("--topic", help="video topic (required for --mode package)")
    parser.add_argument("--niche", required=True, help="niche key from niches.json")
    parser.add_argument("--minutes", type=int, default=8, help="target video length")
    parser.add_argument("--angle", default="", help="optional angle, e.g. 'honest, numbers-driven'")
    parser.add_argument("--out", default="deliverables", help="output directory")
    args = parser.parse_args()

    niche = load_niche(args.niche)
    out_root = Path(args.out)

    if args.mode == "package":
        if not args.topic:
            sys.exit("--topic is required for --mode package")
        angle = args.angle or "honest, specific, numbers-driven; the anti-hype take on this topic"
        prompt = (
            f"Create a complete faceless-YouTube video package.\n\n"
            f"Topic: {args.topic}\n"
            f"Niche: {niche['label']} (typical RPM ${niche['rpm_low']:.0f}-${niche['rpm_high']:.0f}; {niche['notes']})\n"
            f"Target length: {args.minutes} minutes (~{args.minutes * 150} words of narration)\n"
            f"Angle: {angle}\n"
        )
        result = generate(prompt, PACKAGE_SCHEMA)
        out = write_package(result, args.topic, out_root)
        print(f"Package written to {out}/")
        print(f"  Titles: {' | '.join(result['titles'])}")
    else:
        prompt = (
            f"Create a channel launch kit for a new faceless YouTube channel.\n\n"
            f"Niche: {niche['label']} (typical RPM ${niche['rpm_low']:.0f}-${niche['rpm_high']:.0f}; {niche['notes']})\n"
            f"Seed demand (proven search queries in this niche): {', '.join(niche['seed_queries'])}\n"
        )
        result = generate(prompt, CHANNEL_SCHEMA)
        out = write_channel_kit(result, niche["label"], out_root)
        print(f"Channel kit written to {out}/")


if __name__ == "__main__":
    main()
