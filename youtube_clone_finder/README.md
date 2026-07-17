# YouTube Clone Finder

Finds popular, recently-published YouTube videos whose **format** you can
remake with AI tools, ranks them by clone-ability, and shows the revenue math
toward a $10k/month target.

## The strategy (read this first)

"Clone" means **remake the format and topic with an original script, AI
voiceover, and AI visuals** — never re-upload or closely copy a video.
Reused content violates copyright and YouTube's monetization policies, and
since 2025 YouTube's *inauthentic content* policy demonetizes mass-produced
AI slop. Original angle + real polish is what monetizes.

The tool's core signal is the **outlier ratio**: a video with 40x more views
than its channel has subscribers proves the *topic/format* is pulling the
clicks, not the channel's brand. Those formats are open territory — a
well-made AI version can compete for the same demand.

## Setup

1. Get a free API key: [console.cloud.google.com](https://console.cloud.google.com)
   → new project → enable **YouTube Data API v3** → Credentials → API key.
   Free tier is 10,000 quota units/day; a full 4-niche run uses ~2,800.
2. ```bash
   export YOUTUBE_API_KEY="your-key"
   pip install requests
   ```

## Usage

```bash
python clone_finder.py --niche all              # scan all four niches
python clone_finder.py --niche finance --top 15 # one niche, more rows
python clone_finder.py --niche kids_rhymes --days 60 --min-views 500000
```

Outputs a ranked console table plus `clone_report.csv` and `clone_report.md`.
Each row shows views, views/day, outlier ratio, and an estimated $/month a
competent clone could earn (niche RPM × 10% traffic capture — conservative).

Niches, seed queries, and RPM assumptions live in `niches.json` — edit freely.

## The four configured niches

| Niche | RPM | Reality check |
|---|---|---|
| Finance & Investing | $15–32 | Highest RPM on YouTube; affiliate revenue (credit cards, brokers) often exceeds ad revenue. Fewest videos needed for $10k/mo. |
| AI & Tech Tools | $10–25 | Fastest-growing faceless category in 2026; demand outpaces creator supply. |
| Motivation / Stoicism / History | $5–12 | Cheapest to produce (AI voice + cinematic AI b-roll); needs volume. |
| Kids Nursery Rhymes | $0.50–2 | **High risk.** "Made for Kids" (COPPA) kills personalized ads and comments. Pure volume game, and YouTube aggressively demonetizes low-quality AI kids content. |

## Recommended AI production stack (2026)

**Best all-round for faceless videos: Invideo AI** — script → voiceover →
visuals → edit from one prompt, and it's **already connected to this Claude
session**, so Claude can generate a video from a script for you directly.
Best-in-class alternatives by job:

| Job | Tool |
|---|---|
| Full faceless video from a prompt (fastest path) | **Invideo AI** |
| Cinematic b-roll clips (motivation/history especially) | **Google Veo 3.1** or **Sora 2** |
| One dashboard for multiple gen models (Gen-4.5, Veo, Kling 3.0) | **Runway** |
| Script/article → edited video with captions | **Pictory** |
| Avatar presenter or dubbing (if you ever want a "face") | **HeyGen** |
| Kids animation | Veo/Kling for clips + a human editor pass — quality bar is high |

Pipeline that works: this tool finds the target format → Claude writes an
original script in your angle → Invideo/Veo produces the video → you review
before publishing (never auto-publish).

## Reaching $10k/month — honest math

- **Monetization gate:** YouTube Partner Program requires 1,000 subscribers +
  4,000 public watch hours (or 10M Shorts views). Budget 2–4 months of
  consistent uploads before ad revenue starts.
- The report computes, per niche, how many performing videos you'd need at
  the niche's RPM. Finance needs the fewest; kids content needs enormous
  volume.
- Ads are the floor, not the ceiling: in finance and AI-tools niches,
  affiliate links and sponsorships routinely pay 2–5x the ad revenue.
- Expect a hit rate, not uniform performance: plan for ~1 in 5 videos to
  become a top performer. Upload cadence (3–5/week, achievable with AI
  production) is the main lever.
