# YouTube Channel Production Service — offer blueprint

Modeled on what the market sells (Fiverr "YouTube automation" gigs at
$30 / $995 / $3,795), rebuilt as an honest MonemTech-grade productized
service backed by the pipeline in this folder.

## What the market sells vs. what we sell

Competitor gigs advertise "$5K–$8K/month revenue GUARANTEED 100%." Nobody
can guarantee AdSense revenue; gigs that do typically hit the number with
purchased or botted views, which gets the channel demonetized or banned.
**Our differentiator is the honest version of the same deliverables**: we
guarantee the work product (videos, quality bar, schedule, strategy), never
the revenue, and we say exactly why. That is the trust wedge against every
competitor on the platform.

## Tiers

| | **Starter** | **Growth** | **Full Channel** |
|---|---|---|---|
| Price | $49 one-time | $995/mo | $3,495/mo |
| Purpose | Paid trial / lead-in | Serious side channel | Done-for-you channel |
| Videos | 1 sample video | 15/mo | 30/mo |
| Niche research (clone_finder report) | — | monthly | weekly |
| Channel setup + branding kit (producer --mode channel) | — | one-time | one-time, premium pass |
| Hook-based scripts + SEO metadata (producer) | ✔ | ✔ | ✔ |
| Custom CTR thumbnails | — | ✔ | ✔ |
| Upload & scheduling (uploader) | — | ✔ | ✔ |
| Monetization strategy + growth blueprint | — | quarterly | monthly, with affiliate setup |
| Reporting | — | monthly | weekly |
| Revenue guarantee | **Never.** We guarantee deliverables and quality, and we explain why anyone guaranteeing revenue is lying to you. | | |

Client owns the channel, the Google account, and all content — always.

## Unit economics (why the margins work)

Rough per-video production cost with this pipeline:

| Cost item | Est. |
|---|---|
| Script + metadata + thumbnail brief (Claude API, ~10-15k tokens) | ~$0.50–1.50 |
| Video render (Invideo AI plan, amortized ~$30–60/mo over 30 videos) | ~$1–2 |
| Thumbnail (Canva/AI image) | ~$0.25 |
| Human review + edit pass (the quality bar; ~20 min at $30/hr) | ~$10 |
| **Total per video** | **~$12–14** |

At $995/mo for 15 videos, revenue per video is ~$66 → ~80% gross margin.
The human review pass is non-negotiable: it is both the quality moat and
what keeps client channels on the right side of YouTube's
inauthentic-content policy.

## Delivery workflow (per month, per client)

1. `clone_finder.py --niche <client niche>` → ranked clone-target report.
2. Client (or we) approve the target list.
3. `producer.py --topic ... --niche ...` per video → script, titles,
   description, tags, thumbnail brief, Invideo prompt.
4. Render in Invideo/Veo; human review + edit pass on every video.
5. Thumbnail from the brief.
6. `uploader.py --publish-at ...` → scheduled drip (3–5/week).
7. Monthly report: views, watch time, subscriber delta, next month's targets.

## Positioning rules

- Never use the words "guaranteed income/revenue" in any listing or call.
- Publish the quality bar: original scripts, human-reviewed, one revision
  round included, COPPA-compliant handling for kids content.
- Kids-niche clients get the RPM reality check in writing before signing.
- YPP timeline (1,000 subs + 4,000 watch hours, typically 2–4 months) is
  stated up front in every proposal.
