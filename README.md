# Realtor Pro System

Automated **FSBO → skip-trace → outreach** pipeline for real-estate prospecting.

Find for-sale-by-owner listings by ZIP code, resolve owner contact info, save leads, and draft or send outreach — without manual copy-paste into skip-trace tools or email.

---

## What it does

| Stage | Actor | Job |
|-------|--------|-----|
| 1 | **Scraper** | Pull FSBO listings per ZIP (`mock`, `http`, or `playwright`) |
| 2 | **Normalizer** | Clean raw data into structured listings (address, price, beds, baths, sqft) |
| 3 | **Dedup** | Skip addresses already contacted; reuse cached skip-trace data |
| 4 | **Skip tracer** | Resolve owner name / phone / email (mock or BatchData) |
| 5 | **Store** | Upsert into Supabase (`fsbo_listings`) |
| 6 | **Outreach** | Draft or send Gmail messages to owners |
| 7 | **Supervisor** | Fan out work across ZIPs and collect results |

Everything runs as **async actors** with mailboxes — stages don’t call each other directly; they pass messages. That keeps failures isolated and makes the pipeline easy to extend.

---

## Features

- **Three scraper backends** — mock (no network), HTTP + BeautifulSoup, Playwright (JS-heavy sites)
- **Dedup that survives restarts** — local `dedup_state.json` cache + optional Supabase
- **Lead status machine** — `new → traced → contacted → responded` (also `skipped_duplicate`)
- **Hardened skip-trace** — retries, backoff, rate limit, per-run call budget
- **Dry-run mode** — full pipeline, zero writes, zero emails
- **Rescrape cooldown** — don’t hit the same ZIP every few minutes
- **Watch mode** — re-run on an interval
- **CLI** — ZIPs from args or file, backend override, force, dry-run
- **Migrations** — SQL schema + `migrate.py` for Supabase/Postgres

---

## Quick start (easiest path)

```bash
# 1. Clone
git clone https://github.com/alexh30486-ui/Realtor.git
cd Realtor

# 2. Virtualenv + deps
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 3. Env file (optional for demo)
cp .env.example .env
