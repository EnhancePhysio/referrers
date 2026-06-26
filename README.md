# Referral & ROI Dashboard

Live Streamlit dashboard over multiple Cliniko accounts. Shows who
referred whom, how much revenue each referrer generated, and ROI for
paid channels (Google, Meta/social, sponsorships, advertising).

Currently configured for three accounts:

| Practice                              | Shard | Clinics inside the account     |
|---------------------------------------|-------|-------------------------------|
| Enhance Physio                        | au1   | Albury, Wodonga, Lavington     |
| Mudgeeraba                            | au1   | Mudgeeraba                     |
| Mulgrave                              | au4   | Mulgrave                       |

Filters: period (This month / Last month / This quarter / Last quarter /
YTD / Last year / Custom) and clinic (one of the five above, or All).

## Architecture

```
┌──────────────────────────────────┐    Friday 18:00 AEST (or "Run workflow")
│  GitHub Actions — sync.yml       │◄─────────────────
│       loops over accounts.py     │
│       runs sync.py per account   │
└────────────────┬─────────────────┘
                 │ writes + commits
                 ▼
      data/enhance/*.parquet           ← per-account folder
      data/mudgeeraba/*.parquet        ← per-account folder
      data/mulgrave/*.parquet          ← per-account folder
                 │ Streamlit Cloud auto-redeploys on push
                 ▼
┌──────────────────────────────────┐    (invoices still fetched live
│      Streamlit app (app.py)      │     from each account for the
│  reads parquets at startup,      │     selected period)
│  joins composite (account, id)   │
└──────────────────────────────────┘
```

### Files

- `app.py` — Streamlit UI: password gate, sidebar filters, four tabs.
- `accounts.py` — single source of truth: list of accounts + their secret env var names.
- `cliniko_client.py` — thin Cliniko REST client (shard-aware, pagination, retry).
- `data.py` — snapshot-first loaders + composite-key joining logic.
- `sync.py` — offline sync, loops over every account in `accounts.py`.
- `.github/workflows/sync.yml` — cron + manual trigger; exposes all account secrets.
- `.streamlit/secrets.toml.example` — template; copy to `secrets.toml` for local dev.

## Adding or removing an account

1. Edit `accounts.py` — add/remove an `Account(...)` entry.
2. Add the matching API key to **GitHub Actions Secrets** (for the sync).
3. Add the matching API key to **Streamlit Cloud Secrets** (for live invoices).
4. Add an `env:` line to `.github/workflows/sync.yml` exposing the new secret.
5. Run the sync workflow once to populate `data/<new_account>/`.

## One-time setup

### 1. Create a read-only Cliniko user + API key in EACH account

For every Cliniko account: Settings → Users → add a user called e.g.
`Dashboard API`. Uncheck every create/edit/delete permission; leave only
"view" on Patients, Appointments, Invoices, Payments, Practitioners,
Businesses. Then Settings → My Info → Manage API Keys → Create. Copy
each key (they end in the shard suffix, e.g. `-au1`, `-au4`).

### 2. Add the API keys as GitHub Actions secrets

GitHub → repo → **Settings** → **Secrets and variables** → **Actions** →
**New repository secret**, one per account:

- `CLINIKO_API_KEY_ENHANCE`     → Enhance Physio key (ends in `-au1`)
- `CLINIKO_API_KEY_MUDGEERABA`  → Mudgeeraba key (ends in `-au1`)
- `CLINIKO_API_KEY_MULGRAVE`    → Mulgrave key (ends in `-au4`)

### 3. Run the sync workflow once manually

GitHub → repo → **Actions** tab → **Sync Cliniko data** → **Run
workflow**. First run takes 5–10 minutes per account (paginates through
every patient). When it finishes you'll see the bot commit
`chore(sync): refresh Cliniko snapshots …` containing the parquet files
under `data/enhance/`, `data/mudgeeraba/`, `data/mulgrave/`.

From this point the sync runs automatically every Friday 18:00 AEST
(`0 8 * * 5` UTC). Each scheduled run does an incremental patient fetch
per account (typically seconds total).

### 4. Deploy on Streamlit Cloud

At [share.streamlit.io](https://share.streamlit.io):

- **New app** → point at your repo, `main` branch, `app.py`.
- Under **Advanced settings → Secrets**, paste:

```toml
app_password = "a-strong-password"
cliniko_user_agent = "EnhancePhysio-Dashboard (matt@enhance.physio)"

CLINIKO_API_KEY_ENHANCE     = "...-au1"
CLINIKO_API_KEY_MUDGEERABA  = "...-au1"
CLINIKO_API_KEY_MULGRAVE    = "...-au4"

[ad_spend.enhance]
google       = 1500
social_media = 400
advertising  = 0
sports_club  = 2500

[ad_spend.mudgeeraba]
google       = 0
social_media = 0
advertising  = 0
sports_club  = 0

[ad_spend.mulgrave]
google       = 0
social_media = 0
advertising  = 0
sports_club  = 0
```

- Click Deploy. Open the app, enter the password.

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# First time — populate the snapshots (needs the API key env vars):
export CLINIKO_API_KEY_ENHANCE="...-au1"
export CLINIKO_API_KEY_MUDGEERABA="...-au1"
export CLINIKO_API_KEY_MULGRAVE="...-au4"
python sync.py

cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# …then edit .streamlit/secrets.toml with real values

streamlit run app.py
```

## Attribution logic

- **Revenue attribution**: each invoice's `(account, business_id)`
  determines the clinic it belongs to; each invoice's `(account,
  patient_id)` determines the patient and (via `referral_sources`) the
  referrer. All totals are **sum of invoice `total_amount`** for invoices
  issued in the selected period.
- **Paid-channel ROI** uses same-period revenue only. Per-practice ad
  spend rolls up across whichever accounts are currently visible (after
  the clinic filter is applied). For lifetime-value ROI (all invoices
  from patients acquired via a channel), extend `channel_rollup` to
  look back across all historic invoices per patient.

## Sync details

- **Patients**: incremental on `updated_at`, per account. Every run
  rewinds the watermark by 15 min. Records are merged within their
  account by `patient_id` (newer wins).
- **Referral sources / source types / contacts**: full refresh per
  account each run.
- **Businesses**: full refresh per account each run.
- **Invoices**: **not** snapshotted — fetched live every time you pick
  a period (fast: a quarter is typically a few hundred records per
  account).

### Force a full patient re-sync for one account

Delete `data/<account>/patients.parquet` in GitHub (or locally + push),
then re-run the workflow. Next run will treat it as a fresh bootstrap
for that account only.

### Partial failure handling

If one account's API key is missing or invalid, the sync logs a warning
and continues with the others. The workflow only fails if EVERY account
fails — so a single broken key won't block the rest of the snapshot.

## Security notes

- API keys live in **GitHub Actions Secrets** (for the sync) and
  **Streamlit Cloud Secrets** (for live invoice queries). Never in
  the repo.
- `.streamlit/secrets.toml` is git-ignored.
- The committed `data/<account>/*.parquet` files contain patient names +
  Cliniko IDs. Keep the repo **private**.
- Use a read-only Cliniko user for each API key so a key leak can't
  modify records.
- Cliniko IDs are **per-account** — the dashboard uses composite
  `(account, id)` joins everywhere to prevent cross-account collisions.

## Extending

- Lifetime-value ROI (second invoice query per patient_id).
- Monthly trend charts.
- Email a weekly PDF snapshot via SendGrid.
- Referral conversion funnel.
