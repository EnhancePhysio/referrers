# Enhance Physio — Referral & ROI Dashboard

Live Streamlit dashboard over the Cliniko API. Shows who referred whom,
how much revenue each referrer generated, and ROI for paid channels
(Google, Meta/social, sponsorships, advertising).

Filters: period (This month / Last month / This quarter / Last quarter /
YTD / Last year / Custom) and clinic (Albury / Wodonga / Lavington / All).

## Architecture

```
┌────────────────────────────┐      Friday 18:00 AEST (or "Run workflow")
│  GitHub Actions — sync.yml │◄────────────────────────────
│        runs sync.py        │
└──────────────┬─────────────┘
               │ writes + commits
               ▼
      data/patients.parquet         ← incremental (updated_at watermark)
      data/referral_sources.parquet ← full refresh
      data/businesses.parquet       ← full refresh
               │ Streamlit Cloud auto-redeploys on push
               ▼
┌────────────────────────────┐      (invoices still fetched live
│    Streamlit app (app.py)  │       for whichever period you pick)
│  reads parquets at startup │
└────────────────────────────┘
```

Files:

- `app.py` — Streamlit UI: password gate, sidebar filters, four tabs.
- `cliniko_client.py` — thin Cliniko REST client with pagination & retry.
- `data.py` — snapshot-first loaders + joining logic.
- `sync.py` — the offline sync script that populates `data/*.parquet`.
- `.github/workflows/sync.yml` — cron + manual trigger for the sync.
- `.streamlit/secrets.toml` — local secrets (git-ignored).
- `.streamlit/secrets.toml.example` — template; copy to `secrets.toml`.

## One-time setup

### 1. Create a read-only Cliniko user + API key

In Cliniko → Settings → Users → add a user called e.g. `Dashboard API`.
Uncheck every create/edit/delete permission; leave only "view" on
Patients, Appointments, Invoices, Payments, Practitioners, Businesses.
Then Settings → My Info → Manage API Keys → Create. Copy the key.

### 2. Push this folder to GitHub

```bash
cd enhance-physio-dashboard
git init
git add .
git commit -m "Initial commit: Cliniko referral dashboard"
git branch -M main
git remote add origin git@github.com:<your-user>/<repo>.git
git push -u origin main
```

### 3. Add the API key as a GitHub Actions secret

GitHub → this repo → **Settings** → **Secrets and variables** →
**Actions** → **New repository secret**:

- Name: `CLINIKO_API_KEY`
- Value: your Cliniko API key (the one that ends in `-au1`)

### 4. Run the sync workflow once manually

GitHub → this repo → **Actions** tab → **Sync Cliniko data** →
**Run workflow**. First run takes 5–10 minutes (it paginates through
every patient). When it finishes, you'll see the bot commit
`chore(sync): refresh Cliniko snapshots …` containing the three parquet
files.

From this point on the sync runs automatically every Friday 18:00 AEST
(`0 8 * * 5` UTC). Each scheduled run fetches only patients with
`updated_at >=` the last sync — typically tens to a few hundred
records — and completes in seconds.

### 5. Deploy on Streamlit Cloud

At [share.streamlit.io](https://share.streamlit.io):

- **New app** → point at your repo, `main` branch, `app.py`.
- Under **Advanced settings → Secrets**, paste:

```toml
app_password = "a-strong-password"
cliniko_api_key = "PASTE-KEY-HERE"
cliniko_user_agent = "EnhancePhysio-Dashboard (matt@enhance.physio)"

[ad_spend]
google = 1500
social_media = 400
advertising = 0
sports_club = 2500
```

- Click Deploy. Open the app, enter the password.

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# First time — populate the snapshots (needs CLINIKO_API_KEY env var):
export CLINIKO_API_KEY="...-au1"
python sync.py

cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# …then edit .streamlit/secrets.toml with real values

streamlit run app.py
```

## Attribution logic

- **Revenue attribution**: each invoice's `business_id` determines the
  clinic it belongs to; each invoice's patient's `referral_source`
  determines the referrer. All totals are **sum of invoice
  `total_including_tax`** for invoices issued in the selected period.
- **Paid-channel ROI** uses same-period revenue only. For lifetime-value
  ROI (all invoices from patients acquired via a channel), extend
  `channel_rollup` to look back across all historic invoices per patient.

## Sync details

- **Patients**: incremental on `updated_at`. Every run rewinds the
  watermark by 15 min to cover any record that was mid-write during the
  previous fetch. Records are merged by `patient_id` (newer wins).
- **Referral sources**: full refresh each run. ~8k rows, ~10 seconds.
- **Businesses**: full refresh each run. 3 rows.
- **Invoices**: **not** snapshotted — fetched live every time you pick
  a period (fast: a quarter is typically a few hundred records).

### Force a full patient re-sync

Delete `data/patients.parquet` in GitHub (or locally + push), then
re-run the workflow. Next run will treat it as a fresh bootstrap.

## Security notes

- `CLINIKO_API_KEY` lives in **GitHub Actions Secrets** (for the sync)
  and **Streamlit Cloud Secrets** (for live invoice queries). Never in
  the repo.
- `.streamlit/secrets.toml` is git-ignored.
- The committed `data/*.parquet` files contain patient names + Cliniko
  IDs. Keep the repo **private**.
- Password gate is a simple string compare; for stronger auth use a
  Streamlit SSO package or put Cloudflare Access in front of the app.
- Use a read-only Cliniko user for the API key so a key leak can't
  modify records.

## Extending

Common next steps:

- Lifetime-value ROI (second invoice query per patient_id).
- Monthly trend charts (`st.line_chart` on
  `invoice_view.groupby([pd.Grouper(key='issue_date', freq='MS')])`).
- Email a weekly PDF snapshot via SendGrid.
- Referral conversion funnel (new patients → attended → invoiced).
