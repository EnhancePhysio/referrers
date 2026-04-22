# Enhance Physio — Referral & ROI Dashboard

Live Streamlit dashboard over the Cliniko API. Shows who referred whom,
how much revenue each referrer generated, and ROI for paid channels
(Google, Meta/social, sponsorships, advertising).

Filters: period (This month / Last month / This quarter / Last quarter /
YTD / Last year / Custom) and clinic (Albury / Wodonga / Lavington / All).

## Architecture

- `app.py` — Streamlit UI: password gate, sidebar filters, four tabs.
- `cliniko_client.py` — thin Cliniko REST client with pagination & retry.
- `data.py` — data loaders and the invoice/referrer/clinic join logic.
- `.streamlit/secrets.toml` — local secrets (git-ignored).
- `.streamlit/secrets.toml.example` — template; copy to `secrets.toml`.

Caching: every Cliniko call is cached for 1 hour. Click **Refresh from
Cliniko** in the sidebar to force a reload.

## Setup — deploy to Streamlit Community Cloud

1. **Create a read-only Cliniko user.** In Cliniko → Settings → Users →
   add a user called e.g. `Dashboard API`. Uncheck every "create / edit /
   delete" permission; leave only "view" on Patients, Appointments,
   Invoices, Payments, Practitioners, Businesses.
2. **Generate an API key** as that user: Settings → My Info → Manage API
   Keys → Create. Copy the key.
3. **Create a new GitHub repo** and push this folder:
   ```bash
   cd enhance-physio-dashboard
   git init
   git add .
   git commit -m "Initial commit: Cliniko referral dashboard"
   git branch -M main
   git remote add origin git@github.com:<your-user>/<repo>.git
   git push -u origin main
   ```
4. **Deploy on Streamlit Cloud** (https://share.streamlit.io):
   - New app → point at your repo, `main` branch, `app.py`.
   - Under **Advanced settings → Secrets**, paste the contents of
     `.streamlit/secrets.toml.example` and fill in real values:
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
   - Click Deploy.
5. **Open the app** and enter the password you chose.

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# …then edit .streamlit/secrets.toml with real values

streamlit run app.py
```

## Attribution logic

- **Revenue attribution**: each invoice's `business_id` determines the
  clinic it belongs to, and each invoice's patient's `referral_source`
  determines the referrer. All totals are **sum of invoice
  `total_including_tax`** for invoices issued in the selected period.
- **Paid-channel ROI** uses same-period revenue only. For lifetime-value
  ROI (all invoices from patients acquired via a channel), ask Claude
  to extend `channel_rollup` to look back across all historic invoices
  for each patient.

## Security notes

- Secrets live in Streamlit Cloud's Secrets UI, never in Git.
- `.streamlit/secrets.toml` is in `.gitignore`.
- Password gate is a simple string compare; for stronger auth use a
  Streamlit SSO package or put Cloudflare Access in front of the app.
- Use a read-only Cliniko user for the API key so a key leak can't
  modify records.

## Extending

Common next steps:
- Add lifetime-value ROI (needs a second invoice query by patient_id).
- Add monthly trend charts (`st.line_chart` on
  `invoice_view.groupby([pd.Grouper(key='issue_date', freq='MS')])`).
- Email a weekly PDF snapshot (render via
  [stpdfgenerator](https://pypi.org/project/streamlit-pdf-generator/)
  and send via SendGrid).
- Referral conversion funnel (new patients → attended → invoiced).
