"""Enhance Physio — Referral & ROI Dashboard.

Live Streamlit dashboard backed by the Cliniko API. See README.md for setup.
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import requests
import streamlit as st

from accounts import ACCOUNTS
from cliniko_client import ClinikoClient
from data import (
    build_invoice_view,
    channel_rollup,
    load_businesses,
    load_contacts,
    load_invoices_multi,
    load_patients,
    load_referral_source_types,
    load_referral_sources,
    referrer_league_table,
)

st.set_page_config(
    page_title="Enhance Physio — Referral Dashboard",
    page_icon="🏥",
    layout="wide",
)


# --- Password gate --------------------------------------------------------

def _check_password() -> bool:
    """Simple password gate. Password lives in st.secrets['app_password']."""
    if st.session_state.get("auth_ok"):
        return True

    st.title("Enhance Physio — Referral Dashboard")
    pw = st.text_input("Password", type="password")
    if not pw:
        st.stop()
    if pw == st.secrets.get("app_password"):
        st.session_state["auth_ok"] = True
        st.rerun()
    else:
        st.error("Incorrect password.")
        st.stop()
    return False


_check_password()


# --- Data loaders (cached) ------------------------------------------------

@st.cache_resource
def _clients() -> dict[str, ClinikoClient]:
    """Build one ClinikoClient per account whose API key is present in
    Streamlit Secrets. Accounts with a missing key are silently skipped —
    the dashboard still renders for whichever accounts are configured."""
    user_agent = st.secrets.get(
        "cliniko_user_agent",
        "EnhancePhysio-Dashboard (matt@enhance.physio)",
    )
    out: dict[str, ClinikoClient] = {}
    for account in ACCOUNTS:
        key = (
            st.secrets.get(account.secret_env)
            or st.secrets.get(account.secret_env.lower())
            or ""
        )
        if not key:
            continue
        try:
            out[account.label] = ClinikoClient(api_key=key, user_agent=user_agent)
        except Exception as e:
            st.warning(
                f"Could not initialise Cliniko client for {account.display_name}: {e}"
            )
    return out


def _missing_snapshot_error(name: str) -> None:
    """Guide the user to populate a parquet snapshot that doesn't exist yet."""
    st.error(
        f"No `{name}.parquet` snapshot found for any account "
        f"(expected at `data/<account>/{name}.parquet`).\n\n"
        "Trigger the sync once manually: GitHub → this repo → **Actions** → "
        "_Sync Cliniko data_ → **Run workflow**. After it finishes and "
        "auto-commits, Streamlit will redeploy and this page will work.\n\n"
        "From then on it refreshes automatically every Friday 18:00 AEST."
    )
    st.stop()


@st.cache_data(ttl=86400, show_spinner="Loading businesses…")
def _businesses() -> pd.DataFrame:
    try:
        return load_businesses()
    except RuntimeError:
        _missing_snapshot_error("businesses")


@st.cache_data(ttl=86400, show_spinner="Loading referral sources…")
def _referral_sources() -> pd.DataFrame:
    try:
        return load_referral_sources()
    except RuntimeError:
        _missing_snapshot_error("referral_sources")


@st.cache_data(ttl=86400, show_spinner="Loading referral source types…")
def _referral_source_types() -> pd.DataFrame:
    try:
        return load_referral_source_types()
    except RuntimeError:
        _missing_snapshot_error("referral_source_types")


@st.cache_data(ttl=86400, show_spinner="Loading contacts…")
def _contacts() -> pd.DataFrame:
    try:
        return load_contacts()
    except RuntimeError:
        _missing_snapshot_error("contacts")


@st.cache_data(ttl=86400, show_spinner="Loading patients from snapshot…")
def _patients() -> pd.DataFrame:
    try:
        return load_patients()
    except RuntimeError:
        _missing_snapshot_error("patients")


@st.cache_data(ttl=3600, show_spinner="Loading invoices…")
def _invoices(start: date, end: date) -> pd.DataFrame:
    clients = _clients()
    if not clients:
        st.error(
            "No Cliniko API keys configured. Add at least one of "
            f"{[a.secret_env for a in ACCOUNTS]} to Streamlit Secrets."
        )
        st.stop()
    try:
        return load_invoices_multi(clients, start, end)
    except requests.exceptions.SSLError as e:
        st.error(
            f"SSL error connecting to Cliniko: `{type(e).__name__}: {e}`\n\n"
            "Check that your API keys end with the correct shard suffix "
            "(e.g. `-au1`, `-au4`) and that `certifi` is installed."
        )
        st.stop()
    except requests.exceptions.HTTPError as e:
        st.error(
            f"Cliniko returned HTTP {e.response.status_code}.\n\n"
            f"Response body: `{e.response.text[:500]}`"
        )
        st.stop()


# --- Sidebar: period + clinic filter --------------------------------------

def _period_presets(today: date) -> dict[str, tuple[date, date]]:
    first_of_month = today.replace(day=1)
    last_month_end = first_of_month - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)

    q_start_month = ((today.month - 1) // 3) * 3 + 1
    this_q_start = date(today.year, q_start_month, 1)
    if q_start_month == 1:
        last_q_start = date(today.year - 1, 10, 1)
        last_q_end = date(today.year - 1, 12, 31)
    else:
        last_q_start = date(today.year, q_start_month - 3, 1)
        last_q_end = this_q_start - timedelta(days=1)

    return {
        "This month": (first_of_month, today),
        "Last month": (last_month_start, last_month_end),
        "This quarter": (this_q_start, today),
        "Last quarter": (last_q_start, last_q_end),
        "Year to date": (date(today.year, 1, 1), today),
        "Last year": (date(today.year - 1, 1, 1), date(today.year - 1, 12, 31)),
    }


with st.sidebar:
    st.header("Filters")

    today = date.today()
    presets = _period_presets(today)
    period_label = st.selectbox(
        "Period",
        list(presets.keys()) + ["Custom"],
        index=3,  # Default to "Last quarter"
    )
    if period_label == "Custom":
        start_date = st.date_input("Start", value=today - timedelta(days=90))
        end_date = st.date_input("End", value=today)
    else:
        start_date, end_date = presets[period_label]
        st.caption(f"{start_date.isoformat()} → {end_date.isoformat()}")

    businesses = _businesses()
    clinic_options = ["All"] + sorted(businesses["business_name"].tolist())
    clinic_choice = st.selectbox("Clinic", clinic_options, index=0)

    st.divider()
    if st.button("Refresh from Cliniko", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.caption(
        "Patients / referrers are served from a cached snapshot that "
        "auto-refreshes every Friday 18:00 AEST. Invoices are fetched "
        "live for the selected period and cached for 1 hour."
    )


# --- Load + shape data ----------------------------------------------------

referral_sources = _referral_sources()
referral_source_types = _referral_source_types()
contacts = _contacts()
patients = _patients()
invoices = _invoices(start_date, end_date)
invoice_view = build_invoice_view(
    invoices,
    patients,
    referral_sources,
    referral_source_types,
    contacts,
    businesses,
)

if clinic_choice != "All":
    invoice_view = invoice_view[invoice_view["business_name"] == clinic_choice]


# --- Header ---------------------------------------------------------------

st.title("Referral & ROI Dashboard")

# Unique patients across multiple accounts must dedup on (account, patient_id)
# — Cliniko IDs are per-account, so two practices can share the same numeric id.
if invoice_view.empty:
    unique_patients = 0
else:
    unique_patients = invoice_view.drop_duplicates(
        ["account", "patient_id"]
    ).shape[0]

st.caption(
    f"Period: **{start_date.isoformat()} → {end_date.isoformat()}**"
    f" · Clinic: **{clinic_choice}**"
    f" · {len(invoice_view):,} invoices"
    f" · {unique_patients:,} unique patients"
    f" · ${invoice_view['total_incl_tax'].sum():,.0f} total revenue"
)


# --- Tabs -----------------------------------------------------------------

tab_referrers, tab_channels, tab_roi, tab_invoices = st.tabs(
    ["Referrers", "Channel rollup", "ROI (paid channels)", "Invoices"]
)

# -- Referrers -------------------------------------------------------------
with tab_referrers:
    league = referrer_league_table(invoice_view)
    st.subheader("Referrer league table")
    st.caption(
        "One row per (clinic, referral type, referrer). "
        "Revenue = sum of invoice totals (incl. tax) issued in period, "
        "attributed to the referring source."
    )
    if league.empty:
        st.info("No invoices in this period.")
    else:
        display = league.copy()
        display["total_revenue"] = display["total_revenue"].round(2)
        st.dataframe(
            display,
            use_container_width=True,
            hide_index=True,
            column_config={
                "business_name": "Clinic",
                "referral_type": "Type",
                "referral_name": "Referrer",
                "patients_referred": st.column_config.NumberColumn("Patients", format="%d"),
                "invoices": st.column_config.NumberColumn("Invoices", format="%d"),
                "total_revenue": st.column_config.NumberColumn("Revenue", format="$%.0f"),
                "avg_per_patient": st.column_config.NumberColumn("$ / patient", format="$%.0f"),
            },
        )
        st.download_button(
            "Download as CSV",
            data=league.to_csv(index=False),
            file_name=f"referrer-league-{start_date}-{end_date}.csv",
            mime="text/csv",
        )

# -- Channel rollup --------------------------------------------------------
with tab_channels:
    rollup = channel_rollup(invoice_view)
    st.subheader("Revenue by referral channel")
    st.caption(
        "Aggregated to the referral type level — "
        "Contact, Google, Social Media, Sports Club, etc."
    )
    if rollup.empty:
        st.info("No invoices in this period.")
    else:
        st.dataframe(
            rollup.round(2),
            use_container_width=True,
            hide_index=True,
            column_config={
                "business_name": "Clinic",
                "referral_type": "Channel",
                "patients_referred": st.column_config.NumberColumn("Patients", format="%d"),
                "total_revenue": st.column_config.NumberColumn("Revenue", format="$%.0f"),
            },
        )

# -- ROI --------------------------------------------------------------------
with tab_roi:
    st.subheader("ROI — paid channels only")
    st.caption(
        "Spend on each paid channel, compared against revenue attributed "
        "to that channel for the accounts/clinics currently in view. "
        "Ad-spend defaults come from per-practice sections in Streamlit "
        "Secrets (e.g. `[ad_spend.enhance]`, `[ad_spend.mudgeeraba]`)."
    )

    # Determine which accounts contribute revenue to the current view.
    # (After applying the clinic filter, only some accounts may remain.)
    accounts_in_view: list[str] = (
        sorted(invoice_view["account"].dropna().unique().tolist())
        if not invoice_view.empty and "account" in invoice_view.columns
        else [a.label for a in ACCOUNTS]
    )

    # Sum default spend across the in-view accounts. Supports both the
    # new per-practice layout (recommended) and the legacy flat layout.
    legacy_defaults: dict = (
        st.secrets.get("ad_spend", {}) if hasattr(st, "secrets") else {}
    )

    def _spend_for_channel(channel_key: str) -> float:
        total = 0.0
        for label in accounts_in_view:
            per_account = st.secrets.get("ad_spend", {}).get(label, {}) \
                if hasattr(st, "secrets") else {}
            if isinstance(per_account, dict) and channel_key in per_account:
                total += float(per_account[channel_key])
            elif label == "enhance" and channel_key in legacy_defaults:
                # Backwards-compat: the original flat [ad_spend] section
                # is treated as Enhance's spend.
                total += float(legacy_defaults[channel_key])
        return total

    if accounts_in_view:
        st.caption(
            "Aggregating ad spend for: "
            + ", ".join(accounts_in_view)
        )

    # Roll up by channel across the whole (possibly clinic-filtered) view.
    channel_totals = (
        invoice_view.groupby("referral_type")
        .agg(
            patients=("patient_id", "nunique"),
            revenue=("total_incl_tax", "sum"),
        )
        .reset_index()
    )

    paid_channels = ["Google", "Social Media", "Advertising", "Sports Club"]
    paid_totals = channel_totals[
        channel_totals["referral_type"].isin(paid_channels)
    ].set_index("referral_type")

    roi_rows = []
    for channel in paid_channels:
        rev = float(paid_totals["revenue"].get(channel, 0))
        patients_n = int(paid_totals["patients"].get(channel, 0))
        default_spend = _spend_for_channel(channel.lower().replace(" ", "_"))
        roi_rows.append(
            {
                "Channel": channel,
                "Patients referred": patients_n,
                "Revenue ($)": round(rev, 2),
                "Spend ($)": default_spend,
            }
        )

    roi_df = pd.DataFrame(roi_rows)
    edited = st.data_editor(
        roi_df,
        hide_index=True,
        use_container_width=True,
        num_rows="fixed",
        column_config={
            "Spend ($)": st.column_config.NumberColumn(
                help="Edit to override the value from secrets.toml for this session.",
                min_value=0,
                format="$%.0f",
            ),
        },
        disabled=["Channel", "Patients referred", "Revenue ($)"],
    )
    edited["Net ($)"] = (edited["Revenue ($)"] - edited["Spend ($)"]).round(2)
    edited["ROI"] = edited.apply(
        lambda r: (
            f"{(r['Revenue ($)'] - r['Spend ($)']) / r['Spend ($)'] * 100:.0f}%"
            if r["Spend ($)"] > 0
            else "—"
        ),
        axis=1,
    )
    edited["$ per patient"] = edited.apply(
        lambda r: f"${r['Revenue ($)'] / r['Patients referred']:.0f}"
        if r["Patients referred"] > 0
        else "—",
        axis=1,
    )
    st.markdown("**Results**")
    st.dataframe(
        edited,
        use_container_width=True,
        hide_index=True,
    )

    st.caption(
        "ROI = (Revenue − Spend) / Spend. "
        "Revenue is same-period only — for a true payback picture you'd want lifetime "
        "revenue from patients acquired via each channel. Ask me to add that."
    )

# -- Invoices --------------------------------------------------------------
with tab_invoices:
    st.subheader("Invoice detail")
    st.caption("Every invoice in the period, with referrer and clinic resolved.")
    if invoice_view.empty:
        st.info("No invoices in this period.")
    else:
        show = invoice_view[
            [
                "issue_date",
                "business_name",
                "patient_name",
                "referral_type",
                "referral_name",
                "total_incl_tax",
                "status",
                "invoice_number",
            ]
        ].sort_values("issue_date", ascending=False)
        st.dataframe(
            show,
            use_container_width=True,
            hide_index=True,
            column_config={
                "issue_date": "Date",
                "business_name": "Clinic",
                "patient_name": "Patient",
                "referral_type": "Type",
                "referral_name": "Referrer",
                "total_incl_tax": st.column_config.NumberColumn("Total", format="$%.2f"),
                "status": "Status",
                "invoice_number": "Invoice #",
            },
        )
        st.download_button(
            "Download as CSV",
            data=show.to_csv(index=False),
            file_name=f"invoices-{start_date}-{end_date}.csv",
            mime="text/csv",
        )


# --- Data health diagnostics ---------------------------------------------
# A quick way to tell whether empty/weird numbers are from missing source
# data, broken joins, or genuinely no activity in the period.

with st.expander("🔧 Data health diagnostics"):
    def _safe_nunique(df, col):
        """Count distinct non-null values of ``col`` in ``df`` — returns 0
        if the column isn't present (e.g. parquet came back empty)."""
        if df is None or df.empty or col not in df.columns:
            return 0
        return df[col].dropna().nunique()

    n_patients = len(patients)
    n_sources = len(referral_sources)
    n_source_types = len(referral_source_types)
    n_contacts = len(contacts)
    n_invoices = len(invoices)
    n_patients_with_rs = _safe_nunique(referral_sources, "patient_id")

    if invoice_view.empty:
        joined_to_patient = 0
        joined_to_referrer = 0
    else:
        joined_to_patient = invoice_view["patient_name"].fillna("").ne("").sum()
        joined_to_referrer = invoice_view["referral_type"].ne("(none)").sum()

    col_a, col_b, col_c, col_d = st.columns(4)
    col_a.metric("Patients", f"{n_patients:,}")
    col_a.caption(
        f"{n_patients_with_rs:,} have a referral source "
        f"({(n_patients_with_rs / n_patients * 100 if n_patients else 0):.0f}%)"
    )
    col_b.metric("Referral sources", f"{n_sources:,}")
    col_c.metric("Source types", f"{n_source_types:,}")
    col_d.metric("Contacts", f"{n_contacts:,}")

    col_e, col_f, col_g, col_h = st.columns(4)
    col_e.metric("Invoices (period)", f"{n_invoices:,}")
    col_f.metric("Joined to patient", f"{joined_to_patient:,}")
    col_g.metric("Joined to referrer", f"{joined_to_referrer:,}")
    revenue_total = (
        invoice_view["total_incl_tax"].sum()
        if not invoice_view.empty and "total_incl_tax" in invoice_view.columns
        else 0
    )
    col_h.metric("Revenue in period", f"${revenue_total:,.0f}")

    # Per-account row counts — at a glance, see if any practice's sync
    # is missing or has way less data than expected.
    st.markdown("**Per-account row counts**")
    per_acct = []
    for account in ACCOUNTS:
        per_acct.append(
            {
                "account": account.label,
                "display_name": account.display_name,
                "patients": int((patients["account"] == account.label).sum())
                if "account" in patients.columns else 0,
                "referral_sources": int((referral_sources["account"] == account.label).sum())
                if "account" in referral_sources.columns else 0,
                "contacts": int((contacts["account"] == account.label).sum())
                if "account" in contacts.columns else 0,
                "businesses": int((businesses["account"] == account.label).sum())
                if "account" in businesses.columns else 0,
                "invoices_in_period": int((invoices["account"] == account.label).sum())
                if "account" in invoices.columns else 0,
            }
        )
    st.dataframe(pd.DataFrame(per_acct), use_container_width=True, hide_index=True)

    # Show each snapshot's *columns* first — this is how we diagnose a
    # schema mismatch (e.g. old parquet with different column names).
    st.markdown("**Snapshot columns** (what's actually in each parquet)")
    schema_rows = []
    for name, df in [
        ("patients", patients),
        ("referral_sources", referral_sources),
        ("referral_source_types", referral_source_types),
        ("contacts", contacts),
        ("businesses", businesses),
    ]:
        schema_rows.append(
            {
                "table": name,
                "rows": len(df),
                "columns": ", ".join(df.columns.astype(str)) if not df.empty else "(empty)",
            }
        )
    st.dataframe(pd.DataFrame(schema_rows), use_container_width=True, hide_index=True)

    st.markdown("**Sample — referral source types (the category list)**")
    st.dataframe(
        referral_source_types.head(20),
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("**Sample — 5 referral sources (per-patient records)**")
    st.dataframe(
        referral_sources.head(5), use_container_width=True, hide_index=True
    )

    st.markdown("**Sample — 5 contacts**")
    st.dataframe(contacts.head(5), use_container_width=True, hide_index=True)

    st.markdown("**Sample — 5 raw invoices (pre-join)**")
    st.dataframe(invoices.head(5), use_container_width=True, hide_index=True)

    # --- Unmatched patients (the "(none)" bucket) ---
    # For patients in the selected period whose invoices aren't matching
    # to any referral source. Use this list to spot-check 5–10 of them
    # in Cliniko and see whether they *should* have a matching source.
    if not invoice_view.empty:
        unmatched = (
            invoice_view[invoice_view["referral_type"] == "(none)"]
            .groupby(["account", "patient_id"])
            .agg(
                patient_name=("patient_name", "first"),
                business_name=("business_name", "first"),
                invoices=("invoice_id", "nunique"),
                revenue=("total_incl_tax", "sum"),
            )
            .reset_index()
            .sort_values("revenue", ascending=False)
        )
        st.markdown(
            f"**Unmatched patients in period ({len(unmatched):,})** — "
            "these invoices couldn't be joined to any referral source. "
            "Click a few of their names and look them up in Cliniko — does "
            "their patient record actually have a 'How did you find us?' "
            "referrer set?"
        )
        # Also flag how many of these patients DO exist in referral_sources
        # (if any) — that distinguishes 'no record at all' from 'record
        # exists but the join is broken'. Composite (account, patient_id)
        # because IDs are not unique across accounts.
        if {"account", "patient_id"}.issubset(referral_sources.columns):
            rs_pairs = set(
                zip(
                    referral_sources["account"].astype(str),
                    referral_sources["patient_id"].dropna().astype(str),
                )
            )
        else:
            rs_pairs = set()
        unmatched["in_referral_sources_parquet"] = [
            (str(a), str(p)) in rs_pairs
            for a, p in zip(unmatched["account"], unmatched["patient_id"])
        ]
        st.dataframe(
            unmatched.head(50),
            use_container_width=True,
            hide_index=True,
            column_config={
                "patient_id": "Patient ID",
                "patient_name": "Patient",
                "business_name": "Clinic",
                "invoices": "Invoices",
                "revenue": st.column_config.NumberColumn(
                    "Revenue", format="$%.2f"
                ),
                "in_referral_sources_parquet": st.column_config.CheckboxColumn(
                    "Has RS record?",
                    help=(
                        "True = the patient DOES have a record in "
                        "referral_sources.parquet but the join failed "
                        "(suggests a bug). False = no record at all "
                        "(suggests data entry gap or archived record)."
                    ),
                ),
            },
        )
