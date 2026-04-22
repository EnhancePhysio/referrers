"""Enhance Physio — Referral & ROI Dashboard.

Live Streamlit dashboard backed by the Cliniko API. See README.md for setup.
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import requests
import streamlit as st

from cliniko_client import ClinikoClient
from data import (
    build_invoice_view,
    channel_rollup,
    load_businesses,
    load_contacts,
    load_invoices,
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
def _client() -> ClinikoClient:
    return ClinikoClient(
        api_key=st.secrets["cliniko_api_key"],
        user_agent=st.secrets.get(
            "cliniko_user_agent",
            "EnhancePhysio-Dashboard (matt@enhance.physio)",
        ),
    )


def _missing_snapshot_error(name: str) -> None:
    """Guide the user to populate a parquet snapshot that doesn't exist yet."""
    st.error(
        f"The **{name}** snapshot (`data/{name}.parquet`) doesn't exist yet.\n\n"
        "Trigger it once manually: GitHub → this repo → **Actions** → "
        "_Sync Cliniko data_ → **Run workflow**. After it finishes and "
        "auto-commits, Streamlit will redeploy and this page will work.\n\n"
        "From then on it refreshes automatically every Friday 18:00 AEST."
    )
    st.stop()


@st.cache_data(ttl=86400, show_spinner="Loading businesses…")
def _businesses() -> pd.DataFrame:
    try:
        return load_businesses(_client())
    except RuntimeError:
        _missing_snapshot_error("businesses")
    except requests.exceptions.SSLError as e:
        st.error(
            f"SSL error connecting to Cliniko at `{_client().base_url}`.\n\n"
            f"Full error: `{type(e).__name__}: {e}`\n\n"
            "Check: (1) the shard in your API key matches your Cliniko URL "
            "(yours should end in `-au1`), and (2) the app has a fresh CA "
            "bundle (ensure `certifi` is in requirements.txt)."
        )
        st.stop()
    except requests.exceptions.HTTPError as e:
        st.error(
            f"Cliniko returned HTTP {e.response.status_code} at "
            f"`{_client().base_url}/businesses`.\n\n"
            f"Response body: `{e.response.text[:500]}`"
        )
        st.stop()


@st.cache_data(ttl=86400, show_spinner="Loading referral sources…")
def _referral_sources() -> pd.DataFrame:
    try:
        return load_referral_sources(_client())
    except RuntimeError:
        _missing_snapshot_error("referral_sources")


@st.cache_data(ttl=86400, show_spinner="Loading referral source types…")
def _referral_source_types() -> pd.DataFrame:
    try:
        return load_referral_source_types(_client())
    except RuntimeError:
        _missing_snapshot_error("referral_source_types")


@st.cache_data(ttl=86400, show_spinner="Loading contacts…")
def _contacts() -> pd.DataFrame:
    try:
        return load_contacts(_client())
    except RuntimeError:
        _missing_snapshot_error("contacts")


@st.cache_data(ttl=86400, show_spinner="Loading patients from snapshot…")
def _patients() -> pd.DataFrame:
    try:
        return load_patients(_client())
    except RuntimeError:
        _missing_snapshot_error("patients")


@st.cache_data(ttl=3600, show_spinner="Loading invoices…")
def _invoices(start: date, end: date) -> pd.DataFrame:
    return load_invoices(_client(), start, end)


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

st.title("Enhance Physio — Referral & ROI Dashboard")
st.caption(
    f"Period: **{start_date.isoformat()} → {end_date.isoformat()}**"
    f" · Clinic: **{clinic_choice}**"
    f" · {len(invoice_view):,} invoices"
    f" · {invoice_view['patient_id'].nunique():,} unique patients"
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
        "Enter what you spent on each paid channel in this period. "
        "The table compares spend against revenue attributed to that channel "
        "(summed across all clinics, since ad spend is typically brand-wide)."
    )

    # Pull default spend from secrets, if set.
    defaults: dict = st.secrets.get("ad_spend", {}) if hasattr(st, "secrets") else {}

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
        default_spend = float(defaults.get(channel.lower().replace(" ", "_"), 0))
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
