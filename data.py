"""Data loading + joining layer.

Prefers ``data/*.parquet`` snapshots (populated by ``sync.py`` / GitHub
Actions) for the slow-to-fetch Cliniko resources: patients, referral
sources, and businesses. Falls back to live API if a snapshot is missing
(e.g. very first run before the action has executed).

Invoices are always fetched live — they're small per-quarter and always
period-specific, so snapshotting adds little.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from cliniko_client import ClinikoClient

# Snapshots committed to the repo by the sync workflow.
DATA_DIR = Path(__file__).parent / "data"
PATIENTS_PQ = DATA_DIR / "patients.parquet"
REFERRAL_SOURCES_PQ = DATA_DIR / "referral_sources.parquet"
BUSINESSES_PQ = DATA_DIR / "businesses.parquet"


# --- ID helpers ----------------------------------------------------------

def _id(x: Any) -> str | None:
    """Normalise any scalar id into a string (Cliniko mixes str/int)."""
    if x is None:
        return None
    return str(x)


def _link_id(obj: Any, key: str = "self") -> str | None:
    """Extract the numeric id from a Cliniko relationship field, robustly.

    Cliniko is inconsistent across endpoints: sometimes a relationship is a
    nested object ``{"links": {"self": "https://…/resource/123"}}``,
    sometimes it's a plain URL string (notably ``/patients``'s
    ``referral_source``), sometimes it's missing/None, and in rare cases
    it may be a shape we don't expect. Handle all of that without raising
    — a single weird record shouldn't blow up a full-practice sync.
    """
    if obj is None:
        return None
    if isinstance(obj, str):
        tail = obj.rsplit("/", 1)[-1].strip()
        return tail or None
    if isinstance(obj, dict):
        links = obj.get("links")
        if isinstance(links, dict):
            link = links.get(key)
            if isinstance(link, str) and link:
                return link.rsplit("/", 1)[-1]
    return None


# --- Cliniko → row converters (shared by live + sync paths) -------------

def _patient_row(p: dict) -> dict:
    return {
        "patient_id": _id(p.get("id")),
        "first_name": p.get("first_name") or "",
        "last_name": p.get("last_name") or "",
        "referral_source_id": _link_id(p.get("referral_source")),
        "created_at": p.get("created_at"),
        "updated_at": p.get("updated_at"),
    }


def _business_row(b: dict) -> dict:
    return {
        "business_id": _id(b.get("id")),
        "business_name": b.get("business_name") or b.get("label") or "",
    }


def _referral_source_row(r: dict) -> dict:
    rs_type = r.get("referral_source_type")
    if not isinstance(rs_type, dict):
        rs_type = {}
    return {
        "referral_source_id": _id(r.get("id")),
        "referral_type": rs_type.get("name") or r.get("type") or "",
        "referral_name": (r.get("name") or "").strip() or "(blank)",
    }


# --- Live Cliniko fetchers (used by sync.py) ----------------------------

def fetch_businesses_live(client: ClinikoClient) -> pd.DataFrame:
    return pd.DataFrame([_business_row(b) for b in client.businesses()])


def fetch_referral_sources_live(client: ClinikoClient) -> pd.DataFrame:
    return pd.DataFrame(
        [_referral_source_row(r) for r in client.referral_sources()]
    )


def fetch_patients_live(
    client: ClinikoClient,
    updated_since: str | None = None,
) -> pd.DataFrame:
    """Fetch patients, optionally only those updated since an ISO8601 datetime."""
    params = None
    if updated_since:
        params = {"q[]": [f"updated_at:>={updated_since}"]}
    return pd.DataFrame(
        [_patient_row(p) for p in client.paginate("patients", params=params)]
    )


# --- Snapshot-first loaders (used by the dashboard) ---------------------

def load_businesses(client: ClinikoClient | None = None) -> pd.DataFrame:
    if BUSINESSES_PQ.exists():
        return pd.read_parquet(BUSINESSES_PQ)
    if client is None:
        raise RuntimeError(
            f"{BUSINESSES_PQ} not found — run `python sync.py` first."
        )
    return fetch_businesses_live(client)


def load_referral_sources(client: ClinikoClient | None = None) -> pd.DataFrame:
    if REFERRAL_SOURCES_PQ.exists():
        return pd.read_parquet(REFERRAL_SOURCES_PQ)
    if client is None:
        raise RuntimeError(
            f"{REFERRAL_SOURCES_PQ} not found — run `python sync.py` first."
        )
    return fetch_referral_sources_live(client)


def load_patients(client: ClinikoClient | None = None) -> pd.DataFrame:
    if PATIENTS_PQ.exists():
        return pd.read_parquet(PATIENTS_PQ)
    if client is None:
        raise RuntimeError(
            f"{PATIENTS_PQ} not found — run `python sync.py` first."
        )
    return fetch_patients_live(client)


# --- Invoices (always live) ---------------------------------------------

# Cliniko invoice status is a small int with a known mapping. We prefer the
# API-provided `status_description` when present, but fall back to this.
_STATUS_NAMES = {
    10: "Open",
    20: "Paid",
    30: "Closed",
    40: "Open (credit)",
}


def load_invoices(
    client: ClinikoClient,
    start: date,
    end: date,
) -> pd.DataFrame:
    rows = client.invoices(start.isoformat(), end.isoformat())
    out = []
    for inv in rows:
        # Skip soft-deleted invoices — they still come back from the list
        # endpoint but shouldn't count toward revenue.
        if inv.get("deleted_at"):
            continue

        # Cliniko returns monetary totals as string decimals (e.g. "150.0").
        # The field on an invoice is `total_amount` (NOT total_including_tax
        # — that's on invoice items).
        raw_total = inv.get("total_amount")
        try:
            total = float(raw_total) if raw_total not in (None, "") else 0.0
        except (TypeError, ValueError):
            total = 0.0

        status_int = inv.get("status")
        status_name = inv.get("status_description") or _STATUS_NAMES.get(
            status_int, str(status_int) if status_int is not None else ""
        )

        out.append(
            {
                "invoice_id": _id(inv.get("id")),
                "invoice_number": inv.get("number"),
                "issue_date": inv.get("issue_date"),
                "total_incl_tax": total,
                "patient_id": _link_id(inv.get("patient")),
                "business_id": _link_id(inv.get("business")),
                "status": status_name,
            }
        )
    df = pd.DataFrame(out)
    if not df.empty:
        df["issue_date"] = pd.to_datetime(df["issue_date"]).dt.date
    return df


# --- Joining + rollups --------------------------------------------------

def build_invoice_view(
    invoices: pd.DataFrame,
    patients: pd.DataFrame,
    referral_sources: pd.DataFrame,
    businesses: pd.DataFrame,
) -> pd.DataFrame:
    """Wide invoice table with patient, referrer, and clinic resolved."""
    if invoices.empty:
        return invoices.assign(
            patient_name="",
            referral_type="",
            referral_name="",
            business_name="",
        )

    patients = patients.copy()
    patients["patient_name"] = (
        patients["first_name"].fillna("") + " " + patients["last_name"].fillna("")
    ).str.strip()

    df = (
        invoices.merge(
            patients[["patient_id", "patient_name", "referral_source_id"]],
            on="patient_id",
            how="left",
        )
        .merge(referral_sources, on="referral_source_id", how="left")
        .merge(businesses, on="business_id", how="left")
    )

    df["referral_type"] = df["referral_type"].fillna("(none)")
    df["referral_name"] = df["referral_name"].fillna("(none)")
    df["business_name"] = df["business_name"].fillna("(unknown)")
    return df


def referrer_league_table(invoice_view: pd.DataFrame) -> pd.DataFrame:
    """One row per (clinic, referral_type, referral_name): patients + revenue."""
    if invoice_view.empty:
        return pd.DataFrame(
            columns=[
                "business_name",
                "referral_type",
                "referral_name",
                "patients_referred",
                "invoices",
                "total_revenue",
                "avg_per_patient",
            ]
        )

    grouped = (
        invoice_view.groupby(
            ["business_name", "referral_type", "referral_name"], dropna=False
        )
        .agg(
            patients_referred=("patient_id", "nunique"),
            invoices=("invoice_id", "nunique"),
            total_revenue=("total_incl_tax", "sum"),
        )
        .reset_index()
    )
    grouped["avg_per_patient"] = (
        grouped["total_revenue"] / grouped["patients_referred"].replace(0, pd.NA)
    ).round(2)
    return grouped.sort_values(
        ["business_name", "total_revenue"], ascending=[True, False]
    )


def channel_rollup(invoice_view: pd.DataFrame) -> pd.DataFrame:
    """One row per (clinic, referral_type): totals across all referrer names."""
    if invoice_view.empty:
        return pd.DataFrame()
    rollup = (
        invoice_view.groupby(["business_name", "referral_type"], dropna=False)
        .agg(
            patients_referred=("patient_id", "nunique"),
            total_revenue=("total_incl_tax", "sum"),
        )
        .reset_index()
    )
    return rollup.sort_values(
        ["business_name", "total_revenue"], ascending=[True, False]
    )
