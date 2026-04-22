"""Data loading + joining layer.

Pulls from the Cliniko API via ClinikoClient and assembles the DataFrames the
dashboard actually uses: invoices-with-referrer-and-clinic, and a
referrer-level summary.
"""
from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd

from cliniko_client import ClinikoClient


# Cliniko API returns IDs as strings in "links" but as ints in embedded objects.
# Normalise everywhere to str for safe joins.
def _id(x: Any) -> str | None:
    if x is None:
        return None
    return str(x)


def _link_id(obj: dict | None, key: str = "self") -> str | None:
    """Extract the numeric id from a Cliniko "links" object."""
    if not obj:
        return None
    link = obj.get("links", {}).get(key)
    if not link:
        return None
    return link.rsplit("/", 1)[-1]


def load_businesses(client: ClinikoClient) -> pd.DataFrame:
    rows = client.businesses()
    return pd.DataFrame(
        [
            {
                "business_id": _id(b["id"]),
                "business_name": b.get("business_name") or b.get("label") or "",
            }
            for b in rows
        ]
    )


def load_referral_sources(client: ClinikoClient) -> pd.DataFrame:
    rows = client.referral_sources()
    return pd.DataFrame(
        [
            {
                "referral_source_id": _id(r["id"]),
                "referral_type": r.get("referral_source_type", {}).get("name")
                or r.get("type")
                or "",
                "referral_name": (r.get("name") or "").strip() or "(blank)",
            }
            for r in rows
        ]
    )


def _patient_row(p: dict) -> dict:
    return {
        "patient_id": _id(p["id"]),
        "first_name": p.get("first_name", ""),
        "last_name": p.get("last_name", ""),
        "referral_source_id": _link_id(p.get("referral_source")),
        "created_at": p.get("created_at"),
    }


def load_patients(client: ClinikoClient) -> pd.DataFrame:
    """Load every patient. Slow on large practices — prefer
    ``load_patients_by_ids`` when you only need a subset."""
    return pd.DataFrame([_patient_row(p) for p in client.patients()])


def load_patients_by_ids(
    client: ClinikoClient,
    patient_ids: list[str],
) -> pd.DataFrame:
    """Fetch only the patients we actually need (those with invoices in the
    selected period). Much faster than ``load_patients`` for a quarterly
    view, as long as the set is a small fraction of the full patient list."""
    rows = []
    for pid in patient_ids:
        if not pid:
            continue
        try:
            rows.append(_patient_row(client.get(f"patients/{pid}")))
        except Exception:
            # If a patient lookup fails (e.g. archived + inaccessible),
            # skip — their invoice will still appear with a blank referrer.
            continue
    return pd.DataFrame(rows)


def load_invoices(
    client: ClinikoClient,
    start: date,
    end: date,
) -> pd.DataFrame:
    rows = client.invoices(start.isoformat(), end.isoformat())
    out = []
    for inv in rows:
        out.append(
            {
                "invoice_id": _id(inv["id"]),
                "invoice_number": inv.get("number"),
                "issue_date": inv.get("issue_date"),
                "total_incl_tax": float(inv.get("total_including_tax") or 0),
                "patient_id": _link_id(inv.get("patient")),
                "business_id": _link_id(inv.get("business")),
                "status": inv.get("status"),
            }
        )
    df = pd.DataFrame(out)
    if not df.empty:
        df["issue_date"] = pd.to_datetime(df["issue_date"]).dt.date
    return df


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

    df = invoices.merge(
        patients[["patient_id", "patient_name", "referral_source_id"]],
        on="patient_id",
        how="left",
    ).merge(
        referral_sources,
        on="referral_source_id",
        how="left",
    ).merge(
        businesses,
        on="business_id",
        how="left",
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
