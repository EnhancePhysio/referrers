"""Data loading + joining layer.

Prefers ``data/*.parquet`` snapshots (populated by ``sync.py`` / GitHub
Actions) for the slow-to-fetch Cliniko resources. Falls back to live API
if a snapshot is missing.

# Cliniko's referral model (important — not obvious from field names):

``/referral_sources``        — NOT a list of referrer types. It's a
                               per-patient JUNCTION table: each record
                               links one patient to a referral_source_type
                               and optionally to a specific referrer
                               (another Patient or a Contact).

``/referral_source_types``   — the small lookup table of type names
                               ("Google", "Contact", "Patient", "Social
                               Media", "Sports Club", …).

``/contacts``                — Contacts (e.g. "Dr Smith", "Wodonga
                               Raiders"). Used as named referrers when a
                               patient's referral_source has
                               ``referrer_type == "Contact"``.

Invoices are always fetched live — they're period-specific and small
per-quarter, so snapshotting adds little.
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
REFERRAL_SOURCE_TYPES_PQ = DATA_DIR / "referral_source_types.parquet"
CONTACTS_PQ = DATA_DIR / "contacts.parquet"
BUSINESSES_PQ = DATA_DIR / "businesses.parquet"


# --- ID helpers ----------------------------------------------------------

def _id(x: Any) -> str | None:
    """Normalise any scalar id into a string (Cliniko mixes str/int)."""
    if x is None:
        return None
    return str(x)


def _link_id(obj: Any, key: str = "self") -> str | None:
    """Extract the numeric id from a Cliniko relationship field, robustly.

    Cliniko returns relationships in multiple shapes across endpoints:
    - nested object: ``{"links": {"self": "https://…/resource/123"}}``
    - plain URL string: ``"https://…/resource/123"``
    - missing / None / unexpected types

    Handles all of them without raising — one weird record shouldn't blow
    up a full-practice sync.
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
        "created_at": p.get("created_at"),
        "updated_at": p.get("updated_at"),
    }


def _business_row(b: dict) -> dict:
    return {
        "business_id": _id(b.get("id")),
        "business_name": b.get("business_name") or b.get("label") or "",
    }


def _referral_source_row(r: dict) -> dict:
    """A /referral_sources record = one patient ↔ one referrer link."""
    return {
        "referral_source_id": _id(r.get("id")),
        "patient_id": _link_id(r.get("patient")),
        "referral_source_type_id": _link_id(r.get("referral_source_type")),
        # referrer_type is "Patient", "Contact", or null. When null, the
        # type itself IS the referrer (e.g. "Google Ads" — no named
        # person/contact).
        "referrer_type": r.get("referrer_type"),
        "referrer_id": _link_id(r.get("referrer")),
        "subcategory": r.get("subcategory") or "",
        "notes": r.get("notes") or "",
    }


def _referral_source_type_row(r: dict) -> dict:
    return {
        "referral_source_type_id": _id(r.get("id")),
        "referral_type_name": (r.get("name") or "").strip() or "(blank)",
    }


def _contact_row(c: dict) -> dict:
    first = (c.get("first_name") or "").strip()
    last = (c.get("last_name") or "").strip()
    company = (c.get("company") or "").strip()
    # Prefer first+last, then company, then fall back to a placeholder.
    name = " ".join(x for x in (first, last) if x) or company or "(unnamed)"
    return {
        "contact_id": _id(c.get("id")),
        "contact_name": name,
    }


# --- Live Cliniko fetchers (used by sync.py) ----------------------------

def fetch_businesses_live(client: ClinikoClient) -> pd.DataFrame:
    return pd.DataFrame([_business_row(b) for b in client.businesses()])


def fetch_referral_sources_live(client: ClinikoClient) -> pd.DataFrame:
    return pd.DataFrame(
        [_referral_source_row(r) for r in client.referral_sources()]
    )


def fetch_referral_source_types_live(client: ClinikoClient) -> pd.DataFrame:
    return pd.DataFrame(
        [_referral_source_type_row(t) for t in client.referral_source_types()]
    )


def fetch_contacts_live(client: ClinikoClient) -> pd.DataFrame:
    return pd.DataFrame([_contact_row(c) for c in client.contacts()])


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

def _load_or_fetch(
    path: Path,
    client: ClinikoClient | None,
    live_fetcher,
) -> pd.DataFrame:
    if path.exists():
        return pd.read_parquet(path)
    if client is None:
        raise RuntimeError(
            f"{path} not found — run `python sync.py` first."
        )
    return live_fetcher(client)


def load_businesses(client: ClinikoClient | None = None) -> pd.DataFrame:
    return _load_or_fetch(BUSINESSES_PQ, client, fetch_businesses_live)


def load_referral_sources(client: ClinikoClient | None = None) -> pd.DataFrame:
    return _load_or_fetch(REFERRAL_SOURCES_PQ, client, fetch_referral_sources_live)


def load_referral_source_types(
    client: ClinikoClient | None = None,
) -> pd.DataFrame:
    return _load_or_fetch(
        REFERRAL_SOURCE_TYPES_PQ, client, fetch_referral_source_types_live
    )


def load_contacts(client: ClinikoClient | None = None) -> pd.DataFrame:
    return _load_or_fetch(CONTACTS_PQ, client, fetch_contacts_live)


def load_patients(client: ClinikoClient | None = None) -> pd.DataFrame:
    return _load_or_fetch(PATIENTS_PQ, client, fetch_patients_live)


# --- Invoices (always live) ---------------------------------------------

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
        if inv.get("deleted_at"):
            continue

        # Invoice total lives in `total_amount` (string decimal). The
        # `total_including_tax` field is on invoice ITEMS, not the invoice.
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

def _ensure_cols(df: pd.DataFrame, cols: dict[str, Any]) -> pd.DataFrame:
    """Return ``df`` with any missing columns from ``cols`` added with the
    given default value. Tolerates an entirely empty DataFrame (e.g. when a
    Cliniko endpoint returned zero rows and the parquet has no schema)."""
    out = df.copy() if not df.empty else pd.DataFrame()
    for col, default in cols.items():
        if col not in out.columns:
            out[col] = default
    return out


def _resolve_referral(
    referral_sources: pd.DataFrame,
    referral_source_types: pd.DataFrame,
    patients_named: pd.DataFrame,
    contacts: pd.DataFrame,
) -> pd.DataFrame:
    """Flatten each referral_source record into a patient-keyed table
    carrying ``referral_type`` (category name) and ``referral_name``
    (specific referrer, or same as type if no named referrer)."""
    if referral_sources.empty:
        return pd.DataFrame(
            columns=["patient_id", "referral_type", "referral_name"]
        )

    # Guard against empty / schema-less snapshots for the three lookup
    # tables. Without this, a merge on a missing column raises KeyError
    # and takes the whole dashboard down.
    referral_sources = _ensure_cols(
        referral_sources,
        {
            "patient_id": pd.NA,
            "referral_source_type_id": pd.NA,
            "referrer_type": pd.NA,
            "referrer_id": pd.NA,
            "referral_source_id": pd.NA,
        },
    )
    referral_source_types = _ensure_cols(
        referral_source_types,
        {"referral_source_type_id": pd.NA, "referral_type_name": pd.NA},
    )
    contacts = _ensure_cols(
        contacts, {"contact_id": pd.NA, "contact_name": pd.NA}
    )

    rs = referral_sources.merge(
        referral_source_types,
        on="referral_source_type_id",
        how="left",
    )
    rs["referral_type"] = rs["referral_type_name"].fillna("(unknown type)")

    # Resolve the NAME of the specific referrer based on referrer_type.
    # Merge in the contact names and patient names, then coalesce.
    patient_names = patients_named[["patient_id", "patient_name"]].rename(
        columns={"patient_id": "referrer_id", "patient_name": "_ref_patient_name"}
    )
    contact_names = contacts[["contact_id", "contact_name"]].rename(
        columns={"contact_id": "referrer_id", "contact_name": "_ref_contact_name"}
    )

    rs = rs.merge(contact_names, on="referrer_id", how="left")
    rs = rs.merge(patient_names, on="referrer_id", how="left")

    def _name(row):
        # rt may be pd.NA / None / float NaN — coerce to a plain str first
        # so the equality comparisons can't produce a non-bool value.
        rt = row.get("referrer_type")
        rt
