"""Incremental Cliniko → parquet sync.

Designed to run from a GitHub Actions cron (weekly, Friday 18:00 AEST) or
on-demand (locally, or via "Run workflow" in GitHub). Produces three
snapshot files the dashboard reads at runtime:

  data/businesses.parquet         — full refresh each run (tiny)
  data/referral_sources.parquet   — full refresh each run (~8k rows)
  data/patients.parquet           — INCREMENTAL: fetches only patients
                                    with updated_at >= last sync watermark,
                                    merges into the existing snapshot.

First run bootstraps everything from scratch (slow — maybe 5–10 minutes).
Subsequent runs typically fetch tens to a few hundred updated patients and
complete in seconds.

Environment variables (read at runtime):
  CLINIKO_API_KEY       — required, the Cliniko API key (ends in "-au1")
  CLINIKO_USER_AGENT    — optional, defaults to a sensible identifier
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pandas as pd

from cliniko_client import ClinikoClient
from data import (
    DATA_DIR,
    PATIENTS_PQ,
    REFERRAL_SOURCES_PQ,
    BUSINESSES_PQ,
    fetch_businesses_live,
    fetch_patients_live,
    fetch_referral_sources_live,
)

# How far back to rewind the incremental watermark each run, as a guard
# against clock skew and any patients modified in the narrow window
# between our previous fetch's start and finish.
_REWIND_MINUTES = 15


def _client_from_env() -> ClinikoClient:
    api_key = os.environ.get("CLINIKO_API_KEY", "").strip()
    if not api_key:
        print("ERROR: CLINIKO_API_KEY env var not set.", file=sys.stderr)
        sys.exit(1)
    user_agent = os.environ.get(
        "CLINIKO_USER_AGENT",
        "EnhancePhysio-Dashboard-Sync (matt@enhance.physio)",
    )
    return ClinikoClient(api_key=api_key, user_agent=user_agent)


def _sync_businesses(client: ClinikoClient) -> None:
    print("→ Fetching businesses…")
    t0 = time.time()
    df = fetch_businesses_live(client)
    df.to_parquet(BUSINESSES_PQ, index=False)
    print(f"  ✓ {len(df)} businesses → {BUSINESSES_PQ.name} ({time.time() - t0:.1f}s)")


def _sync_referral_sources(client: ClinikoClient) -> None:
    print("→ Fetching referral sources…")
    t0 = time.time()
    df = fetch_referral_sources_live(client)
    df.to_parquet(REFERRAL_SOURCES_PQ, index=False)
    print(
        f"  ✓ {len(df)} referral sources → {REFERRAL_SOURCES_PQ.name} "
        f"({time.time() - t0:.1f}s)"
    )


def _sync_patients(client: ClinikoClient) -> None:
    if PATIENTS_PQ.exists():
        existing = pd.read_parquet(PATIENTS_PQ)
    else:
        existing = pd.DataFrame()

    if not existing.empty and "updated_at" in existing.columns:
        last = pd.to_datetime(existing["updated_at"], utc=True, errors="coerce").max()
        if pd.isna(last):
            since = None
        else:
            since = (last - pd.Timedelta(minutes=_REWIND_MINUTES)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
    else:
        since = None

    if since:
        print(f"→ Incremental: fetching patients updated since {since}…")
    else:
        print("→ First run: fetching ALL patients (this may take several minutes)…")

    t0 = time.time()
    new_df = fetch_patients_live(client, updated_since=since)
    elapsed = time.time() - t0
    print(f"  ✓ Fetched {len(new_df)} patient records ({elapsed:.1f}s)")

    if existing.empty:
        merged = new_df
    elif new_df.empty:
        merged = existing
    else:
        # Drop any rows in the existing snapshot that have been superseded
        # by an update in this run, then append the fresh rows.
        keep = existing[~existing["patient_id"].isin(new_df["patient_id"])]
        merged = pd.concat([keep, new_df], ignore_index=True)

    merged.to_parquet(PATIENTS_PQ, index=False)
    print(f"  ✓ Wrote {len(merged)} total patients → {PATIENTS_PQ.name}")


def main() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    client = _client_from_env()

    _sync_businesses(client)
    _sync_referral_sources(client)
    _sync_patients(client)

    print("✓ Sync complete.")


if __name__ == "__main__":
    main()
