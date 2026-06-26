"""Incremental Cliniko → parquet sync, multi-account.

Runs from a GitHub Actions cron (weekly, Friday 18:00 AEST) or on-demand.
Loops over every Cliniko account defined in ``accounts.py`` and writes
their snapshots into a per-account folder:

  data/<account>/businesses.parquet         — full refresh each run
  data/<account>/referral_sources.parquet   — full refresh each run
  data/<account>/referral_source_types.parquet
  data/<account>/contacts.parquet
  data/<account>/patients.parquet           — INCREMENTAL on updated_at

Each account is independent: if (say) Mudgeeraba's key isn't in the env,
the sync logs a warning and continues with the others — Enhance still
gets its snapshot refreshed.

Environment variables (read at runtime, names defined in ``accounts.py``):
  CLINIKO_API_KEY_ENHANCE
  CLINIKO_API_KEY_MUDGEERABA
  CLINIKO_API_KEY_MULGRAVE
  CLINIKO_USER_AGENT    — optional, defaults to a sensible identifier
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pandas as pd

from accounts import ACCOUNTS, Account
from cliniko_client import ClinikoClient
from data import (
    DATA_DIR,
    fetch_businesses_live,
    fetch_contacts_live,
    fetch_patients_live,
    fetch_referral_source_types_live,
    fetch_referral_sources_live,
)

# Pull-back window (minutes) on the patient incremental watermark, to
# cover any record that was mid-write during the previous fetch.
_REWIND_MINUTES = 15


def _account_dir(account: Account) -> Path:
    p = DATA_DIR / account.label
    p.mkdir(parents=True, exist_ok=True)
    return p


def _client_for(account: Account) -> ClinikoClient | None:
    api_key = os.environ.get(account.secret_env, "").strip()
    if not api_key:
        print(
            f"  ⚠ {account.display_name}: ${account.secret_env} not set — skipping.",
            file=sys.stderr,
        )
        return None
    user_agent = os.environ.get(
        "CLINIKO_USER_AGENT",
        "EnhancePhysio-Dashboard-Sync (matt@enhance.physio)",
    )
    return ClinikoClient(api_key=api_key, user_agent=user_agent)


def _sync_simple(name: str, fetcher, client: ClinikoClient, out_path: Path) -> None:
    t0 = time.time()
    df = fetcher(client)
    df.to_parquet(out_path, index=False)
    rel = out_path.relative_to(DATA_DIR)
    print(f"    ✓ {len(df):>6} {name:<22} → {rel} ({time.time() - t0:.1f}s)")


def _sync_patients(client: ClinikoClient, out_path: Path) -> None:
    if out_path.exists():
        existing = pd.read_parquet(out_path)
    else:
        existing = pd.DataFrame()

    if not existing.empty and "updated_at" in existing.columns:
        last = pd.to_datetime(existing["updated_at"], utc=True, errors="coerce").max()
        since = (
            (last - pd.Timedelta(minutes=_REWIND_MINUTES)).strftime("%Y-%m-%dT%H:%M:%SZ")
            if not pd.isna(last)
            else None
        )
    else:
        since = None

    mode = f"incremental since {since}" if since else "FULL (first run)"
    print(f"    → patients: {mode}")
    t0 = time.time()
    new_df = fetch_patients_live(client, updated_since=since)
    elapsed = time.time() - t0

    if existing.empty:
        merged = new_df
    elif new_df.empty:
        merged = existing
    else:
        keep = existing[~existing["patient_id"].isin(new_df["patient_id"])]
        merged = pd.concat([keep, new_df], ignore_index=True)

    merged.to_parquet(out_path, index=False)
    rel = out_path.relative_to(DATA_DIR)
    print(
        f"    ✓ {len(new_df):>6} updated, {len(merged):>6} total patients "
        f"→ {rel} ({elapsed:.1f}s)"
    )


def _sync_account(account: Account) -> bool:
    """Sync one account. Returns True if it ran successfully."""
    print(f"\n=== {account.display_name} ({account.label}) ===")
    client = _client_for(account)
    if client is None:
        return False

    print(f"    shard: {client.shard}")
    out = _account_dir(account)
    try:
        _sync_simple("businesses", fetch_businesses_live, client, out / "businesses.parquet")
        _sync_simple("referral_source_types", fetch_referral_source_types_live, client, out / "referral_source_types.parquet")
        _sync_simple("contacts", fetch_contacts_live, client, out / "contacts.parquet")
        _sync_simple("referral_sources", fetch_referral_sources_live, client, out / "referral_sources.parquet")
        _sync_patients(client, out / "patients.parquet")
        return True
    except Exception as e:
        print(
            f"  ✗ {account.display_name} sync failed: {type(e).__name__}: {e}",
            file=sys.stderr,
        )
        return False


def main() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    results: dict[str, bool] = {}
    for account in ACCOUNTS:
        results[account.label] = _sync_account(account)

    print("\n=== Summary ===")
    for label, ok in results.items():
        print(f"  {'✓' if ok else '✗'} {label}")

    # Exit nonzero only if EVERY account failed. Partial failure (e.g. one
    # missing key) shouldn't block the rest of the snapshot refresh.
    if not any(results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
