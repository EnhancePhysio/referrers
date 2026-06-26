"""Single source of truth for which Cliniko accounts the dashboard pulls.

Add a new clinic-group account in three steps:
  1. Add an entry to ``ACCOUNTS`` below (label is used for the data folder).
  2. Add the API key to GitHub Actions Secrets under the named env var
     (so ``sync.py`` running in the workflow can read it).
  3. Add the same key to Streamlit Cloud Secrets under the same name
     (so ``app.py``'s live-invoice path can read it).
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Account:
    label: str          # folder name + internal id; lowercase, no spaces
    display_name: str   # shown in UI / debug
    secret_env: str     # env var (GitHub) AND Streamlit secrets key


ACCOUNTS: list[Account] = [
    Account(
        label="enhance",
        display_name="Enhance Physio (Albury / Wodonga / Lavington)",
        secret_env="CLINIKO_API_KEY_ENHANCE",
    ),
    Account(
        label="mudgeeraba",
        display_name="Mudgeeraba",
        secret_env="CLINIKO_API_KEY_MUDGEERABA",
    ),
    Account(
        label="mulgrave",
        display_name="Mulgrave",
        secret_env="CLINIKO_API_KEY_MULGRAVE",
    ),
]


def account_by_label(label: str) -> Account:
    for a in ACCOUNTS:
        if a.label == label:
            return a
    raise KeyError(f"Unknown account label: {label!r}")
