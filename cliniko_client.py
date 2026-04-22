"""Cliniko API client.

Handles authentication, pagination, and rate limiting for the Cliniko REST API.
Docs: https://docs.api.cliniko.com/
"""
from __future__ import annotations

import time
from typing import Iterator

import requests


class ClinikoClient:
    """Minimal Cliniko API client with pagination and basic rate-limit handling."""

    # Valid Cliniko shards — used to catch placeholder values and typos.
    _VALID_SHARDS = {
        "au1", "au2", "au3", "au4",
        "uk1", "uk2",
        "us1",
        "ca1",
    }

    def __init__(self, api_key: str, user_agent: str, shard: str | None = None):
        # Strip whitespace — API keys pasted into Streamlit Secrets sometimes
        # pick up a trailing newline or space, which breaks shard derivation.
        api_key = (api_key or "").strip()
        if not api_key:
            raise ValueError("cliniko_api_key is empty.")

        # Catch the common mistake of leaving the placeholder from the
        # secrets.toml template in place.
        if "PASTE" in api_key.upper() or "YOUR" in api_key.upper():
            raise ValueError(
                "cliniko_api_key still contains placeholder text "
                "(e.g. 'PASTE-YOUR-NEW-CLINIKO-KEY-HERE'). "
                "Paste your real Cliniko API key into Streamlit Secrets."
            )

        # Cliniko API keys are of the form "<key>-<shard>" e.g. "...-au1".
        # Derive shard from the key if not explicitly provided.
        if shard is None:
            if "-" not in api_key:
                raise ValueError(
                    "Could not derive shard from API key. "
                    "Pass shard explicitly (e.g. 'au1')."
                )
            shard = api_key.rsplit("-", 1)[-1].strip().lower()

        if shard not in self._VALID_SHARDS:
            raise ValueError(
                f"Derived Cliniko shard '{shard}' is not a known shard "
                f"(expected one of {sorted(self._VALID_SHARDS)}). "
                "Check that your cliniko_api_key in Streamlit Secrets is "
                "a real key and ends in the shard code, e.g. '...-au1'."
            )

        self.shard = shard
        self.base_url = f"https://api.{shard}.cliniko.com/v1"
        self.session = requests.Session()
        self.session.auth = (api_key, "")
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": user_agent,
            }
        )

    # Inter-request delay (seconds) — stays well under Cliniko's ~200/min
    # rate limit without being slow. Set to 0 to disable.
    _INTER_REQUEST_DELAY = 0.1

    # Max retries for a single request before giving up.
    _MAX_RETRIES = 10

    def _request_with_retry(self, url: str, params: dict | None = None):
        """GET with robust 429 + transient-error handling."""
        last_exc: Exception | None = None
        for attempt in range(self._MAX_RETRIES):
            try:
                resp = self.session.get(url, params=params, timeout=30)
            except (requests.ConnectionError, requests.Timeout) as e:
                last_exc = e
                time.sleep(min(2 ** attempt, 30))
                continue

            if resp.status_code == 429:
                # Honour Retry-After; fall back to exponential backoff.
                retry_after = resp.headers.get("Retry-After")
                sleep_for = (
                    int(retry_after)
                    if retry_after and retry_after.isdigit()
                    else min(2 ** attempt + 1, 30)
                )
                time.sleep(sleep_for)
                continue

            if 500 <= resp.status_code < 600:
                # Transient server error — back off and retry.
                time.sleep(min(2 ** attempt, 30))
                continue

            resp.raise_for_status()
            if self._INTER_REQUEST_DELAY:
                time.sleep(self._INTER_REQUEST_DELAY)
            return resp

        # Exhausted retries.
        if last_exc:
            raise last_exc
        resp.raise_for_status()  # type: ignore[possibly-unbound]

    def get(self, path: str, params: dict | None = None) -> dict:
        """GET a single resource / page."""
        url = f"{self.base_url}/{path.lstrip('/')}"
        return self._request_with_retry(url, params).json()

    def paginate(
        self,
        path: str,
        params: dict | None = None,
        page_size: int = 100,
    ) -> Iterator[dict]:
        """Yield every record across every page of a Cliniko list endpoint."""
        params = {**(params or {}), "per_page": page_size}
        url = f"{self.base_url}/{path.lstrip('/')}"

        while url:
            resp = self._request_with_retry(url, params)
            payload = resp.json()
            # The "collection" key in the payload is named after the resource,
            # and is the only list-valued top-level key.
            collection_key = next(
                (k for k, v in payload.items() if isinstance(v, list)),
                None,
            )
            if collection_key is None:
                return
            for item in payload[collection_key]:
                yield item

            # Follow the "next" link for the next page; the next link already
            # carries all the query params we need.
            url = payload.get("links", {}).get("next")
            params = None

    # --- Convenience wrappers -------------------------------------------------

    def businesses(self) -> list[dict]:
        return list(self.paginate("businesses"))

    def referral_sources(self) -> list[dict]:
        return list(self.paginate("referral_sources"))

    def patients(self) -> list[dict]:
        """All patients (including archived — their invoice history still counts)."""
        return list(self.paginate("patients"))

    def patient(self, patient_id: str) -> dict:
        """Fetch a single patient by ID."""
        return self.get(f"patients/{patient_id}")

    def invoices(self, issued_from: str, issued_to: str) -> list[dict]:
        """Invoices issued between two dates (YYYY-MM-DD, inclusive)."""
        params = {
            "q[]": [
                f"issue_date:>={issued_from}",
                f"issue_date:<={issued_to}",
            ]
        }
        return list(self.paginate("invoices", params=params))
