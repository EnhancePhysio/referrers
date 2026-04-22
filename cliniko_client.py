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

    def __init__(self, api_key: str, user_agent: str, shard: str | None = None):
        # Cliniko API keys are of the form "<key>-<shard>" e.g. "...-au1".
        # Derive shard from the key if not explicitly provided.
        if shard is None:
            if "-" not in api_key:
                raise ValueError(
                    "Could not derive shard from API key. "
                    "Pass shard explicitly (e.g. 'au1')."
                )
            shard = api_key.rsplit("-", 1)[-1]

        self.base_url = f"https://api.{shard}.cliniko.com/v1"
        self.session = requests.Session()
        self.session.auth = (api_key, "")
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": user_agent,
            }
        )

    def get(self, path: str, params: dict | None = None) -> dict:
        """GET a single resource / page. Retries once on 429."""
        url = f"{self.base_url}/{path.lstrip('/')}"
        for attempt in range(3):
            resp = self.session.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                # Rate-limited — honour Retry-After if present.
                retry_after = int(resp.headers.get("Retry-After", "5"))
                time.sleep(retry_after)
                continue
            resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()
        return {}

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
            for attempt in range(3):
                resp = self.session.get(url, params=params, timeout=30)
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", "5"))
                    time.sleep(retry_after)
                    continue
                resp.raise_for_status()
                break
            else:
                resp.raise_for_status()

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

            # Follow the "next" link for the next page; params already in the URL.
            url = payload.get("links", {}).get("next")
            params = None

    # --- Convenience wrappers -------------------------------------------------

    def businesses(self) -> list[dict]:
        return list(self.paginate("businesses"))

    def referral_sources(self) -> list[dict]:
        return list(self.paginate("referral_sources"))

    def patients(self, archived: bool = False) -> list[dict]:
        params = {} if archived else {"q[]": "archived_at:blank"}
        return list(self.paginate("patients", params=params))

    def invoices(self, issued_from: str, issued_to: str) -> list[dict]:
        """Invoices issued between two dates (YYYY-MM-DD, inclusive)."""
        params = {
            "q[]": [
                f"issue_date:>={issued_from}",
                f"issue_date:<={issued_to}",
            ]
        }
        return list(self.paginate("invoices", params=params))
