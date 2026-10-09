"""A small, polite JSON client: identifies itself, spaces requests, retries transient failures."""

from __future__ import annotations

import time
from typing import Any

import requests

from qcommerce import log

logger = log.get(__name__)

USER_AGENT = (
    "quick-commerce-cdc-lakehouse/0.2 (+https://github.com/rohit91jacob/quick-commerce-cdc-lakehouse)"
)
RETRY_STATUS = {429, 500, 502, 503, 504}


class FetchError(RuntimeError):
    pass


class PoliteClient:
    def __init__(
        self,
        *,
        min_interval_s: float = 0.5,
        timeout_s: float = 30.0,
        retries: int = 4,
        extra_retry_status: set[int] | None = None,
    ) -> None:
        self.retry_status = RETRY_STATUS | (extra_retry_status or set())
        self.min_interval_s = min_interval_s
        self.timeout_s = timeout_s
        self.retries = retries
        self._last = 0.0
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})

    def get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        for attempt in range(self.retries + 1):
            wait = self._last + self.min_interval_s - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()
            try:
                response = self._session.get(url, params=params, timeout=self.timeout_s)
            except requests.RequestException as exc:
                error: str = str(exc)
            else:
                if response.status_code == 200:
                    return response.json()
                error = f"HTTP {response.status_code}"
                if response.status_code not in self.retry_status:
                    raise FetchError(f"GET {url}: {error}")
            if attempt < self.retries:
                backoff = min(60.0, 2.0**attempt)
                logger.warning("fetch retry", extra={"url": url, "error": error, "sleep_s": backoff})
                time.sleep(backoff)
        raise FetchError(f"GET {url}: {error} after {self.retries + 1} attempts")
