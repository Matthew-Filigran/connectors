"""Urlscan client"""

from typing import Iterator, List
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests
import logging

from pydantic.v1 import BaseModel, parse_raw_as

__all__ = [
    "UrlscanClient",
]

class UrlscanClient:
    """Urlscan.io client"""

    def __init__(self, url: str, api_key: str):
        """Initializer.
        :param url: Urlscan URL
        :param api_key: Urlscan api key
        """
        self._url = url
        self._api_key = api_key

        if not url:
            raise ValueError("Urlscan URL must be set")

        if not api_key:
            raise ValueError("Urlscan API key must be set")

        # Fail-fast on slow endpoints
        self.request_timeout = (5, 30)  # 5s connect, 30s read
        self.logger = logging.getLogger(__name__)

    def query(self, date_math: str) -> Iterator[str]:
        """Process the feed URL and return any indicators.
        :param date_math: Date math string for the feed.
        :return: Feed results.
        """
        # if date_math already in url, remove it
        parsed_url = urlparse(self._url)
        query_params = parse_qs(parsed_url.query)
        query_params["q"] = [f"date:>{date_math}"]
        new_query = urlencode(query_params, doseq=True)
        updated_url = urlunparse(parsed_url._replace(query=new_query))

        # Update the date_math in the query parameters
        try:
            resp = requests.get(
                updated_url,
                headers={"API-key": self._api_key},
                timeout=self.request_timeout,
            )
            resp.raise_for_status()
        except requests.exceptions.Timeout:
            self.logger.warning(
                "UrlscanClient timeout fetching %s; skipping", updated_url
            )
            return iter(())
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 403:
                self.logger.warning(
                    "UrlscanClient forbidden (403) fetching %s; skipping enterprise feed",
                    updated_url,
                )
                return iter(())
            raise

        # parse and yield all page_urls
        parsed = parse_raw_as(UrlscanResponse, resp.text)
        for result in parsed.results:
            yield result.page_url


class UrlscanResult(BaseModel):
    """Urlscan result"""

    page_url: str


class UrlscanResponse(BaseModel):
    """Urlscan response"""

    results: List[UrlscanResult]
