"""Urlscan connector"""

from __future__ import annotations

import logging
from collections import Counter
from pathlib import Path
from typing import Iterator, NamedTuple
from urllib.parse import urlparse

import stix2
import validators
import yaml
from pycti import Indicator, StixCoreRelationship
from pycti.connector.opencti_connector_helper import (
    OpenCTIConnectorHelper,
    get_config_variable,
)
from stix2.v21 import _Observable as Observable  # noqa

from .client import UrlscanClient  # updated client import
import requests  # for catching HTTPError below


class UrlscanConnector:
    def __init__(self):
        # load config and helper
        config_file_path = Path(__file__).parent.parent / "config.yml"
        config = yaml.safe_load(config_file_path.read_text())
        self._helper = OpenCTIConnectorHelper(config)

        # fetch core settings
        interval = get_config_variable(
            "CONNECTOR_INTERVAL",
            ["connector", "interval"],
            config,
            default=86400,
            isNumber=True,
        )
        lookback = get_config_variable(
            "CONNECTOR_LOOKBACK",
            ["connector", "lookback"],
            config,
            default=3,
            isNumber=True,
        )
        urlscan_url = get_config_variable(
            "URLSCAN_API_URL", ["urlscan", "url"], config
        )
        urlscan_api_key = get_config_variable(
            "URLSCAN_API_KEY", ["urlscan", "api_key"], config
        )

        # instantiate client with timeouts built in
        self._client = UrlscanClient(urlscan_url, urlscan_api_key)

        # start the internal scheduling loop
        self._loop = ConnectorLoop(
            helper=self._helper,
            process_function=self._process_feed,
            interval=interval,
            lookback=lookback,
        )
        self._loop.start()
        self._loop.join()

    def _process_feed(self, work_id: str, date_math: str) -> None:
        """Process the external connector feed
        :param work_id: Work ID
        :param date_math: Date math string
        """
        bundle_objects = []

        # if client.query raises 403, catch and mark the work as processed
        try:
            results: Iterator[str] = self._client.query(date_math=date_math)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 403:
                logging.getLogger(__name__).warning(
                    "UrlscanConnector: Forbidden (403) accessing feed; skipping this run"
                )
                self._helper.api.work.to_processed(
                    work_id, "Skipped enterprise-only feed", False
                )
                return
            raise

        # for each URL returned, create observables/indicators
        for url in results:
            obs = self._create_url_observable(url)
            if obs:
                bundle_objects.extend(obs)

        if bundle_objects:
            self._helper.send_bundle(bundle_objects)
        # mark work as done
        self._helper.api.work.to_processed(work_id, "Done", False)

    # ... rest of the file unchanged, e.g., _create_url_observable, helpers, etc. ...

class Observation(NamedTuple):
    """Result from making an observable"""

    observable: Observable
    indicator: stix2.Indicator = None
    relationship: stix2.Relationship = None
