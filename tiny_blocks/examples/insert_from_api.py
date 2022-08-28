import logging
from functools import lru_cache
from typing import Literal, Iterator
import requests
import pandas as pd
import numpy as np
from pydantic import AnyUrl, Field
from requests.adapters import HTTPAdapter, Retry
from tiny_blocks.transform.base import KwargsTransformBase, TransformBase


__all__ = ["KwargsInsertFromAPI", "InsertFromAPI"]


logger = logging.getLogger(__name__)


class KwargsInsertFromAPI(KwargsTransformBase):
    """
    Kwargs Enrich from API source
    """

    total_retries: int = 3
    backoff_factor: int = 1
    timeout: int = 30
    default_value: str | int | float | None = None


class InsertFromAPI(TransformBase):
    """
    Enrich from API source.

    This block has been coded as example of how to enrich
    data from an API.
    """

    name: Literal["enrich_from_api"] = "enrich_from_api"
    url: AnyUrl = Field(..., description="Api URL")
    from_column: str = Field(description="Source column")
    to_column: str = Field(description="Result column")
    kwargs: KwargsInsertFromAPI = KwargsInsertFromAPI()

    def get_iter(
        self, generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        """
        Enrich from API
        """
        func = lru_cache(lambda x: self.request_api_data(x))

        for chunk in generator:
            chunk[self.to_column] = chunk[self.from_column].apply(func)
            yield chunk

    def request_api_data(self, value):
        """
        Request Data from an API

        :param value: Any value from a specific column
        :return: A value from a API
        """

        # if nan value, return default
        if np.isnan(value):
            return self.kwargs.default_value

        # implement retry strategy
        retry_strategy = Retry(
            total=self.kwargs.total_retries,
            backoff_factor=self.kwargs.backoff_factor,
        )
        retry_adapter = HTTPAdapter(max_retries=retry_strategy)

        # make a request
        with requests.Session() as session:
            session.mount("https://", retry_adapter)
            session.mount("http://", retry_adapter)
            response = session.get(
                url=self.url,
                json={"value", value},
                timeout=self.kwargs.timeout,
            )

        # return default if response is not ok
        if not response.ok:
            return self.kwargs.default_value

        # return result value
        return response.json()["result"]
