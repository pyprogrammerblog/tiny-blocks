import logging
from typing import Literal

from pydantic import Field
from smart_stream.models.blocks.extract.base import check_types
from smart_stream.models.sources.json import JSONSource
import pandas as pd
from smart_stream.models.blocks.extract.base import (
    KwargsExtractBlock,
    ExtractBlock,
)

__all__ = ["ReadJSONBlock"]


logger = logging.getLogger(__name__)


class KwargsDelayedReadJSON(KwargsExtractBlock):
    """
    Kwargs for ReadJSON
    """

    orient: str = "records"
    lines: str = None
    storage_options: dict = None
    blocksize: int = None
    sample: int = None
    encoding: str = "utf-8"
    errors: Literal["strict"] = "strict"


class KwargsReadJSON(KwargsExtractBlock):
    """
    Kwargs for ReadJSON
    """

    orient: Literal[
        "split", "records", "index", "columns", "values"
    ] = "records"
    chucksize: int = None
    storage_options: dict = None
    nrows: int = None


class ReadJSONBlock(ExtractBlock):
    """
    ReadJSON Block
    """

    name: Literal["read_json"]
    kwargs: KwargsReadJSON = KwargsReadJSON()
    kwargs_delayed: KwargsDelayedReadJSON = KwargsDelayedReadJSON()
    source: JSONSource = Field(..., description="Source Data")

    @check_types
    def delayed(self) -> dd.DataFrame:
        """
        Read JSON
        """
        kwargs = self.kwargs_delayed.to_dict() | self.kwargs.to_dict()
        ddf = dd.read_json(self.source.path, **kwargs)
        return ddf

    @check_types
    def dispatch(self) -> pd.DataFrame:
        """
        Read JSON
        """
        kwargs = self.kwargs.to_dict()
        df = pd.read_json(self.source.path, **kwargs)
        return df
