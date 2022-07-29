import logging
from typing import Literal, Iterator

from pydantic import Field
from blocks.etl.extract.base import check_types
from blocks.sources.json import JSONSource
import pandas as pd
from blocks.etl.extract.base import (
    KwargsExtractBlock,
    ExtractBlock,
)

__all__ = ["ReadJSONBlock"]


logger = logging.getLogger(__name__)


class KwargsReadJSON(KwargsExtractBlock):
    """
    Kwargs for ReadJSON
    """

    orient: Literal[
        "split", "records", "index", "columns", "values"
    ] = "records"
    storage_options: dict = None
    nrows: int = None


class ReadJSONBlock(ExtractBlock):
    """
    ReadJSON Block
    """

    name: Literal["read_json"]
    kwargs: KwargsReadJSON = KwargsReadJSON()
    source: JSONSource = Field(..., description="Source Data")

    @check_types
    def get_iter(self, chunksize: int = 10000) -> Iterator[pd.DataFrame]:
        kwargs = self.kwargs.to_dict() | {"chunksize": chunksize}
        for chunk in pd.read_json(self.source.path, **kwargs):
            yield chunk

    @check_types
    def process(self) -> pd.DataFrame:
        """
        Read JSON
        """
        kwargs = self.kwargs.to_dict()
        df = pd.read_json(self.source.path, **kwargs)
        return df
