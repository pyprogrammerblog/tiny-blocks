import logging
from typing import Literal

import dask.dataframe as dd
from smart_stream.models.sinks.json import JSONSink
import pandas as pd
from smart_stream.models.blocks.load.base import LoadBlock, KwargsLoadBlock

__all__ = ["WriteJSONBlock", "KwargsWriteJSON", "KwargsDelayedWriteJSON"]

logger = logging.getLogger(__name__)


class KwargsWriteJSON(KwargsLoadBlock):
    """
    Kwargs for WriteJSON Block
    """

    pass


class KwargsDelayedWriteJSON(KwargsLoadBlock):
    """
    Kwargs for WriteJSON Block
    """

    pass


class WriteJSONBlock(LoadBlock):
    """
    WriteJSON Block
    """

    name: Literal["to_json"]
    kwargs: KwargsWriteJSON = KwargsWriteJSON()
    kwargs_delayed: KwargsDelayedWriteJSON = KwargsDelayedWriteJSON()
    sink: JSONSink

    def delayed(self, block: dd.DataFrame):
        """
        Write JSON operation
        """
        kwargs = self.kwargs.to_dict() | self.kwargs_delayed.to_dict()
        return block.to_json(self.sink.path, **kwargs)

    def dispatch(self, block: pd.DataFrame):
        """
        Write JSON Operation
        """
        kwargs = self.kwargs.to_dict()
        return block.to_json(self.sink.path, **kwargs)
