import logging
from uuid import UUID, uuid4
from typing import Iterator, List
import pandas as pd
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


SourceType = List[Iterator[pd.DataFrame]] | Iterator[pd.DataFrame]


class KwargsBase(BaseModel):
    """
    Base for Kwargs Models
    """

    def to_dict(self):
        return self.dict(exclude_none=True)

    def to_json(self):
        return self.json(exclude_none=True)


class BaseBlock(BaseModel):
    """
    Base Block class
    """

    uuid: UUID = Field(default_factory=uuid4, description="UUID")
    name: str = Field(..., description="Block name")
    description: str = Field(default=None, description="Description")

    def __str__(self):
        return f"Block-{self.name}-{self.uuid}"
