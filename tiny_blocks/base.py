import logging
from typing import Literal, Iterator
from uuid import UUID, uuid4

import pandas as pd
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


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
    Base DelayedTask class
    """

    uuid: UUID = Field(default_factory=uuid4, description="UUID")
    name: Literal["base"] = Field("base", description="Block name")
    description: str = Field(default=None, description="Description")

    def __str__(self):
        return f"Block-{self.name.capitalize()}-{self.uuid}"


class FanIn:
    def __init__(self, *generators: Iterator[pd.DataFrame]):
        self.generators = generators


class Pipe:
    """
    Represent the glue between all operations in an ETL Operation
    """

    def __init__(self, *generator: Iterator[pd.DataFrame]):
        self.generator = generator
