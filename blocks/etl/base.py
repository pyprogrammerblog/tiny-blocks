import abc
import logging
from uuid import UUID
from uuid import uuid4
from typing import Literal
from typing import Iterable

import pandas as pd
from pydantic import BaseModel, Field
from blocks.dependencies import Inputs

logger = logging.getLogger(__name__)


class KwargsBase(BaseModel):
    """
    Base for Kwargs Models
    """

    def to_dict(self):
        return self.dict(exclude_unset=True)

    def to_json(self):
        return self.json(exclude_unset=True)


class BaseBlock(BaseModel):
    """
    Base DelayedTask class
    """
    uuid: UUID = Field(default_factory=uuid4, description="UUID")
    name: Literal["base"] = "base"
    description: str = Field(default=None, description="Description")
    input: Inputs = Field(default_factory=dict, description="Input Blocks")

    def __rshift__(self, *other):
        return other

    def __iter__(self):
        for chunk in self.process():
            yield chunk

    def __next__(self):
        pass

    def __str__(self):
        return f"{self.uuid}-{self.name.capitalize()}-Block"

    def __call__(self, **kwargs):
        return self.process(**kwargs)

    @abc.abstractmethod
    def get_iter(self, **kwargs) -> Iterable[pd.DataFrame]:
        raise NotImplementedError

    @abc.abstractmethod
    def process(self, **kwargs) -> pd.DataFrame:
        raise NotImplementedError
