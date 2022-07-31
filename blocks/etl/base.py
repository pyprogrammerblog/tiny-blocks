import logging
from uuid import UUID
from uuid import uuid4
from typing import Literal
from pydantic import BaseModel, Field

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
    name: Literal["base"] = Field("base", description="Block name")
    description: str = Field(default=None, description="Description")

    def __rshift__(self, *other):
        return other

    def __str__(self):
        return f"Block-{self.name.capitalize()}-{self.uuid}"
