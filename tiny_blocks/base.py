from pydantic import BaseModel, Field

import logging


logger = logging.getLogger(__name__)


class Row(dict):
    def columns(self):
        return list(self.keys())


class BaseBlock(BaseModel):
    """
    Base Block class
    """

    name: str = Field(..., description="Block name")
    version: str = Field(default="v1", description="Version Block")
    description: str = Field(default=None, description="Description")

    def __str__(self):
        return f"Block-{self.name}"
