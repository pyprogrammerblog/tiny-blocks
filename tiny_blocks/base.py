from uuid import UUID, uuid4
from pydantic import BaseModel, Field
import logging


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
    Base Block class
    """

    uuid: UUID = Field(default_factory=uuid4, description="UUID")
    name: str = Field(..., description="Block name")
    version: str = Field(default="v1", description="Version Block")
    description: str = Field(default=None, description="Description")

    def __str__(self):
        return f"Block-{self.name}-{self.uuid}"
