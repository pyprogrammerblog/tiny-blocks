import logging
from pydantic import BaseModel


logger = logging.getLogger(__name__)


class BaseBlock(BaseModel):
    """
    Base Block class
    """

    name: str
    version: str = "0.0.1"
    description: str = None

    def __str__(self):
        return f"Block-{self.name}-{self.version}"
