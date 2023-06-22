import uuid

from pydantic import BaseModel, Field

import logging


logger = logging.getLogger(__name__)


class BaseBlock:
    """
    Base Block class
    """

    def __init__(self, version: float = 1.0, description: str = None):
        """
        Constructor
        """
        self.uuid = uuid.uuid4()
        self.name = self.__class__.__name__
        self.version = version
        self.description = description

    def __str__(self):
        return f"Block-{self.name}"
