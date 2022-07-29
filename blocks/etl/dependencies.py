import logging
from typing import Union
import uuid
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class NoInput(BaseModel):
    """
    No dependency at all (normally extract blocks)
    """

    pass


class OneInput(BaseModel):
    """
    For most of the operations, each block depends on just one block
    """

    df: uuid.UUID = Field(..., description="UUID Block")


class TwoInputs(BaseModel):
    """
    Two blocks dependency
    """

    df_left: uuid.UUID = Field(..., description="UUID Left Block")
    df_right: uuid.UUID = Field(..., description="UUID Right Block")


# More dependency types to be added here...
Inputs = Union[NoInput, OneInput, TwoInputs]
