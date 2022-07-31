from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)


__all__ = ["BaseSink"]


class BaseSink(BaseModel):
    pass
