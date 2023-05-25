from pydantic import Field, FilePath, validator, BaseModel
from tiny_blocks.extract.base import ExtractBase
from typing import Iterator, Literal, List, Type
from pathlib import Path

import logging
import csv
import boto3
import tempfile


logger = logging.getLogger(__name__)


__all__ = ["FromCSV"]


class FromCSV(ExtractBase):
    """
    ReadCSV Block. Defines the read CSV Operation

    Basic example:
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> read_csv = FromCSV(path="/path/to/file.csv")
        >>> generator = read_csv.get_iter()
    """

    name: Literal["read_csv"] = Field(default="read_csv")
    path: FilePath = Field(..., description="Path")
    row_model: Type[BaseModel] = Field(..., description="Row model")
    headers: List[str] = Field(default=None, description="Headers")
    newline: str = Field(default="", description="Newline")

    @validator("path")
    def directory_exists(cls, path):
        if not Path(path).parent.is_dir():
            raise ValueError(f"Folder '{Path(path).parent}' does not exists")
        return path

    def get_iter(self) -> Iterator[BaseModel]:

        with open(self.path, newline=self.newline) as csvfile:
            for row in csv.DictReader(csvfile, fieldnames=self.headers):
                yield self.row_model(**row)
