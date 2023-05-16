from pydantic import Field, FilePath, validator, BaseModel
from tiny_blocks.extract.base import ExtractBase
from typing import Iterator, Literal, List
from tiny_blocks.base import Row
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
    headers: List[str] = Field(default=None)
    newline: str = Field(default="")

    @validator("path")
    def directory_exists(cls, path):
        if not Path(path).parent.is_dir():
            raise ValueError(f"Folder '{Path(path).parent}' does not exists")
        return path

    def get_iter(self) -> Iterator[Row]:

        with open(self.path, newline=self.newline) as csvfile:
            for row in csv.DictReader(csvfile, fieldnames=self.headers):
                yield Row(row)


class FromS3CSV(ExtractBase):
    """
    ReadS3CSV Block. Defines the read CSV Operation from S3

    Basic example:
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> read_csv = FromCSV(key='key', bucket="bucket", s3_config={...})
        >>> generator = read_csv.get_iter()
    """

    name: Literal["read_csv"] = Field(default="read_csv")
    s3_config: dict = Field(..., description="S3 configuration")
    bucket: str = Field(..., description="Bucket name")
    key: str = Field(..., description="Key Object")
    headers: List[str] = Field(default=None)

    def get_iter(self) -> Iterator[Row]:

        with tempfile.NamedTemporaryFile(mode="r", encoding="utf-8") as f:
            s3 = boto3.resource("s3", **self.s3_config)
            s3.download_fileobj(self.bucket, self.key, f)

            for row in csv.DictReader(f, fieldnames=self.headers):
                yield Row(row)
