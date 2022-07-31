from pydantic import BaseModel, Field


class BaseSource(BaseModel):
    name: str = Field(..., description="Name")
    validation_schema: bool = False
