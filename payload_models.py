from enum import Enum
from typing import Optional, List, Any, Dict, Union

from pydantic import BaseModel, Field, field_validator, JsonValue


## Fetch
class OperationByEnum(str, Enum):
    OR = "or"
    AND = "and"


class OperatorByEnum(str, Enum):
    EQ = "eq"
    IN = "in"
    NOT = "not"
    GT = "gt"
    lt = "lt"
    LIKE = "like"
    ILIKE = "ilike"


class OrderByEnum(str, Enum):
    asc = "asc"
    desc = "desc"


class FetchSort(BaseModel):
    field: str
    order_by: OrderByEnum


class FetchFilter(BaseModel):
    operator: OperatorByEnum
    name: str
    value: List[Any]
    operation: Optional[OperationByEnum] = OperationByEnum.AND  # default value


class FetchInnerPayload(BaseModel):
    modelName: str
    fields: List[str]
    filters: List[FetchFilter]
    joins: Optional[List[Dict]] = []
    pageNumber: Optional[int] = Field(default=1, ge=1)
    pageSize: Optional[int] = Field(default=10, ge=1, le=100)
    sort: Optional[FetchSort] = None
    distinct: Optional[bool] = False

    @field_validator("filters")
    def validate_filters(cls, v):
        if v:
            for f in v:
                value = getattr(f, "value", [])
                len_value = len(value)
                operator = getattr(f, "operator", "")

                if len_value < 1:
                    raise ValueError("Filters must have at least one value")
                elif len_value > 1 and operator != OperatorByEnum.IN:
                    raise ValueError("Multiple filters not supported")
        return v


class FetchPayload(BaseModel):
    # payload: Dict[str,FetchInnerPayload]
    payload: FetchInnerPayload


## Save
class SaveInnerPayload(BaseModel):
    modelName: str
    id: Optional[Union[int, str]] = None
    saveInput: JsonValue


class SavePayload(BaseModel):
    payload: SaveInnerPayload
