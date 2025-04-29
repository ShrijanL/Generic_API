from enum import Enum
from typing import Optional, List, Any, Dict, Union

from pydantic import (
    ConfigDict,
    BaseModel,
    Field,
    field_validator,
    JsonValue,
    SecretStr,
    EmailStr,
)


class PydanticConfigV1:
    """
    Custom configuration for pydantic objects.
    """

    model_config = ConfigDict(
        extra="forbid",  # Forbid extra fields
        str_strip_whitespace=True,  # Remove white spaces
    )


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


class FetchSort(BaseModel, PydanticConfigV1):
    field: str
    order_by: OrderByEnum


class FetchFilter(BaseModel, PydanticConfigV1):
    operator: OperatorByEnum
    name: str
    value: List[Any]
    operation: Optional[OperationByEnum] = OperationByEnum.AND  # default value


class FetchInnerPayload(BaseModel, PydanticConfigV1):
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

    @field_validator("modelName")
    def validate_modelName(cls, v):
        if v:
            if not "." in v or v.count(".") != 1:
                raise ValueError("modelName must be in format as 'db.table'.")
        return v


class FetchPayload(BaseModel, PydanticConfigV1):
    # payload: Dict[str,FetchInnerPayload]
    payload: FetchInnerPayload


## Save
class SaveInnerPayload(BaseModel, PydanticConfigV1):
    modelName: str
    id: Optional[Union[int, str]] = None
    saveInput: JsonValue

    @field_validator("modelName")
    def validate_modelName(cls, v):
        if v:
            if not "." in v or v.count(".") != 1:
                raise ValueError("modelName must be in format as 'db.table'.")
        return v


class SavePayload(BaseModel, PydanticConfigV1):
    payload: SaveInnerPayload


# Login
class LoginInnerPayload(BaseModel, PydanticConfigV1):
    email: EmailStr
    password: SecretStr


class LoginPayload(BaseModel, PydanticConfigV1):
    payload: LoginInnerPayload


## Delete
class DeleteInnerPayload(BaseModel, PydanticConfigV1):
    modelName: str
    filters: List[FetchFilter]

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

class DeletePayload(BaseModel, PydanticConfigV1):
    payload: DeleteInnerPayload