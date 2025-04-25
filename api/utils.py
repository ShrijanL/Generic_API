from typing import Any, Optional

from fastapi import HTTPException
from fastapi.responses import JSONResponse
from pydantic import create_model, Field
from pydantic.config import ConfigDict
from starlette import status

from fastapi import Request
from fastapi.security import OAuth2PasswordBearer
from fastapi.security.utils import get_authorization_scheme_param


def raise_exception(
    error: str, code: str, status_code: status = status.HTTP_400_BAD_REQUEST
):
    raise HTTPException(status_code=status_code, detail={"error": error, "code": code})


class CustomOAuth2PasswordBearer(OAuth2PasswordBearer):
    """
    Inheriting `OAuth2PasswordBearer` to customize the error message
    """

    def __init__(
        self,
        tokenUrl: str,
        scheme_name: Optional[str] = None,
        scopes: Optional[dict] = None,
        description: Optional[str] = None,
        auto_error: bool = True,
    ):
        super().__init__(
            tokenUrl=tokenUrl,
            scheme_name=scheme_name,
            scopes=scopes,
            description=description,
            auto_error=auto_error,
        )

    async def __call__(self, request: Request) -> Optional[str]:
        authorization = request.headers.get("Authorization")
        scheme, param = get_authorization_scheme_param(authorization)

        # If no authorization header is provided or it's invalid
        if not authorization or scheme.lower() != "bearer":
            if self.auto_error:
                # Use the custom raise_exception function here
                raise_exception(
                    error="Authorization token is missing or invalid.",
                    code="GA-020",
                )
            else:
                return None
        return param


def error_response(
    error: str, code: str, status_code: status = status.HTTP_400_BAD_REQUEST
):
    return JSONResponse(
        status_code=status_code,
        content={
            "error": error,
            "code": code,
        },
    )


def success_response(data, message: str, status_code: status = status.HTTP_200_OK):
    return JSONResponse(
        status_code=status_code,
        content={
            "data": data,
            "message": message,
        },
    )


def convert_to_pydantic_model(model):
    # Field names are generated acc to DB name.
    # for django, db_column is taken instead of field_name in django models.
    fields = {}
    for column in model.columns:

        if column.primary_key:
            continue

        try:
            python_type = column.type.python_type
        except NotImplementedError:
            python_type = Any
        except AttributeError:
            pass

        default = ...
        if column.nullable:
            python_type = Optional[python_type]
            default = None

        # if col has a def value, make it optional.
        if column.default or column.server_default:
            python_type = Optional[python_type]
            default = None

        if column.default is not None and hasattr(column.default, "arg"):
            default = column.default.arg

        length = getattr(column.type, "length", None)

        if length:
            fields[column.name] = (python_type, Field(default, max_length=length))
        else:
            fields[column.name] = (python_type, default)

    config_dict = ConfigDict(
        title=model.name, extra="forbid", str_strip_whitespace=True
    )
    pydantic_model = create_model(model.name, **fields, __config__=config_dict)

    return pydantic_model


def configure_joins(query, joins, engine):
    for k, v in joins.items():
        key = engine.find_field(k)
        value = engine.find_field(v)

        value_table = value.table

        query = query.join(value_table, key == value)

    return query


def check_rec_to_be_updated(rec_id, session, model):
    """
    Checks if the records to be updated exists or not.
    :param model:
    :param rec_id:
    :param session:
    :return:
    """
    rec = session.query(model).filter_by(id=rec_id).first()
    if not rec:
        raise_exception(error=f"Not yet record, {rec_id}", code="GA-020")
