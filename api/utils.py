from typing import Any, Optional

from fastapi import HTTPException
from fastapi.responses import JSONResponse
from pydantic import create_model, Field
from pydantic.config import ConfigDict
from starlette import status


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


def success_response(data: str, message: str, status_code: status = status.HTTP_200_OK):
    return JSONResponse(
        status_code=status_code,
        content={
            "data": data,
            "message": message,
        },
    )


def raise_exception(
    error: str, code: str, status_code: status = status.HTTP_400_BAD_REQUEST
):
    raise HTTPException(status_code=status_code, detail={"error": error, "code": code})


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
