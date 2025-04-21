from typing import Any, Optional

from pydantic import create_model, Field
from pydantic.config import ConfigDict


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

        default = None
        if column.nullable:
            python_type = Optional[python_type]

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
