from datetime import datetime, timedelta
from typing import Annotated

import jwt
from fastapi import Depends
from sqlalchemy import insert, update, inspect

from .api_config import (
    SECRET_KEY,
    ACCESS_TOKEN_EXPIRE_MINUTES,
    algorithm,
)
from .utils import (
    convert_to_pydantic_model,
    raise_exception,
    CustomOAuth2PasswordBearer,
)

oauth2_scheme = CustomOAuth2PasswordBearer(tokenUrl="/login")


def handle_save_input(model, rec_id, saveInput):
    insert_statements = []
    model_schema_pydantic_model = convert_to_pydantic_model(model)

    if rec_id and len(saveInput) > 1:
        raise_exception(error="Only 1 record to update at once", code="GA-020")

    id_fld = list(inspect(model).primary_key)[0]

    for save_item in saveInput:
        fld_values = {}

        try:
            valid_rec = model_schema_pydantic_model(**save_item)
        except Exception as e:
            error_msg = e.errors()[0].get("msg")
            error_loc = e.errors()[0].get("loc")
            raise_exception(error=f"{error_msg}. {error_loc}", code="GA-021")

        for fld, value in valid_rec:
            column = getattr(model.c, fld, None)

            # only append field and value to insert statement if following condition
            if not (
                (column.default or column.server_default)
                and not value
                and value is not False
                and not column.nullable
            ):
                fld_values[fld] = value

        if rec_id:
            insert_statements.append(
                update(model).where(id_fld == rec_id).values(**fld_values)
            )
        else:
            insert_statements.append(insert(model).values(**fld_values))

    return insert_statements


def insert_records(statements, session):
    """
    Functionality for save / update records.
    :param statements:
    :param session:
    :return:
    """
    all_res = []

    try:
        for stmt in statements:
            result = session.execute(stmt)
            if result.rowcount > 0:
                all_res.append(result.lastrowid)
        session.commit()
    except Exception as e:
        session.rollback()
        raise_exception(error=e.args[0], code="GA-022")
    finally:
        session.close()

    return all_res


def delete_records(statement, session):
    """
    Functionality for deleting records.
    :param statement:
    :param session:
    :return:
    """
    try:
        session.execute(statement)
        session.commit()
    except Exception as e:
        session.rollback()
        raise_exception(error=str(e), code="GA-023")
    finally:
        session.close()


def create_tokens(user_id: int, require_refresh: bool = True):
    issued_at = datetime.utcnow()

    access_expires_delta = issued_at + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    refresh_expires_delta = issued_at + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    access_encode = {
        "exp": access_expires_delta,
        "sub": str(user_id),
        "iat": issued_at,
        "type": "access",
    }
    access_token = jwt.encode(access_encode, SECRET_KEY, algorithm=algorithm)

    refresh_encode = {
        "exp": refresh_expires_delta,
        "sub": str(user_id),
        "iat": issued_at,
        "type": "refresh",
    }
    refresh_token = jwt.encode(refresh_encode, SECRET_KEY, algorithm=algorithm)

    # send only access token when refresh is not required.
    if not require_refresh:
        return access_token

    return access_token, refresh_token


def refresh_get_access_token(refresh_token: str):
    try:
        decode_refresh = jwt.decode(refresh_token, SECRET_KEY, algorithms=algorithm)

        user_id = decode_refresh.get("sub")

        new_access_token = create_tokens(user_id=user_id, require_refresh=False)

        return new_access_token

    except jwt.ExpiredSignatureError:
        raise_exception(
            error="Refresh token has expired, Please login again.", code="GA-024"
        )


def decode_token(token: Annotated[str, Depends(oauth2_scheme)]) -> str:

    decoded_token = {}

    try:
        decoded_token = jwt.decode(token, SECRET_KEY, algorithms=algorithm)

    except jwt.ExpiredSignatureError:
        raise_exception(error="Token has expired.", code="GA-025")
    except Exception as e:
        raise_exception(error=str(e), code="GA-026")

    return decoded_token.get("sub")
