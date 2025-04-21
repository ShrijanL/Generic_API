from utils import convert_to_pydantic_model
from sqlalchemy import insert, update, inspect
from api_config import (
    SECRET_KEY,
    ACCESS_TOKEN_EXPIRE_MINUTES,
    REFRESH_TOKEN_EXPIRE_MINUTES,
    algorithm,
)
from datetime import datetime, timedelta
import jwt
from typing import Annotated
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")


def handle_save_input(model, rec_id, saveInput):

    insert_statements = []
    model_schema_pydantic_model = convert_to_pydantic_model(model)

    if rec_id and len(saveInput) > 1:
        raise Exception({"error": "Only 1 record to update at once", "code ": "GA-013"})

    id_fld = list(inspect(model).primary_key)[0]

    for save_item in saveInput:
        fld_values = {}

        try:
            model_schema_pydantic_model(**save_item)
        except Exception as e:
            error_msg = e.errors()[0].get("msg")
            error_loc = e.errors()[0].get("loc")
            raise Exception({"error": f"{error_msg}. {error_loc}", "code ": "GA-014"})

        for fld, value in list(save_item.items()):
            column = getattr(model.c, fld, None)

            if (
                column.default
                and not value
                and value is not False
                and not column.nullable
            ):
                save_item.pop(fld)

            fld_values[fld] = value

        if rec_id:
            insert_statements.append(
                update(model).where(id_fld == rec_id).values(**fld_values)
            )
        else:
            insert_statements.append(insert(model).values(**fld_values))

    return insert_statements


def insert_records(statements, session):

    all_res = []

    try:
        for stmt in statements:
            result = session.execute(stmt)
            if result.rowcount > 0:
                all_res.append(result.lastrowid)
        session.commit()
    except Exception as e:
        session.rollback()
        raise Exception({"error": e.args[0], "code": "GA-015"})
    finally:
        session.close()

    return all_res


def create_tokens(user_id: int, refresh: bool = False):

    issued_at = datetime.utcnow()
    # access_expires_delta = issued_at + timedelta(seconds=5)
    # refresh_expires_delta = issued_at + timedelta(seconds=5)

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

    if not refresh:
        return access_token, refresh_token
    else:
        return access_token


def refresh_get_access_token(refresh_token: str):

    try:
        decode_refresh = jwt.decode(refresh_token, SECRET_KEY, algorithms=algorithm)

        user_id = decode_refresh.get("sub")

        new_access_token = create_tokens(user_id=user_id, refresh=True)

        return new_access_token

    except jwt.ExpiredSignatureError:
        raise HTTPException(
            detail="Refresh token has expired, Please login again.",
            headers={"WWW-Authenticate": "Bearer"},
            status_code=status.HTTP_400_BAD_REQUEST,
        )


def decode_token(token: Annotated[str, Depends(oauth2_scheme)]) -> str:

    try:
        decoded_token = jwt.decode(token, SECRET_KEY, algorithms=algorithm)

        return decoded_token.get("sub")

    except jwt.ExpiredSignatureError:
        raise HTTPException(
            detail="Token has expired.",
            headers={"WWW-Authenticate": "Bearer"},
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"},
        )
