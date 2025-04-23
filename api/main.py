from typing import Union, Dict, Any

from fastapi import Depends
from fastapi import FastAPI

from .api_deps import generic_fetch, generic_save, generic_login
from .services import decode_token, refresh_get_access_token

app = FastAPI()


@app.get("/index/hi")
async def read_root():
    return {"Hello": "World"}


@app.get("/index/{name}")
def read_root_1(name: str):
    return {"name": name}


@app.get("/items/{item_id}")
async def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}


@app.post("/fetch")
async def fetch(payload: Dict[str, Any], user_id: str or dict = Depends(decode_token)):
    message = generic_fetch(payload)

    return message


@app.post("/save")
async def save(payload: Dict[str, Any]):
    message = generic_save(payload)

    return message


@app.post("/login")
async def login(payload: Dict[str, Any]):
    access_token, refresh_token = generic_login(payload)

    return {"access_token": access_token, "refresh_token": refresh_token}


@app.post("/refresh")
def refresh(payload: Dict[str, Any]):
    refresh_token = payload.get("payload").get("refresh_token")
    new_access_token = refresh_get_access_token(refresh_token)

    return {"new_access_token": new_access_token}
