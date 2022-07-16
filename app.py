from routers import api_router
from fastapi import FastAPI
from db import ClickHouse
from config import Settings


app = FastAPI()
app.include_router(router=api_router)


@app.on_event("startup")
async def on_start_app():
    """run before app started
    """
    await ClickHouse.create_db(db_name=Settings.CLICK_HOUSE_DB)