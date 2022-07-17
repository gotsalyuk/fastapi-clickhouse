from fastapi import FastAPI
from apps.helpers import check_file_exists
from routers import api_router
from db import ClickHouse
from config import Settings
import os


app = FastAPI()
app.include_router(router=api_router)


@app.on_event("startup")
async def on_start_app():
    """run before app started
    """
    await ClickHouse.create_db(db_name=Settings.CLICK_HOUSE_DB)

    file_dir_exists = await check_file_exists(Settings.FILE_DIR)
    if not file_dir_exists:
        os.mkdir(Settings.FILE_DIR)

    Settings.FILE_DIR = os.path.abspath(Settings.FILE_DIR)
