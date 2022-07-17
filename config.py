import os
import dotenv


dotenv.load_dotenv(
    os.path.join(os.path.dirname(__file__), '.env')
)


class Settings:
    # ClickHouse
    CLICK_HOUSE_HOST = os.environ.get("CLICK_HOUSE_HOST")
    CLICK_HOUSE_PORT = int(os.environ.get("CLICK_HOUSE_PORT"))
    CLICK_HOUSE_DB = os.environ.get("CLICK_HOUSE_DB")

    # Server
    HTTP_SERVER_HOST = os.environ.get("HTTP_SERVER_HOST")
    HTTP_SERVER_PORT = int(os.environ.get("HTTP_SERVER_PORT"))
    COUNT_WORKERS_UVICORN = int(os.environ.get("COUNT_WORKERS_UVICORN", 1))
    BASE_HTTP_PREFIX = os.environ.get("BASE_HTTP_PREFIX")
    FILE_DIR = "files/"

    # redis
    REDIS_HOST = os.environ.get('REDIS_HOST')
    REDIS_PORT = int(os.environ.get('REDIS_PORT'))
    REDIS_DB = int(os.environ.get('REDIS_DB'))
    REDIS_TTL = int(os.environ.get('REDIS_TTL'))