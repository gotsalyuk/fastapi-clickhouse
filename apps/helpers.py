from datetime import datetime
from uuid import uuid1
import os
import aiofiles


async def check_file_exists(file_path: str) -> bool:
    return os.path.exists(file_path)


async def remove_file(file_path: str) -> bool:
    return os.remove(path=file_path)


async def string_is_date(check_date: str) -> bool:
    formats = ["%Y-%m-%d",
               "%Y-%m-%d %H:%M:%S",
               "%Y-%m-%d %H:%M:%S.%f",
               ]
    for form in formats:
        try:
            datetime.strptime(check_date, form)
            return True
        except:
            pass

    return


async def date_string_to_datetime(date: str) -> datetime or None:
    formats = ["%Y-%m-%d",
               "%Y-%m-%d %H:%M:%S",
               "%Y-%m-%d %H:%M:%S.%f",
               ]
    for form in formats:
        try:
            return datetime.strptime(date, form)
        except:
            pass

    return None


async def get_unique_value():
    return str(uuid1().hex)


async def save_file_to_disk(file_name: str, file: bytes) -> None:
    async with aiofiles.open(file_name, mode='wb') as f:
        await f.write(file)