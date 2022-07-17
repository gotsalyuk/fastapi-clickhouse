from fastapi import APIRouter, HTTPException, File
from apps.csv_to_db import csv_to_db
from apps.drop_data import drop_data
from apps.group_csv_from_db import get_grouped_file
from urls import Urls


api_router = APIRouter()


@api_router.post(Urls.import_csv)
async def import_csv(file: bytes = File()):
    """

        upload csv

        Args:

            file (bytes):

        Returns:

            dict: result
        """
    result = await csv_to_db(file=file)
    if result.get("ok") is True:
        return result
    raise HTTPException(status_code=503, detail=result)


@api_router.get(Urls.group_data)
async def group_data(file_id: str, group_fields: str = None):
    """

        sorted file

        Args:

            file_id (str): file_id
            group_fields (str): field1,field2,field3

        Returns:

            list: result dicts
        """
    result = await get_grouped_file(file_id=file_id, group_fields=group_fields)
    if result.get("ok") is True:
        return result

    raise HTTPException(status_code=503, detail=result)


@api_router.get(Urls.drop_file)
async def drop_file(file_id: str):
    """

        delete file and drop all reference tables

        Args:

            file_id (str): file_id

        Returns:

            dict: result
    """
    result = await drop_data(file_id=file_id)
    if result.get("ok") is True:
        return result
    raise HTTPException(status_code=503, detail=result)





