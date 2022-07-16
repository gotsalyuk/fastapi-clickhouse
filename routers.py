from fastapi import APIRouter, HTTPException, Response
from urls import Urls
from db import ClickHouse


api_router = APIRouter()


@api_router.get(Urls.health_check)
async def health_check():
    """ check health 
    """
    click_house_connected = await ClickHouse.check_connection_to_click_house()

    if click_house_connected:
        return Response(status_code=200)
    
    else:
        details = "click house not connected"

    raise HTTPException(status_code=503, detail=details)