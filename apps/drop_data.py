from apps.services import check_table_exist, get_all_grouped_db_id_and_delete_redis_key
from apps.helpers import check_file_exists, remove_file
from config import Settings
from asyncio import gather
from db import ClickHouse


async def drop_table(file_id: str) -> bool:
    query = f"DROP TABLE file_data_{file_id};"
    return await ClickHouse.execute_sql(query=query)


async def drop_data(file_id: str) -> dict:
    tasks = [
        check_file_exists(file_path=f"{Settings.FILE_DIR}/{file_id}.csv"),
        check_table_exist(file_id=file_id),
        get_all_grouped_db_id_and_delete_redis_key(file_id=file_id)
    ]

    results = await gather(*tasks)

    file_exists, table_exists, ids_to_delete = results[0], results[1], results[2]

    tasks = []
    if file_exists:
        tasks.append(remove_file(file_path=f"{Settings.FILE_DIR}/{file_id}.csv"))

    if table_exists:
        tasks.append(drop_table(file_id=file_id))
    for id_to_delete in ids_to_delete:
        tasks.append(drop_table(file_id=id_to_delete))

    await gather(*tasks)

    return {"ok": True, "message": file_id}
