from apps.helpers import get_unique_value, save_file_to_disk
from config import Settings
from pandas import DataFrame
from io import StringIO
from asyncio import gather
from db import ClickHouse
from apps.services import get_column_types_from_file, get_column_names, get_create_table_query, \
    get_insert_query_dict


async def csv_to_db(file: bytes) -> dict:

    data_frame = DataFrame(StringIO(str(file, 'utf-8')))

    tasks = [
        get_column_names(data_frame_element=data_frame.values[0]),
        get_column_types_from_file(data_frame_element=data_frame.values[1]),
        get_unique_value()
    ]

    results = await gather(*tasks)

    file_id = results[2]
    column_names = results[0]
    column_types = results[1]

    create_table_query = await get_create_table_query(file_id=file_id, column_names=column_names,
                                                      column_types=column_types)
    tasks = [
        save_file_to_disk(file_name=f"{Settings.FILE_DIR}/{file_id}.csv", file=file),
        ClickHouse.execute_sql(query=create_table_query)
        ]

    await gather(*tasks)

    task_lists = []

    count_lists_to_insert = (len(data_frame.values) / 1000) - 1
    offset_from = 1

    if count_lists_to_insert >= 1:
        count_lists_checked = 0
        offset_to = offset_from + 999
        while count_lists_checked < count_lists_to_insert:
            task_list = []
            for value in data_frame.values[offset_from:offset_to]:
                task_list.append(get_insert_query_dict(values=value, column_names=column_names,
                                                       column_types=column_types))
            task_lists.append(task_list)
            count_lists_checked += 1
            offset_from = offset_to
            offset_to += 999

    task_list = []

    for value in data_frame.values[offset_from:-1]:
        task_list.append(get_insert_query_dict(values=value, column_names=column_names, column_types=column_types))
    task_lists.append(task_list)

    tasks = []

    for task_list in task_lists:
        results = await gather(*task_list)
        tasks.append(ClickHouse.insert_many(table=f"file_data_{file_id}", values=results))

    await gather(*tasks)

    return {"file_id": file_id, "ok": True}
