from apps.helpers import get_unique_value
from db import ClickHouse
from asyncio import gather
from apps.services import check_table_exist, get_search_sql_query, check_columns_exist, get_grouped_content, \
    get_grouped_db_id, create_or_update_group_ids, get_create_group_table_query


async def create_group_table_and_insert_values(parent_file_id: str, group_db_id: str, group_fields: str):
    query = await get_create_group_table_query(parent_file_id=parent_file_id, file_id=group_db_id,
                                               group_fields=group_fields)
    await ClickHouse.execute_sql(query=query)
    search_query = await get_search_sql_query(file_id=parent_file_id, group_fields=group_fields.split(","))
    results = await ClickHouse.fetchall(query=search_query)

    count_lists_to_insert = (len(results) / 1000) - 1
    offset_from = 1

    tasks_list = []

    if count_lists_to_insert >= 1:
        count_lists_checked = 0
        offset_to = offset_from + 999
        while count_lists_checked < count_lists_to_insert:
            query_list = []
            for value in results[offset_from:offset_to]:
                query_list.append(value)
            tasks_list.append(ClickHouse.insert_many(table=f"file_data_{group_db_id}", values=query_list))
            count_lists_checked += 1
            offset_from = offset_to
            offset_to += 999

    query_list = []

    for value in results[offset_from:-1]:
        query_list.append(value)

    tasks_list.append(ClickHouse.insert_many(table=f"file_data_{group_db_id}", values=query_list))

    await gather(*tasks_list)


async def prepare_grouped_table(file_id: str, group_fields: str) -> None:
    parent_file_id = file_id
    file_id = await get_unique_value()

    group_db_id = await get_grouped_db_id(file_id=parent_file_id, group_fields=group_fields)

    if group_db_id:
        db_exists = await check_table_exist(file_id=group_db_id)
        if db_exists:
            return

        await create_group_table_and_insert_values(parent_file_id=parent_file_id,
                                                   group_db_id=group_db_id, group_fields=group_fields)
        return

    else:
        await create_or_update_group_ids(parent_file_id=parent_file_id, file_id=file_id, group_fields=group_fields)
        await create_group_table_and_insert_values(parent_file_id=parent_file_id,
                                                   group_db_id=file_id, group_fields=group_fields)
        return


async def get_grouped_file(file_id: str, group_fields: str) -> dict:
    table_exists = await check_table_exist(file_id=file_id)

    if not table_exists:
        return {"ok": False, "message": f"table file_data_{file_id} not exists"}

    if not group_fields:
        query = await get_search_sql_query(file_id=file_id)
        results = await ClickHouse.fetchall(query=query)
        return {"ok": True, "message": results}

    group_fields = group_fields.strip().replace(" ", "").lower()

    column_exists = await check_columns_exist(file_id=file_id, group_fields=group_fields.split(','))

    if not column_exists is True:
        return {"ok": False, "message": f"field '{column_exists}' not exists in table file_data_{file_id}"}

    result = await get_grouped_content(file_id=file_id, group_fields=group_fields)

    if result is not None:
        return {"ok": True, "message": result}

    await prepare_grouped_table(file_id, group_fields)

    query = await get_search_sql_query(file_id=file_id, group_fields=group_fields.split(","))

    results = await ClickHouse.fetchall(query=query)

    return {"ok": True, "message": results}

