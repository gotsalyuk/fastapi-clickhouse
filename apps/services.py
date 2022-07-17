import ujson
from apps.helpers import string_is_date, date_string_to_datetime
from db import ClickHouse
from redis import Redis


async def check_table_exist(file_id: str, name_prefix: str = "file_data_") -> bool:
    table = await ClickHouse.fetchone(f"SHOW TABLES LIKE '{name_prefix}{file_id}';")
    if table and table.get("name"):
        return True
    return False


async def get_columns_types(file_id: str, column_names: list) -> bool or str:
    create_table = await ClickHouse.fetchone(f"SHOW CREATE TABLE file_data_{file_id};")
    result = []
    create_table = create_table["statement"].split("ENGINE")[0]
    create_table = create_table.split("\n")
    for row in create_table:
        for column_name in column_names:
            if column_name in row:
                result.append(row.strip().replace('`', "").replace(',', ""))
    return result


async def get_column_types_from_file(data_frame_element: list) -> list:
    column_types = []
    values = data_frame_element[0].replace('"', '').split(',')
    for value in values:
        value = value.strip()
        if value.isnumeric():
            column_types.append("Int64")
        elif await string_is_date(check_date=value):
            column_types.append("DateTime")
        else:
            column_types.append("String")

    return column_types


async def check_columns_exist(file_id: str, group_fields: list) -> bool or str:
    if len(group_fields) > 0:
        create_table = await ClickHouse.fetchone(f"SHOW CREATE TABLE file_data_{file_id};")
        for group_field in group_fields:
            if not group_field in create_table["statement"]:
                return group_field
    return True


async def get_column_names(data_frame_element: list) -> list:
    column_names = data_frame_element[0].strip().replace('"', '').split(',')
    return [ column.replace(" ", "").lower() for column in column_names ]


async def get_search_sql_query(file_id: str, group_fields: list = []) -> str:
    if len(group_fields) > 0:
        group_fields = ["t." + field for field in group_fields]
        group_fields = ', '.join(group_fields)

        return f"SELECT {group_fields} FROM file_data_{file_id} t" \
               f" GROUP BY {group_fields} ORDER BY {group_fields} DESC;"

    return f"SELECT * FROM file_data_{file_id};"


async def get_create_table_query(file_id: str, column_names: list, column_types: list) -> str:
    create_table_query = f"CREATE TABLE if not exists file_data_{file_id} ("
    for i in range(len(column_types)):
        if i > 0:
            create_table_query += f"{column_names[i]} Nullable({column_types[i]}),"
        else:
            create_table_query += f"{column_names[i]} {column_types[i]},"

    return f"{create_table_query[:-1]}) ENGINE = MergeTree ORDER BY {column_names[0]};"


async def get_insert_query_dict(values: list, column_names: list, column_types: list) -> dict:
    query_dict = {}
    values = values[0].replace('"', "").split(",")

    for i in range(len(values)):
        if column_types[i] == "Int64":
            value = int(values[i])
        elif column_types[i] == "DateTime":
            value = await date_string_to_datetime(date=values[i])
        else:
            value = values[i]
        query_dict[column_names[i]] = value

    return query_dict


async def get_create_group_table_query(parent_file_id: str, file_id: str, group_fields: str) -> str:
    create_table_query = f"CREATE TABLE if not exists file_data_{file_id} ("
    columns = await get_columns_types(file_id=parent_file_id, column_names=group_fields.split(','))
    create_table_query += ", ".join(columns) + ") ENGINE = MergeTree"
    create_table_query = create_table_query + " ORDER BY " + columns[0].split(" ")[0] + ";" \
        if len(columns) > 1 else create_table_query + ";"
    return create_table_query


async def get_grouped_db_id(file_id: str, group_fields: str) -> str or None:
    group_db_ids = await Redis.get(key=file_id)

    if group_db_ids:
        group_db_ids = ujson.loads(group_db_ids)
        for fields, db_id in group_db_ids.items():
            if fields == group_fields:
                return db_id
    return None


async def get_all_grouped_db_id_and_delete_redis_key(file_id: str) -> list or None:
    group_db_ids = await Redis.get(key=file_id)
    result = []
    if group_db_ids:
        group_db_ids = ujson.loads(group_db_ids)
        for fields, db_id in group_db_ids.items():
            result.append(db_id)
        await Redis.delete(key=file_id)
    return result


async def create_or_update_group_ids(parent_file_id: str, file_id: str, group_fields: str) -> bool:
    group_db_ids = await Redis.get(key=parent_file_id)

    if group_db_ids:
        group_db_ids = ujson.loads(group_db_ids)
        if not group_db_ids.get(group_fields):
            group_db_ids[group_fields] = file_id
    else:
        group_db_ids = {group_fields: file_id}

    await Redis.set(key=parent_file_id, value=ujson.dumps(group_db_ids))


async def get_grouped_content(file_id: str, group_fields: str) -> list or None:
    group_db_id = await get_grouped_db_id(file_id=file_id, group_fields=group_fields)
    if group_db_id:
        table_exists = await check_table_exist(file_id=group_db_id)
        if table_exists:
            return await ClickHouse.fetchall(query=f"SELECT * FROM file_data_{group_db_id};")
    return None
