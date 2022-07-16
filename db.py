from asynch import connect, connection
from asynch.cursors import DictCursor
from config import Settings


class ClickHouse:

    @classmethod
    async def create_db(cls, db_name: str) -> bool:
        """Create clickhouse db on first run

        Args:
            db_name (str): name of db
        Returns:
            bool: result
        """
        conn = await connect(
            host=Settings.CLICK_HOUSE_HOST,
            port=Settings.CLICK_HOUSE_PORT,
        )
        async with conn.cursor(cursor=DictCursor) as cursor:
            result = await cursor.execute(query=f"create database if not exists {db_name};")
        await conn.close()

        if result:
            return True
        return False

    
    @classmethod
    async def conn(cls,) -> connection.Connection:
        """ click house connection

        Returns:
            connection.Connection: Connection
        """
        return await connect(
            host=Settings.CLICK_HOUSE_HOST,
            port=Settings.CLICK_HOUSE_PORT,
            database=Settings.CLICK_HOUSE_DB,
        )

    @classmethod
    async def execute_sql(cls, query) -> bool:
        """execute sql

        Args:
            query (_type_): sql query
        Returns:
            bool: result
        """
        conn = await cls.conn()
        async with conn.cursor(cursor=DictCursor) as cursor:
            result = await cursor.execute(query)
        await conn.close()

        if result:
            return True
        return False


    @classmethod
    async def fetchall(cls, query: str) -> list:
        """ get many records

        Args:
            query (str): sql query

        Returns:
            list: record dict
        """
        conn = await cls.conn()
        async with conn.cursor(cursor=DictCursor) as cursor:
            await cursor.execute(query)
            ret = cursor.fetchall()
        await conn.close()
        return ret

    @classmethod
    async def fetchone(cls, query: str) -> dict:
        """ get one record

        Args:
            query (str): sql query

        Returns:
            dict: record
        """
        conn = await cls.conn()
        async with conn.cursor(cursor=DictCursor) as cursor:
            await cursor.execute(query)
            ret = cursor.fetchone()
        await conn.close()
        return ret

    @classmethod
    async def insert_many(cls, table: str, values: list) -> bool:
        """ insert_many records

        Args:
            table (str): table name

            values (list): dicts

        Returns:
            bool: insert result
        """
        insert_fields = ','.join([i for i in values[0].keys()])
        conn = await cls.conn()
        async with conn.cursor(cursor=DictCursor) as cursor:
            result = await cursor.execute(
                f"""INSERT INTO {table} ({insert_fields}) VALUES """, values
            )
        await conn.close()
        if result:
            return True
        return False


    @classmethod
    async def check_connection_to_click_house(cls) -> bool:
        """ check click house is alive

        Returns:
            bool: flag_connected
        """
        try:
            conn = await cls.conn()
            flag_connected = conn.connected
            await conn.close()
            return flag_connected
        except Exception as e:
            print(e, flush=True)
            return False






