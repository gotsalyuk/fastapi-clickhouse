from config import Settings
from aredis import StrictRedis


class Redis:

    @classmethod
    async def conn(cls) -> StrictRedis:
        return StrictRedis(host=Settings.REDIS_HOST,
                           port=Settings.REDIS_PORT,
                           db=Settings.REDIS_DB)

    @classmethod
    async def set(cls, key: str, value: str) -> None:
        """
        :param value:
        :param key:
        """
        redis = await cls.conn()
        await redis.set(key, value, Settings.REDIS_TTL)

    @classmethod
    async def get(cls, key: str) -> str or None:
        """
        :param key:
        """
        redis = await cls.conn()
        value = await redis.get(key)
        if value:
            return value.decode("utf-8")
        return None

    @classmethod
    async def delete(cls, key: str) -> None:
        """
        :param key:
        """
        redis = await cls.conn()
        await redis.delete(key)


