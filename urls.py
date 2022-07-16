from config import Settings


BASE_HTTP_PREFIX = Settings.BASE_HTTP_PREFIX


class Urls:
    health_check = BASE_HTTP_PREFIX + "/healthcheck"