from config import Settings


BASE_HTTP_PREFIX = Settings.BASE_HTTP_PREFIX


class Urls:
    import_csv = BASE_HTTP_PREFIX + "/import_csv"
    group_data = BASE_HTTP_PREFIX + "/group_data"
    drop_file = BASE_HTTP_PREFIX + "/drop_file"
