from uvicorn import Config, Server
from config import Settings


if __name__ == "__main__":
    try:
        server = Server(
            Config("init_application:app",
                   host=Settings.HTTP_SERVER_HOST,
                   port=Settings.HTTP_SERVER_PORT,
                   workers=Settings.COUNT_WORKERS_UVICORN,
                   debug=False)
        )
        server.run()
    except Exception as ex:
        print(ex, flush=True)
        exit(1)



