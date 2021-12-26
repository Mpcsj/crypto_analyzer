from sqlalchemy import create_engine
import os

DATABASE_LOCATION = os.environ.get('DB_PATH')


class DbConnection:
    __instance = None

    def __init__(self):
        self.engine = create_engine(DATABASE_LOCATION)

    def execute_command(self, command: str):
        return self.engine.execute(command)

    @classmethod
    def get_instance(cls) -> 'DbConnection':
        if cls.__instance is None:
            cls.__instance = DbConnection()
        return cls.__instance
