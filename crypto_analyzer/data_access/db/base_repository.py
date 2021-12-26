from crypto_analyzer.data_access.db.db_connector import DbConnection
from abc import ABC, abstractmethod


class BaseRepository(ABC):
    _instance = None

    def __init__(self):
        self.connection = DbConnection.get_instance()
        self.connection.execute_command(self._get_create_db_if_not_exists_str())

    @abstractmethod
    def _get_create_db_if_not_exists_str(self) -> str:
        pass

    @classmethod
    def get_instance(cls) -> 'BaseRepository':
        if cls._instance is None:
            # cls.__instance = type(cls.__name__, (cls,), {})
            # cls.__instance.self = cls.__instance
            cls._instance = BaseRepository()
        return cls._instance
