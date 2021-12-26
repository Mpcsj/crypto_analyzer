from abc import ABC, abstractmethod

import pandas
import sqlalchemy.exc
from airflow.utils import sqlalchemy

from crypto_analyzer.data_access.db.db_connector import DbConnection


class BaseRepository(ABC):
    _instance = None
    _collection_name = None

    def __init__(self):
        self.connection = DbConnection.get_instance()
        self.connection.execute_command(self._get_create_db_if_not_exists_str())

    def save_from_df(self, data: pandas.DataFrame):
        try:
            data.to_sql(self._collection_name, con=self.connection.engine, if_exists='append', index=False)
        except sqlalchemy.exc.IntegrityError as err:
            print('Integrity error >>', err)
        except Exception as err:
            print(err)

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
