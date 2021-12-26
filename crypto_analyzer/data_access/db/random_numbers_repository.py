import pandas
import pandas as pd

from crypto_analyzer.data_access.db.base_repository import BaseRepository


class RandomNumbersRepository(BaseRepository):
    _collection_name = 'random_numbers'

    def _get_create_db_if_not_exists_str(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._collection_name}(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value INTEGER NOT NULL
        );
        """

    def find_all_as_df(self) -> pandas.DataFrame:
        return pd.read_sql(f"SELECT * FROM {self._collection_name}", con=self.connection.engine)

    @classmethod
    def get_instance(cls) -> 'RandomNumbersRepository':
        if cls._instance is None:
            cls._instance = RandomNumbersRepository()
        return cls._instance
