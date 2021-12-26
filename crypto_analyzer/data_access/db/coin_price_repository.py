import pandas
import sqlalchemy.exc

from crypto_analyzer.data_access.db.base_repository import BaseRepository


class CoinPriceRepository(BaseRepository):
    def _get_create_db_if_not_exists_str(self) -> str:
        return """
        CREATE TABLE IF NOT EXISTS coin_price(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            coin_name VARCHAR(200),
            curr_price_in_usd REAL, 
            updated_at VARCHAR(20),
            UNIQUE(coin_name,updated_at)
        );
        """

    def save_from_df(self, data: pandas.DataFrame):
        try:
            data.to_sql('coin_price', con=self.connection.engine, if_exists='append', index=False)
        except sqlalchemy.exc.IntegrityError as err:
            print('Integrity error >>', err)
        except Exception as err:
            print(err)

    @classmethod
    def get_instance(cls) -> 'CoinPriceRepository':
        if cls._instance is None:
            cls._instance = CoinPriceRepository()
        return cls._instance
