from crypto_analyzer.utils import file_utils
import pandas as pd
from crypto_analyzer.data_access.db.coin_price_repository import CoinPriceRepository


class CmcLoader:
    name = 'load-coin-market-cap'

    def __init__(self):
        self.input_file_url = file_utils.get_latest_file_url(file_utils.get_parsed_csv_folder_url())

    def run(self):
        df = pd.read_csv(self.input_file_url)
        CoinPriceRepository.get_instance().save_from_df(df)

    @staticmethod
    def run_task():
        CmcLoader().run()
