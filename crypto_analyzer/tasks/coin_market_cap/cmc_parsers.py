from typing import Optional

from crypto_analyzer.utils import file_utils
from crypto_analyzer.utils import date_utils
from crypto_analyzer.tasks.coin_market_cap import get_current_folder_url
from selectorlib import Extractor
import pandas as pd


class CmcParser:
    name = 'transform-coin-market-cap-data'

    def __init__(self, input_html_file_url: str = None, output_file_url: str = None):
        self.extractor = Extractor.from_yaml_file(self.get_yml_selector_file_url())
        self.input_html_file_url = input_html_file_url
        if input_html_file_url is None:
            self.input_html_file_url = self.get_latest_html_file_url()
        self.output_file_url = output_file_url
        if output_file_url is None:
            self.output_file_url = self.get_default_output_file_url()

    @staticmethod
    def get_default_output_file_url():
        return file_utils.append_url(
            file_utils.get_parsed_csv_folder_url(),
            f'{date_utils.get_curr_utc_time_to_filename()}.csv'
        )

    @staticmethod
    def get_latest_html_file_url():
        return file_utils.get_latest_file_url(file_utils.get_extracted_html_folder_url())

    @staticmethod
    def get_yml_selector_file_url() -> str:
        return file_utils.append_url(
            get_current_folder_url(),
            'yml_selectors',
            'main_page.yml'
        )

    @staticmethod
    def get_html(input_html_file_url: str):
        return file_utils.read_str_file(input_html_file_url)

    def run(self):
        res = self.parse(self.input_html_file_url)
        res.to_csv(self.output_file_url, index=False)

    def parse(self, input_html_file_url):
        html = self.get_html(input_html_file_url)
        extracted = self.extractor.extract(html)
        print(extracted)
        return self.clean_to_df(extracted)

    @classmethod
    def clean_to_df(cls, extracted: dict) -> pd.DataFrame:
        rows = []
        updated_at = date_utils.get_curr_utc_date()
        for row in extracted['coin_table']:
            curr_name = row['coin_name']
            curr_price = row['curr_price_in_usd']
            if curr_name:
                curr_name = curr_name.strip()
            curr_price = cls.clean_price(curr_price)
            rows.append([curr_name, curr_price, updated_at])
        res = pd.DataFrame(data=rows, columns=['coin_name', 'curr_price_in_usd', 'updated_at'])
        res = res.dropna()  # small validation
        return res

    @staticmethod
    def clean_price(price_value: Optional[str]):
        if price_value:
            res = price_value.strip().replace('$', '').replace(',', '')
            return float(res)
        return None

    @staticmethod
    def run_task():
        CmcParser().run()
