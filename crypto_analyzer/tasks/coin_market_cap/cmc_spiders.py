from crypto_analyzer.tasks.coin_market_cap.cmc_controllers import CmcController
from crypto_analyzer.utils import file_utils
from crypto_analyzer.utils import date_utils


class CmcSpider:
    name = 'crawl-coin-market-cap'

    def __init__(self):
        self.controller = CmcController()

    @staticmethod
    def get_output_file_url():
        return file_utils.append_url(
            file_utils.get_extracted_html_folder_url(),
            f'{date_utils.get_curr_utc_time_to_filename()}.html'
        )

    def run(self):
        print('run spider')
        res = self.controller.extract_main_page()
        file_utils.write_str_file(self.get_output_file_url(), res.text)

    @staticmethod
    def run_task():
        CmcSpider().run()
