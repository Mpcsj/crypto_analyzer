import requests


class CmcController:
    def __init__(self):
        self.base_url = 'https://coinmarketcap.com/'

    def extract_main_page(self) -> requests.Response:
        res = requests.get(self.base_url)
        return res
