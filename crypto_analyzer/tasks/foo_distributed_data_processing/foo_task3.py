from crypto_analyzer.data_access.db.random_numbers_repository import RandomNumbersRepository
from time import sleep

class FooTask3:
    name = 'foo-task3'

    def run(self):
        print('sleep for some time ...')
        sleep(30)
        repository = RandomNumbersRepository.get_instance()
        data = repository.find_all_as_df()
        print('sum values >> ', data['value'].sum())
        print('cleaning table...')
        repository.clear_table()
        print('done')


if __name__ == '__main__':
    FooTask3().run()
