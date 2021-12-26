import pandas
import numpy as np

from crypto_analyzer.data_access.db.random_numbers_repository import RandomNumbersRepository


class FooTask2:
    name = 'foo-task2'

    def __init__(self, worker_id: int):
        self.worker_id = worker_id

    def get_df_with_random_values(self) -> pandas.DataFrame:
        rows = np.random.randint(200, size=1) # generate just one random number
        return pandas.DataFrame(data=rows, columns=['value'])

    def run(self):
        print(f'generating random set of numbers::worker_id>>{self.worker_id}')
        RandomNumbersRepository.get_instance().save_from_df(
            self.get_df_with_random_values()
        )


if __name__ == '__main__':
    FooTask2(2).run()