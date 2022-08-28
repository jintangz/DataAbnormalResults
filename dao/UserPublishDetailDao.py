import pprint
from typing import AnyStr, Dict
import yaml
from pandas import DataFrame

from dao.UserPublishDao import UserPublishDao
from myutils.HiveRunner import HiveRunner


class UserPublishDetailDao(UserPublishDao):
    __slots__ = ['_database', '_table_name', '_data', '_time_col', '_dim_cols', '_measure_cols', '_start_date',
                 '_end_date', '_hiveRunner']

    def __init__(self, hiveRunner: HiveRunner, yaml_file_path: AnyStr = './UserPublishDao.yaml'):
        super(UserPublishDetailDao, self).__init__(hiveRunner, yaml_file_path)
        self._database: AnyStr = self._config.get('detail_database')
        self._table_name: AnyStr = self._config.get('detail_table_name')
        self.query()

    def query(self):
        sql = f"""select * from {self._database}.{self._table_name} 
                  where {self._time_col} between {self._start_date} and {self._end_date}"""
        self._data: DataFrame = self._hiveRunner.query(sql)
        return self._data

    def get_data(self):
        return self._data

    def get_time_col(self):
        return self._time_col

    def get_dim_cols(self):
        return self._dim_cols

    def get_measure_cols(self):
        return self._measure_cols


if __name__ == '__main__':
    config = yaml.safe_load(open(r'D:\CodeProject\pythonProject\dao\UserPublishDao.yaml', encoding='utf8'))
    pprint.pprint(config)
