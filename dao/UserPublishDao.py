from abc import ABCMeta, abstractmethod
import pprint
from typing import AnyStr, Dict, List
import yaml
from myutils.HiveRunner import HiveRunner


class UserPublishDao(metaclass=ABCMeta):
    __slots__ = ['_database', '_table_name', '_data', '_time_col', '_dim_cols', '_measure_cols', '_start_date',
                 '_end_date', '_hiveRunner', '_config']

    def __init__(self, hiveRunner: HiveRunner, yaml_file_path: AnyStr = './UserPublishDao.yaml'):
        self._hiveRunner = hiveRunner
        self._config: Dict = yaml.safe_load(open(yaml_file_path, encoding='utf8'))
        self._time_col: AnyStr = config.get('time_col')
        self._dim_cols: List[AnyStr] = config.get('dim_cols')
        self._measure_cols: List[AnyStr] = config.get('measure_cols')
        self._start_date: AnyStr = config.get('start_date')
        self._end_date: AnyStr = config.get('end_date')

    @abstractmethod
    def query(self):
        pass

    @abstractmethod
    def get_data(self):
        pass

    @abstractmethod
    def get_time_col(self):
        pass

    @abstractmethod
    def get_dim_cols(self):
        pass

    @abstractmethod
    def get_measure_cols(self):
        pass


if __name__ == '__main__':
    config = yaml.safe_load(open(r'D:\CodeProject\pythonProject\dao\UserPublishDao.yaml', encoding='utf8'))
    pprint.pprint(config)
    class Parent:
        def __init__(self):
            self._sql = """select * from `{}`.`{}` 
                  where `{}` between '{}' and '{}'"""

    class Son(Parent):
        def __init__(self):
            super(Son, self).__init__()
            self._database = 'bi_inter'
            self._table_name = 'test'
            self._time_col = 'pt_date'
            self._start_date = '2022-08-17'
            self._end_date = '2022-09-17'
            print(self._sql.format(self._database, self._table_name,self._time_col,self._start_date,self._end_date))
    Son()