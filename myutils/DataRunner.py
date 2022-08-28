from abc import ABCMeta,abstractmethod
from typing import Optional, AnyStr, List
import pandas as pd

from myutils.Column import Column


class DataRunner(metaclass=ABCMeta):
    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @abstractmethod
    def query(self, sql):
        pass

    @abstractmethod
    def create_tabel(self, table_name: Optional[AnyStr], columns: List[Column], *args, **kwargs):
        pass

    @abstractmethod
    def insert(self, data: pd.DataFrame, columns: List[Column], table_name: Optional[AnyStr], *args, **kwargs):
        pass

    @abstractmethod
    def drop_table(self, table_name: AnyStr, *args, **kwargs):
        pass