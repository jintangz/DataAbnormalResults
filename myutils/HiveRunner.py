from typing import Optional, AnyStr, Dict, List
from pyhive import hive
from logging import Logger
from pandas import DataFrame

from myutils.DataRunner import DataRunner
from myutils.Column import Column

class HiveRunner(DataRunner):
    __slots__ = ['_host', '_port', '_username', '_password', '_auth', '_database', '_config', '_cursor', '_conn',
                 '_logger']

    def __init__(self, host: Optional[AnyStr], port: Optional[AnyStr], username: Optional[AnyStr],
                 password: Optional[AnyStr], auth: Optional[AnyStr], database: Optional[AnyStr],
                 config: Optional[Dict], logger: Optional[Logger]):
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._auth = auth
        self._database = database
        self._config = config
        self._logger = logger
        self._conn = hive.connect(host=host, port=port, username=username, password=password, auth=auth,
                                  database=database, config=config)
        self._cursor = self._conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cursor.close()
        self._conn.close()

    def query(self, sql: Optional[AnyStr]) -> DataFrame:
        self._cursor.execute(sql)
        self._logger.info(f"""execute sql: {sql}""")
        result:List = self._cursor.fetchall()
        columns:List = [_[0] for _ in self._cursor.description]
        self._logger.info(f"""result data shape: {len(result)},{len(columns)}\ncolumns: {','.join(columns)}""")
        return DataFrame(data=result, columns=columns)

    def create_tabel(self, table_name: Optional[AnyStr], columns: List[Column], *args, **kwargs):
        database:AnyStr = kwargs.get('detail_database', default=self._database)
        part_col:AnyStr = kwargs.get('partition_col', default=None)
        column_str:AnyStr = ','.join(list(map(lambda x: x.__str__(), columns)))
        part_col_str:AnyStr = f'partitioned by ({part_col.__str__()})' if part_col is not None else ''
        sql:AnyStr = f"""create table `{database}`.`{table_name}` (
                {column_str}){part_col_str} 
                row format delimited
                fields terminated by '\t'
                lines terminated by '\n'
                stored as orc"""
        self._logger.info(f"""{sql}""")
        self._cursor.execute(sql)

    def insert(self, data: DataFrame, columns: List[Column], table_name: Optional[AnyStr], *args, **kwargs):
        database: AnyStr = kwargs.get('detail_database', default=self._database)
        column_str: AnyStr = ','.join(list(map(lambda x: f'`{x.name}`', columns)))
        df: DataFrame = data.loc[:, list(map(lambda x:x.name, columns))]
        value_str: AnyStr = ','.join([f"""({','.join(list(map(str, row[1].values)))})""" for row in df.iterrows()])
        sql: AnyStr = f"""insert into table `{database}`.`{table_name}` ({column_str}) values {value_str}"""
        self._logger.info(f"""insert into table {database}.{table_name} from DataFrame""")
        self._cursor.execute(sql)

    def drop_table(self, table_name: AnyStr, *args, **kwargs):
        database: AnyStr = kwargs.get('detail_database', default=self._database)
        sql: AnyStr = f"""drop table if exists table `{database}`.`{table_name}`"""
        answer: AnyStr = input(f"Drop table {database}.{table_name}?(Y/N)")
        if answer.upper() == 'Y':
            self._cursor.execute(sql)
            self._logger.info(sql)
        else:
            self._logger.info(f"Failed to drop table {database}.{table_name}")