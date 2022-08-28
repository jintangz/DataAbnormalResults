import pandas as pd
from typing import Optional, AnyStr, Callable, Tuple, Union, Dict, List

from dao.UserPublishDao import UserPublishDao


class AggregateDataService:

    def __init__(self, dao: UserPublishDao):
        self._dao: UserPublishDao = dao

    def aggregate(self, agg_func: Union[Tuple[Callable, ...], Dict[AnyStr, Callable]],
                  agg_dim_cols: Tuple[Tuple[AnyStr, ...], ...] = None) -> pd.DataFrame:
        data: pd.DataFrame = self._dao.get_data()
        if agg_dim_cols is None:
            # 默认 [(pt_date), (pt_date, region),(pt_date, ...), ...]
            agg_dim_cols: Tuple[Tuple[AnyStr, ...], ...] = tuple([(self._dao.get_time_col(), dim_col) for dim_col in
                                                                  self._dao.get_dim_cols() if
                                                                  dim_col != self._dao.get_time_col()] + [
                                                                     (self._dao.get_time_col())])
        df: pd.DataFrame = pd.concat(
            [data.groupby(by=dim_col, as_index=False).agg(func=agg_func).reset_index() for dim_col in agg_dim_cols])
        df.columns = list(
            map(lambda x: '_'.join(x) if x[0] not in self._dao.get_dim_cols() or x[0] == self._dao.get_time_col() else
            x[0], df.columns))
        cols: List[AnyStr] = list(filter(lambda x: x in df.columns, self._dao.get_dim_cols()))
        df['agg_dim_name'] = self._dao.get_time_col()
        df['agg_dim_value'] = '全部'
        for col in cols:
            tmp = df[col].apply(lambda x: '' if x is None else '+' + str(x))
            df['agg_dim_name'] = df['agg_dim_name'].str.cat(tmp)
        df[cols] = df[cols].fillna('全部')
        for col in cols:
            df['agg_dim_value'] = df['agg_dim_value'].astype(str) + '_' + df[col].astype(str)
        df.groupby(by='pt_date').transform()
        return df


if __name__ == '__main__':
    a: Tuple[int, ...] = (1, 2, 3, 4)
