import pandas as pd
from typing import Optional, AnyStr, Callable, Tuple, Union, Dict

from dao.UserPublishDao import UserPublishDao


class AggregateDataService:

    def __init__(self, dao: UserPublishDao):
        self._dao: UserPublishDao = dao

    def aggregate(self, agg_func: Union[Tuple[Callable], Dict[AnyStr, Callable]],
                  agg_dim_cols: Tuple[Tuple[AnyStr]] = None,
                  *args, **kwargs):
        data: pd.DataFrame = self._dao.get_data()
        if agg_dim_cols is None:
            agg_dim_cols: Tuple[Tuple[AnyStr]] = ((self._dao.get_time_col(), dim_col) for dim_col in
                                                  self._dao.get_dim_cols() if
                                                  dim_col != self._dao.get_time_col())
        data.groupby(by=agg_dim_cols, as_index=False).agg(func=agg_func)
