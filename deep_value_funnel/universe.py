from __future__ import annotations
import logging
from datetime import date, timedelta

import akshare as ak
import pandas as pd  # 必须确保 pd 已定义

from deep_value_funnel import config
from deep_value_funnel.http_utils import call_with_retry, df_nonempty
from deep_value_funnel.symbols import is_star_or_main_board_a, is_st_name

logger = logging.getLogger(__name__)

def _load_spot_universe() -> pd.DataFrame:
    """改为拉重新浪 A 股实时行情，并映射字段名以适配后续逻辑。"""
    def _fetch() -> pd.DataFrame:
        df_sina = ak.stock_zh_a_spot_sina()
        
        column_map = {
            "code": "代码",
            "name": "名称",
            "trade": "最新价",
            "per": "市盈率-动态",
            "amount": "成交额",
            "volume": "成交量",
            "mktcap": "总市值",
            "nmc": "流通市值"
        }
        
        df_mapped = df_sina.rename(columns=column_map)
        # 统一代码格式，去掉 sh/sz 前缀
        df_mapped["代码"] = df_mapped["代码"].str.replace("sh", "").str.replace("sz", "")
        return df_mapped

    df = call_with_retry("stock_zh_a_spot_sina", _fetch, validate=df_nonempty)
    return df

# ... 后面保持不变 ...
