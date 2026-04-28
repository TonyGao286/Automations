"""
阶段 0：构建基础股票池。

数据来源：
- ``ak.stock_zh_a_spot_sina``：使用新浪接口获取快照，避开东财反爬封锁。
- ``ak.stock_history_dividend``：拉取上市日期（新浪汇总页）。
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import akshare as ak
import pandas as pd

from deep_value_funnel import config
from deep_value_funnel.http_utils import call_with_retry, df_nonempty
from deep_value_funnel.symbols import is_star_or_main_board_a, is_st_name

logger = logging.getLogger(__name__)


def _load_spot_universe() -> pd.DataFrame:
    """
    拉重新浪 A 股实时行情。
    由于东财接口在 GitHub Actions 环境下易被拦截，此处切换为新浪数据源并进行字段映射。
    """

    def _fetch() -> pd.DataFrame:
        # 获取全量 A 股实时行情
        df_sina = ak.stock_zh_a_spot_sina()
        
        # 字段名映射表：将新浪列名转换为代码逻辑通用的中文列名
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
        
        # 处理代码格式：新浪可能包含 sh/sz 前缀，统一提取为 6 位纯数字代码
        df_mapped["代码"] = df_mapped["代码"].str.extract(r'(\d{6})')
        
        return df_mapped

    # 使用 retry 机制确保网络波动时能自动重试
    df = call_with_retry("stock_zh_a_spot_sina", _fetch, validate=df_nonempty)
    return df


def _load_listing_dates() -> pd.DataFrame:
    """拉取新浪「历史分红」汇总表（含上市日期）。"""

    def _fetch() -> pd.DataFrame:
        return ak.stock_history_dividend()

    df = call_with_retry("stock_history_dividend", _fetch, validate=df_nonempty)
    return df


def build_base_universe(as_of: date | None = None) -> pd.DataFrame:
    """
    返回经过「ST / 北交所 / 上市年限」清洗后的行情 DataFrame。

    列至少包含：代码、名称、最新价、市盈率-动态；并附加 ``listing_date``。
    """
    as_of = as_of or date.today()
    cutoff = as_of - timedelta(days=365 * config.LISTING_MIN_YEARS)

    # 1. 加载实时快照
    spot = _load_spot_universe()
    
    # 2. 加载上市日期数据
    listing = _load_listing_dates()[["代码", "上市日期"]].copy()
    listing["代码"] = listing["代码"].astype(str).str.zfill(6)

    # 3. 合并数据
    df = spot.copy()
    df["代码"] = df["代码"].astype(str).str.zfill(6)
    df = df.merge(listing, on="代码", how="left")

    # --- 过滤 A：ST / 名称异常 ---
    mask_st = df["名称"].astype(str).apply(is_st_name)
    n_st = int(mask_st.sum())
    df = df.loc[~mask_st].copy()
    logger.info("剔除 ST 名称股票：%s 只", n_st)

    # --- 过滤 B：北交所等代码段 ---
    mask_board = df["代码"].apply(is_star_or_main_board_a)
    n_bse = int((~mask_board).sum())
    df = df.loc[mask_board].copy()
    logger.info("剔除北交所等代码段：%s 只", n_bse)

    # --- 过滤 C：上市日期（无法证明已满 5 年的保守剔除） ---
    df["listing_date"] = pd.to_datetime(df["上市日期"], errors="coerce").dt.date
    mask_age = df["listing_date"].notna() & (df["listing_date"] <= cutoff)
    n_young = int((~mask_age).sum())
    df = df.loc[mask_age].copy()
    logger.info("剔除上市未满 %s 年或缺失上市日：%s 只", config.LISTING_MIN_YEARS, n_young)

    # --- 数据类型转换 ---
    df["最新价"] = pd.to_numeric(df["最新价"], errors="coerce")
    df["市盈率-动态"] = pd.to_numeric(df["市盈率-动态"], errors="coerce")

    logger.info(
        "基础池构建完成：剩余 %s 只（统计日 %s）",
        len(df),
        as_of.isoformat(),
    )
    return df


def apply_pe_prefilter(df: pd.DataFrame) -> pd.DataFrame:
    """
    漏斗第一层：极低估值初筛（PE 在 (0, PE_MAX]）。
    剔除亏损（PE<=0）与缺失，显著减少后续 K 线请求量。
    """
    pe = df["市盈率-动态"]
    mask = pe.notna() & (pe > 0) & (pe <= config.PE_MAX)
    out = df.loc[mask].copy()
    logger.info(
        "PE 初筛（0<PE<=%s）：%s -> %s 只",
        config.PE_MAX,
        len(df),
        len(out),
    )
    return out
