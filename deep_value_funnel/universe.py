"""
阶段 0：构建基础股票池。

数据来源：
- ``ak.stock_zh_a_spot_em``：全市场最新价与动态市盈率（一次分页拉全量，属于「最便宜」的全局快照）。
- ``ak.stock_history_dividend``：一次性获取上市日期（新浪汇总页），避免对数千只股票逐只打个股信息接口。
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
    """拉取东财沪深京 A 股实时行情。"""

    def _fetch() -> pd.DataFrame:
        return ak.stock_zh_a_spot_em()

    df = call_with_retry("stock_zh_a_spot_em", _fetch, validate=df_nonempty)
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

    spot = _load_spot_universe()
    listing = _load_listing_dates()[["代码", "上市日期"]].copy()
    listing["代码"] = listing["代码"].astype(str).str.zfill(6)

    df = spot.copy()
    df["代码"] = df["代码"].astype(str).str.zfill(6)

    df = df.merge(listing, on="代码", how="left")

    # --- ST / 名称异常 ---
    mask_st = df["名称"].astype(str).apply(is_st_name)
    n_st = int(mask_st.sum())
    df = df.loc[~mask_st].copy()
    logger.info("剔除 ST 名称股票：%s 只", n_st)

    # --- 北交所等代码段 ---
    mask_board = df["代码"].apply(is_star_or_main_board_a)
    n_bse = int((~mask_board).sum())
    df = df.loc[mask_board].copy()
    logger.info("剔除北交所等代码段：%s 只", n_bse)

    # --- 上市日期：缺失则保守剔除（无法证明已满 5 年）---
    df["listing_date"] = pd.to_datetime(df["上市日期"], errors="coerce").dt.date
    mask_age = df["listing_date"].notna() & (df["listing_date"] <= cutoff)
    n_young = int((~mask_age).sum())
    df = df.loc[mask_age].copy()
    logger.info("剔除上市未满 %s 年或缺失上市日：%s 只", config.LISTING_MIN_YEARS, n_young)

    # --- 数值列 ---
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
