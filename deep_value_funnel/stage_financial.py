"""
阶段 1（新顺序下）：财务「漏斗」——毛利率、五年经营现金流、经营现金流/净利润。

设计要点：
- 本阶段排在 **PE 初筛之后、日 K 回撤之前**，先用财务条件砍掉绝大部分标的，
  再对幸存者拉 ``stock_zh_a_hist``（及腾讯备用源），降低 K 线接口触封概率。
- 毛利率与「经营现金流/净利润」来自东财 ``stock_financial_analysis_indicator_em``；
  「连续 5 个完整年度经营现金流为正」使用 ``stock_cash_flow_sheet_by_yearly_em``。
- 上述 akshare 调用均经 ``call_with_retry`` + 请求后节流。
"""

from __future__ import annotations

import logging

import akshare as ak
import pandas as pd

from deep_value_funnel import config
from deep_value_funnel.http_utils import call_with_retry, df_nonempty
from deep_value_funnel.symbols import to_em_h10_code

logger = logging.getLogger(__name__)


def _fetch_indicator(sec_code: str) -> pd.DataFrame:
    def _go() -> pd.DataFrame:
        return ak.stock_financial_analysis_indicator_em(symbol=sec_code)

    return call_with_retry(f"{sec_code}:indicator_em", _go, validate=df_nonempty)


def _fetch_cashflow_yearly(em_h10: str) -> pd.DataFrame:
    def _go() -> pd.DataFrame:
        return ak.stock_cash_flow_sheet_by_yearly_em(symbol=em_h10)

    return call_with_retry(f"{em_h10}:cashflow_yearly", _go, validate=df_nonempty)


def _latest_report_row(ind: pd.DataFrame) -> pd.Series | None:
    """按报告期降序取最新一期。"""
    if ind.empty:
        return None
    d = ind.copy()
    d["_rd"] = pd.to_datetime(d["REPORT_DATE"], errors="coerce")
    d = d.sort_values("_rd", ascending=False)
    return d.iloc[0]


def _check_gross_margin(latest: pd.Series) -> tuple[bool, float | None]:
    """销售毛利率（字段 ``XSMLL``，单位为 %）。"""
    if "XSMLL" not in latest.index:
        return False, None
    gm = float(pd.to_numeric(latest["XSMLL"], errors="coerce"))
    if pd.isna(gm):
        return False, None
    return gm >= config.GROSS_MARGIN_MIN, gm


def _check_ocf_vs_np(latest: pd.Series) -> tuple[bool, float | None]:
    """
    最新一期：经营现金流净额 > 净利润。

    东财 ``NCO_NETPROFIT`` 为「经营活动产生的现金流量净额 / 归属母公司净利润」的比值，
    当其大于 1 时，等价于经营现金流高于净利润（同口径下的近似替代）。
    """
    if "NCO_NETPROFIT" not in latest.index:
        return False, None
    ratio = float(pd.to_numeric(latest["NCO_NETPROFIT"], errors="coerce"))
    if pd.isna(ratio):
        return False, None
    return ratio > 1.0, ratio


def _check_five_years_positive_ocf(cfy: pd.DataFrame) -> bool:
    """最近 5 个完整会计年度（年报 12-31）经营现金流净额均 > 0。"""
    if cfy.empty or "REPORT_DATE" not in cfy.columns or "NETCASH_OPERATE" not in cfy.columns:
        return False
    d = cfy.copy()
    d["_rd"] = pd.to_datetime(d["REPORT_DATE"], errors="coerce")
    annual = d[(d["_rd"].dt.month == 12) & (d["_rd"].dt.day == 31)].copy()
    annual = annual.sort_values("_rd", ascending=False).head(5)
    if len(annual) < 5:
        return False
    vals = pd.to_numeric(annual["NETCASH_OPERATE"], errors="coerce")
    return bool((vals > 0).all())


def screen_financials(row: pd.Series) -> dict | None:
    """
    对单行候选做财务深度过滤（**不要求**已计算 ``drawdown``）。

    必需列：``代码``、``名称``、``最新价``、``市盈率-动态``、``sec_code``（东财 ``600519.SH`` 形式）。

    成功则返回字典（含 ``indicator_df``，**不含** ``drawdown``，由后续 K 线阶段写入）。
    """
    code = str(row["代码"]).zfill(6)
    sec = str(row["sec_code"])
    em_h10 = to_em_h10_code(code)

    try:
        ind = _fetch_indicator(sec)
    except Exception:
        logger.exception("[%s] 拉取主要财务指标失败", code)
        return None

    latest = _latest_report_row(ind)
    if latest is None:
        return None

    ok_gm, gm = _check_gross_margin(latest)
    ok_ratio, nco_np = _check_ocf_vs_np(latest)
    if not (ok_gm and ok_ratio):
        return None

    try:
        cfy = _fetch_cashflow_yearly(em_h10)
    except Exception:
        logger.exception("[%s] 拉取年度现金流量表失败", code)
        return None

    if not _check_five_years_positive_ocf(cfy):
        return None

    return {
        "代码": code,
        "名称": row["名称"],
        "最新价": float(row["最新价"]),
        "市盈率-动态": float(row["市盈率-动态"]),
        "sec_code": sec,
        "销售毛利率_最近一期pct": gm,
        "经营现金流净额_净利润比_最近一期": nco_np,
        "indicator_df": ind,
    }
