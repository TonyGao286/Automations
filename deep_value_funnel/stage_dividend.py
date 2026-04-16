"""
阶段 3（新顺序下）：分红「漏斗」——股息率（东财口径）与近三年平均分红率。

上游需已通过 PE、财务与日 K 回撤；``stock_fhps_detail_em`` 经 ``call_with_retry`` 拉取。

数据来源 ``ak.stock_fhps_detail_em``：
- ``现金分红-股息率``：东财在对应分配方案下给出的股息率（小数形式，如 0.05 表示 5%）。
- ``现金分红-现金分红比例``：与「每 10 股派息（元）」数值一致，可结合 ``总股本`` 与
  ``stock_financial_analysis_indicator_em`` 中的 ``PARENTNETPROFIT`` 还原分红占净利润比。
"""

from __future__ import annotations

import logging

import akshare as ak
import pandas as pd

from deep_value_funnel import config
from deep_value_funnel.http_utils import call_with_retry, df_nonempty

logger = logging.getLogger(__name__)


def _fetch_fhps(code: str) -> pd.DataFrame:
    def _go() -> pd.DataFrame:
        return ak.stock_fhps_detail_em(symbol=str(code).zfill(6))

    return call_with_retry(f"{code}:fhps_detail", _go, validate=df_nonempty)


def _pick_latest_dividend_yield(fh: pd.DataFrame) -> float | None:
    """取「已实施」方案中、报告期最新的一条股息率（小数）。"""
    if "方案进度" not in fh.columns:
        return None
    done = fh[fh["方案进度"].astype(str).str.contains("实施", na=False)].copy()
    if done.empty:
        return None
    done["_rd"] = pd.to_datetime(done["报告期"], errors="coerce")
    done = done.sort_values("_rd", ascending=False)
    for _, r in done.iterrows():
        dv = pd.to_numeric(r.get("现金分红-股息率"), errors="coerce")
        if pd.notna(dv):
            return float(dv)
    return None


def _three_year_avg_payout(fh: pd.DataFrame, ind: pd.DataFrame) -> float | None:
    """
    计算最近三个「年报」分配方案（已实施）的分红率平均值（净利润口径，百分数）。

    单年分红率 = (每 10 股派息元数 / 10 * 总股本) / 归属母公司净利润 * 100。
    """
    if "方案进度" not in fh.columns:
        return None
    done = fh[fh["方案进度"].astype(str).str.contains("实施", na=False)].copy()
    if done.empty:
        return None
    done["_rd"] = pd.to_datetime(done["报告期"], errors="coerce")
    annual = done[(done["_rd"].dt.month == 12) & (done["_rd"].dt.day == 31)].copy()
    annual = annual.sort_values("_rd", ascending=False)

    ind_local = ind.copy()
    ind_local["_rd"] = pd.to_datetime(ind_local["REPORT_DATE"], errors="coerce")

    payouts: list[float] = []
    for _, fr in annual.iterrows():
        if len(payouts) >= 3:
            break
        rd = fr["_rd"]
        if pd.isna(rd):
            continue
        # 与利润表年报对齐：同自然年的 12 月报告期取最新一条（兼容 12-30/12-31 披露差异）
        hit = ind_local[
            (ind_local["_rd"].dt.year == rd.year) & (ind_local["_rd"].dt.month == 12)
        ].sort_values("_rd", ascending=False)
        if hit.empty or "PARENTNETPROFIT" not in hit.columns:
            continue
        np_val = float(pd.to_numeric(hit.iloc[0]["PARENTNETPROFIT"], errors="coerce"))
        cash_per10 = float(pd.to_numeric(fr.get("现金分红-现金分红比例"), errors="coerce"))
        shares = float(pd.to_numeric(fr.get("总股本"), errors="coerce"))
        if np_val <= 0 or shares <= 0 or cash_per10 <= 0:
            continue
        cash_total = cash_per10 / 10.0 * shares
        payouts.append(cash_total / np_val * 100.0)

    if len(payouts) < 3:
        return None
    return float(sum(payouts[:3]) / 3.0)


def get_dividend_metrics_for_export(code: str, ind: pd.DataFrame) -> dict:
    """
    仅用于中间态 CSV：拉取分红送配并计算股息率 / 三年平均分红率，**不参与**分红硬条件过滤。

    与正式漏斗中 ``screen_dividend`` 使用同一套 ``call_with_retry`` 与计算公式，便于人工核对。
    """
    c = str(code).zfill(6)
    try:
        fh = _fetch_fhps(c)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[%s] step2 分红展示数据拉取失败：%s", c, exc)
        return {
            "股息率_东财最近实施_小数": None,
            "股息率_最近实施_pct": None,
            "近三年平均分红率_pct": None,
            "分红数据备注": f"拉取失败: {exc!s}",
        }

    dv = _pick_latest_dividend_yield(fh)
    payout = _three_year_avg_payout(fh, ind)
    dv_pct = round(float(dv) * 100, 4) if dv is not None else None
    return {
        "股息率_东财最近实施_小数": dv,
        "股息率_最近实施_pct": dv_pct,
        "近三年平均分红率_pct": payout,
        "分红数据备注": "",
    }


def screen_dividend(fin_result: dict) -> dict | None:
    """
    在已通过财务条件的 ``fin_result`` 上验证分红条件。

    ``fin_result`` 必须包含 ``indicator_df``（财务阶段缓存的主要指标表）。
    """
    code = str(fin_result["代码"]).zfill(6)
    ind = fin_result.get("indicator_df")
    if ind is None or ind.empty:
        return None

    try:
        fh = _fetch_fhps(code)
    except Exception:
        logger.exception("[%s] 拉取分红送配详情失败", code)
        return None

    dv = _pick_latest_dividend_yield(fh)
    avg_payout = _three_year_avg_payout(fh, ind)

    ok_yield = dv is not None and dv >= config.DIV_YIELD_MIN
    ok_payout = avg_payout is not None and avg_payout >= config.PAYOUT_RATIO_MIN
    if not (ok_yield or ok_payout):
        return None

    out = {k: v for k, v in fin_result.items() if k != "indicator_df"}
    out["股息率_东财最近实施_小数"] = dv
    out["近三年平均分红率_pct"] = avg_payout
    out["分红条件说明"] = (
        "股息率达标" if ok_yield else ""
    ) + ("；" if ok_yield and ok_payout else "") + ("三年分红率达标" if ok_payout else "")
    return out
