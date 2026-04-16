"""
阶段 2（新顺序下）：行情侧「漏斗」——近 250 日最大回撤。

日 K（``stock_zh_a_hist`` / 腾讯备用源）放在 **PE + 财务** 之后，
仅对已通过毛利率、现金流等条件的少数标的请求，显著降低被封概率。

PE 初筛在 ``universe.apply_pe_prefilter`` 中完成。
"""

from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timedelta

import pandas as pd

from deep_value_funnel import config
from deep_value_funnel.hist_fetch import fetch_kline_qfq_normalized

logger = logging.getLogger(__name__)


def _hist_date_span() -> tuple[str, str]:
    """构造日 K 起止日期（向前多取自然日，保证约 250 个交易日）。"""
    end = datetime.now()
    start = end - timedelta(days=420)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def compute_max_drawdown_250(code: str, last_price: float) -> float | None:
    """
    计算相对于近 ``config.HIST_WINDOW`` 根 K 线「最高价」的最大回撤比例。

    定义：dd = 1 - last_price / max_high；
    当现价较高点下跌 50% 时，dd=0.5。
    """
    start_date, end_date = _hist_date_span()

    try:
        df = fetch_kline_qfq_normalized(code, start_date, end_date)
    except Exception:
        logger.exception("[%s] 获取日 K（多数据源）均失败", code)
        return None

    if "最高" not in df.columns or "收盘" not in df.columns:
        logger.warning("[%s] K 线字段异常：%s", code, list(df.columns))
        return None

    tail = df.tail(config.HIST_WINDOW)
    if tail.empty:
        return None

    max_high = pd.to_numeric(tail["最高"], errors="coerce").max()
    if not max_high or max_high <= 0 or not last_price or last_price <= 0:
        return None

    return float(1.0 - (last_price / max_high))


def screen_drawdown_stage(fin_passed: list[dict]) -> list[dict]:
    """
    对已通过财务条件的候选列表逐只拉日 K，计算回撤并过滤。

    输入 ``fin_passed``：每项为 ``screen_financials`` 返回的字典（**不含** ``drawdown``）。
    输出：浅拷贝并写入 ``drawdown``，且满足 ``drawdown >= DRAWDOWN_MIN`` 的列表。
    """
    candidates = list(fin_passed)
    if config.MAX_HIST_CANDIDATES is not None:
        candidates = candidates[: config.MAX_HIST_CANDIDATES]
        logger.info(
            "调试模式：日 K / 回撤阶段仅处理前 %s 只（财务已通过）",
            config.MAX_HIST_CANDIDATES,
        )

    out: list[dict] = []
    total = len(candidates)
    for i, fin in enumerate(candidates, start=1):
        code = str(fin["代码"]).zfill(6)
        name = str(fin["名称"])
        price = float(fin["最新价"])
        logger.info("回撤/K 线漏斗 [%s/%s] %s %s …", i, total, code, name)

        dd = compute_max_drawdown_250(code, price)
        if dd is None:
            continue
        if dd < config.DRAWDOWN_MIN:
            continue

        row = fin.copy()
        row["drawdown"] = dd
        out.append(row)

        if config.HIST_INTER_STOCK_SLEEP > 0 and i < total:
            gap = config.HIST_INTER_STOCK_SLEEP + random.uniform(0.0, 0.45)
            time.sleep(gap)

    logger.info(
        "回撤过滤（近 %s 日、回撤>=%.0f%%）：保留 %s / %s 只",
        config.HIST_WINDOW,
        config.DRAWDOWN_MIN * 100,
        len(out),
        total,
    )
    return out
