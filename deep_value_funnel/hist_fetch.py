"""
A 股日 K 线多数据源拉取与列名统一。

东财 ``stock_zh_a_hist`` 依赖 ``push2his.eastmoney.com``，在部分网络/运营商环境下
易出现 ``RemoteDisconnected``；腾讯 ``stock_zh_a_hist_tx`` 走不同域名，常可作为主源。

对外统一为与东财一致的 ``日期`` / ``最高`` / ``收盘`` 等字段，供回撤计算使用。
"""

from __future__ import annotations

import logging
import time
from typing import Literal

import akshare as ak
import pandas as pd

from deep_value_funnel import config
from deep_value_funnel.http_utils import call_with_retry, df_nonempty

logger = logging.getLogger(__name__)

Provider = Literal["tencent", "eastmoney"]


def _to_tx_symbol(code: str) -> str:
    """腾讯接口：``sh600519`` / ``sz000001``（小写）。"""
    c = str(code).zfill(6)
    return f"sh{c}" if c.startswith("6") else f"sz{c}"


def _fetch_tencent_raw(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    return ak.stock_zh_a_hist_tx(
        symbol=_to_tx_symbol(code),
        start_date=start_date,
        end_date=end_date,
        adjust="qfq",
    )


def _fetch_eastmoney_raw(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    return ak.stock_zh_a_hist(
        symbol=str(code).zfill(6),
        period="daily",
        start_date=start_date,
        end_date=end_date,
        adjust="qfq",
    )


def _normalize_to_em_columns(df: pd.DataFrame, provider: Provider) -> pd.DataFrame:
    """将不同数据源的列对齐为东财日 K 风格（至少含 日期、最高、收盘）。"""
    out = df.copy()
    if provider == "tencent":
        # 腾讯：date, open, close, high, low, amount
        colmap = {"date": "日期", "open": "开盘", "close": "收盘", "high": "最高", "low": "最低"}
        out = out.rename(columns={k: v for k, v in colmap.items() if k in out.columns})
    if "日期" not in out.columns:
        raise ValueError(f"{provider}: 缺少日期列，实际列={list(out.columns)}")
    if "最高" not in out.columns or "收盘" not in out.columns:
        raise ValueError(f"{provider}: 缺少最高/收盘列，实际列={list(out.columns)}")
    out["日期"] = pd.to_datetime(out["日期"], errors="coerce")
    out = out.dropna(subset=["日期"]).sort_values("日期").reset_index(drop=True)
    out["最高"] = pd.to_numeric(out["最高"], errors="coerce")
    out["收盘"] = pd.to_numeric(out["收盘"], errors="coerce")
    return out


def fetch_kline_qfq_normalized(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    按 ``config.HIST_PROVIDER_ORDER`` 依次尝试，返回对齐列名后的日 K（前复权）。

    任一数据源成功即返回；全部失败则抛出最后一个异常。
    """
    last_exc: Exception | None = None
    for i, provider in enumerate(config.HIST_PROVIDER_ORDER):
        label = f"{code}:kline:{provider}"
        try:
            if provider == "tencent":

                def _go() -> pd.DataFrame:
                    return _fetch_tencent_raw(code, start_date, end_date)

                raw = call_with_retry(
                    label,
                    _go,
                    validate=df_nonempty,
                    max_retries=config.HIST_PROVIDER_RETRIES,
                )
            elif provider == "eastmoney":

                def _go2() -> pd.DataFrame:
                    return _fetch_eastmoney_raw(code, start_date, end_date)

                raw = call_with_retry(
                    label,
                    _go2,
                    validate=df_nonempty,
                    max_retries=config.HIST_PROVIDER_RETRIES,
                )
            else:
                continue

            norm = _normalize_to_em_columns(raw, provider)
            if len(norm) < 60:
                raise ValueError(f"{provider}: 有效 K 线不足 60 根（{len(norm)}）")
            logger.debug("[%s] 日 K 使用数据源：%s（%s 根）", code, provider, len(norm))
            return norm
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            logger.warning(
                "[%s] 数据源 %s 失败：%s",
                code,
                provider,
                exc,
            )
            if i + 1 < len(config.HIST_PROVIDER_ORDER):
                time.sleep(config.HIST_FALLBACK_PAUSE)

    assert last_exc is not None
    raise last_exc
