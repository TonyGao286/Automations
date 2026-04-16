"""
网络请求封装：重试、退避与节流。

东财、新浪等数据源在高频访问下可能返回空数据、429 或断连，
此处统一做「可恢复错误」的重试与 ``time.sleep`` 节奏控制。
"""

from __future__ import annotations

import http.client
import logging
import random
import time
from typing import Callable, TypeVar

import pandas as pd
import urllib3.exceptions

from deep_value_funnel import config

logger = logging.getLogger(__name__)

T = TypeVar("T")


def _is_transient_connection_error(exc: BaseException) -> bool:
    """判断是否为常见的可重试网络断连（东财 push 接口易触发）。"""
    if isinstance(
        exc,
        (
            ConnectionError,
            TimeoutError,
            urllib3.exceptions.ProtocolError,
            http.client.RemoteDisconnected,
            http.client.IncompleteRead,
        ),
    ):
        return True
    # requests 常包装为 ConnectionError，子原因里带 RemoteDisconnected 文本
    text = f"{type(exc).__name__} {exc!s}".lower()
    if "remote end closed connection" in text or "connection aborted" in text:
        return True
    if "connection reset" in text:
        return True
    return False


def _sleep_throttle() -> None:
    """请求后的节流休眠（固定 + 随机抖动）。"""
    delay = config.REQUEST_BASE_SLEEP + random.uniform(0.0, config.REQUEST_JITTER)
    time.sleep(delay)


def call_with_retry(
    label: str,
    func: Callable[[], T],
    validate: Callable[[T], bool] | None = None,
    *,
    max_retries: int | None = None,
) -> T:
    """
    调用 ``func`` 并在失败时重试。

    :param label: 日志用简短描述（如股票代码 + 接口名）
    :param func: 无参可调用，返回任意类型（通常为 DataFrame 或标量）
    :param validate: 可选；若返回 False 视为失败并触发重试
    :param max_retries: 覆盖 ``config.MAX_RETRIES``（仍表示「最多重试几次」，不含首次）
    """
    attempts = int(max_retries if max_retries is not None else config.MAX_RETRIES)
    attempts = max(1, attempts)
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            result = func()
            ok = True
            if validate is not None:
                ok = validate(result)
            if not ok:
                raise ValueError(f"{label}: 校验未通过（空数据或字段缺失）")
            _sleep_throttle()
            return result
        except Exception as exc:  # noqa: BLE001 — 数据源异常类型繁杂，统一记录后重试
            last_exc = exc
            wait = config.RETRY_BACKOFF_BASE**attempt + random.uniform(0, 0.6)
            if _is_transient_connection_error(exc):
                wait += config.CONNECTION_RETRY_EXTRA_BASE + attempt * config.CONNECTION_RETRY_EXTRA_STEP
            is_last = attempt >= attempts
            if is_last:
                logger.warning(
                    "[%s] 第 %s/%s 次失败：%s；已达重试上限",
                    label,
                    attempt,
                    attempts,
                    exc,
                )
            else:
                logger.warning(
                    "[%s] 第 %s/%s 次失败：%s；%.2f 秒后重试",
                    label,
                    attempt,
                    attempts,
                    exc,
                    wait,
                )
            time.sleep(wait)
    assert last_exc is not None
    raise last_exc


def df_nonempty(df: pd.DataFrame | None) -> bool:
    """判断 DataFrame 是否非空。"""
    return df is not None and not df.empty
