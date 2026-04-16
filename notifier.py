#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
筛选结果推送：读取 ``deep_value_pool_*.csv``，通过 Server酱（ServerChan）Webhook 发送 Markdown。

``PUSH_KEY`` 从环境变量读取，缺失时跳过推送（不打断主流程）。
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)

EMPTY_POOL_MESSAGE = "今日市场无极度低估标的，继续耐心等待。"

# Server酱³（Turbo）与旧版 sc.ftqq.com
_SCT_PREFIX = "SCT"


def _serverchan_url(push_key: str) -> str:
    key = push_key.strip()
    if key.upper().startswith(_SCT_PREFIX):
        return f"https://sctapi.ftqq.com/{key}.send"
    return f"https://sc.ftqq.com/{key}.send"


def _fmt_num(v, *, suffix: str = "", ndigits: int | None = None) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    try:
        x = float(v)
    except (TypeError, ValueError):
        return str(v)
    if ndigits is not None:
        x = round(x, ndigits)
    s = f"{x:g}" if ndigits is None else f"{x:.{ndigits}f}"
    return f"{s}{suffix}"


def build_markdown_from_pool(df: pd.DataFrame) -> str:
    """将深度价值池 DataFrame 转为排版清晰的 Markdown（无数据时返回空串，由调用方替换为固定文案）。"""
    if df is None or df.empty:
        return ""

    want = [
        ("代码", "股票代码"),
        ("名称", "名称"),
        ("市盈率-动态", "PE（动态）"),
        ("最新价", "最新价"),
        ("股息率_pct", "股息率（%）"),
        ("回撤幅度_pct", "回撤幅度（%）"),
        ("近三年平均分红率_pct", "近三年平均分红率（%）"),
        ("分红条件说明", "分红条件"),
    ]

    lines: list[str] = ["## 深度价值池", ""]
    for n, (_, row) in enumerate(df.iterrows(), start=1):
        code = row.get("代码", "")
        name = row.get("名称", "")
        lines.append(f"### {n}. {code} {name}")
        lines.append("")
        for col, label in want:
            if col in ("代码", "名称"):
                continue
            if col not in df.columns:
                continue
            val = row.get(col)
            if col == "股息率_pct" or col == "回撤幅度_pct" or col == "近三年平均分红率_pct":
                lines.append(f"- **{label}**：{_fmt_num(val, ndigits=3, suffix='')}")
            elif col == "市盈率-动态":
                lines.append(f"- **{label}**：{_fmt_num(val, ndigits=3)}")
            elif col == "最新价":
                lines.append(f"- **{label}**：{_fmt_num(val, ndigits=3)}")
            else:
                text = "" if val is None or (isinstance(val, float) and pd.isna(val)) else str(val).strip()
                lines.append(f"- **{label}**：{text or '—'}")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def send_serverchan_markdown(*, title: str, desp: str, push_key: str, timeout: int = 30) -> None:
    """调用 Server酱接口发送 Markdown（``desp`` 支持 Markdown）。"""
    url = _serverchan_url(push_key)
    resp = requests.post(
        url,
        data={"title": title, "desp": desp},
        timeout=timeout,
    )
    resp.raise_for_status()
    try:
        payload = resp.json()
    except ValueError:
        logger.info("Server酱响应非 JSON，HTTP %s，正文前 200 字：%s", resp.status_code, resp.text[:200])
        return
    if not isinstance(payload, dict):
        return
    errno = payload.get("errno")
    if errno is not None and int(errno) != 0:
        raise RuntimeError(f"Server酱返回错误 errno={errno}: {payload.get('errmsg')}")
    code = payload.get("code")
    if code is not None and int(code) != 0:
        err = payload.get("message") or payload.get("data") or payload
        raise RuntimeError(f"Server酱返回错误：{err}")


def notify_deep_value_pool(csv_path: Path | str) -> None:
    """
    读取最终池 CSV，若有 ``PUSH_KEY`` 则推送摘要。

    - 仅表头、无数据行：推送固定文案 ``EMPTY_POOL_MESSAGE``。
    - 有数据：推送结构化 Markdown。
    """
    path = Path(csv_path).resolve()
    if not path.exists():
        logger.warning("推送跳过：CSV 不存在：%s", path)
        return

    push_key = (os.environ.get("PUSH_KEY") or "").strip()
    if not push_key:
        logger.info("未设置环境变量 PUSH_KEY，跳过 Server酱推送。")
        return

    df = pd.read_csv(path, encoding="utf-8-sig")
    title_date = path.stem.replace("deep_value_pool_", "")
    title = f"A股深度价值漏斗｜{title_date}"

    if df.empty:
        desp = EMPTY_POOL_MESSAGE
    else:
        desp = build_markdown_from_pool(df)
        if not desp.strip():
            desp = EMPTY_POOL_MESSAGE

    try:
        send_serverchan_markdown(title=title, desp=desp, push_key=push_key)
        logger.info("Server酱推送成功：%s", title)
    except Exception:
        logger.exception("Server酱推送失败")
