#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
太辰光 (300570) 每日自动分析脚本
====================================
设计目标：在 GitHub Actions 上每个交易日定时运行，
采集当日行情、资金流向、均线偏离，生成 JSON 日报并推送微信通知。

架构说明（两阶段解耦）：
  Phase 1  并行采集  ──  用 ThreadPoolExecutor 同时拉取 4 个接口，
                         结果写入 raw_cache/{code}_{date}.json
  Phase 2  离线分析  ──  读取缓存 JSON，纯 CPU 计算，无网络依赖
  即使某个接口卡死/超时，其余数据仍可完整保存，第二阶段照常运行。

运行方式：
    python analyze_tcg.py                     # 默认：采集 + 分析
    python analyze_tcg.py --code 300570       # 显式指定代码
    python analyze_tcg.py --fetch-only        # 只采集，保存缓存后退出
    python analyze_tcg.py --skip-fetch        # 跳过采集，直接读缓存分析
    python analyze_tcg.py --no-notify         # 跳过推送（调试用）
"""

import argparse
import json
import logging
import os
import socket
import sys
import time
import warnings
from concurrent.futures import Future, ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout
from datetime import datetime, timedelta
from pathlib import Path

# ── CI 环境 UTF-8 输出 + 清除代理 ─────────────────────────────────
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

warnings.filterwarnings("ignore")
for _k in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "all_proxy", "ALL_PROXY"):
    os.environ[_k] = ""

# 全局 socket 超时：防止网络请求被无限阻塞导致 Action 被取消
socket.setdefaulttimeout(45)

# ── 给所有 requests 调用强制注入浏览器 User-Agent ─────────────────
# 东方财富等接口对无 UA 的请求会直接 RemoteDisconnected（疑似反爬）
# akshare 默认不带 UA，因此在 import akshare 前先打补丁
import requests as _requests

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Connection":      "keep-alive",
}
_orig_session_request = _requests.Session.request

def _patched_request(self, method, url, **kwargs):
    headers = kwargs.pop("headers", None) or {}
    merged  = {**_DEFAULT_HEADERS, **headers}   # 用户传入的 header 优先级更高
    return _orig_session_request(self, method, url, headers=merged, **kwargs)

_requests.Session.request = _patched_request

import akshare as ak
import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("analyze_tcg")

# ── 默认目标股票 ───────────────────────────────────────────────────
DEFAULT_CODE = "300570"
DEFAULT_NAME = "太辰光"

# 单个采集任务最长等待时间（秒），超过则视为失败不阻塞主流程
FETCH_TASK_TIMEOUT = 60
# _safe 默认重试间隔（秒）和重试次数
RETRY_DELAY  = 4.0
RETRY_COUNT  = 3
# 东方财富主源失败时立即切兜底，不浪费时间反复重试（每次重试要等 4/8/16s）
EM_FAST_FAIL_RETRY = 0   # 0 = 失败一次即放弃，立刻切到 sina/雪球


# ═══════════════════════════════════════════════════════════════════
#  工具函数
# ═══════════════════════════════════════════════════════════════════

def _safe(fn, default=None, retries: int = RETRY_COUNT, delay: float = RETRY_DELAY):
    """带指数退避重试的安全调用包装，任意异常返回 default 而非崩溃。"""
    for attempt in range(retries + 1):
        try:
            return fn()
        except Exception as exc:
            if attempt < retries:
                wait = delay * (2 ** attempt)   # 指数退避：4s → 8s → 16s
                logger.warning("  [重试 %d/%d] %s  等待 %.0fs ...", attempt + 1, retries, exc, wait)
                time.sleep(wait)
            else:
                logger.warning("  [放弃] %s", exc)
    return default


def _pct(v) -> float | None:
    """安全转为 float，失败返回 None。"""
    try:
        return round(float(v), 3)
    except (TypeError, ValueError):
        return None


def _load_cache(cache_path: Path) -> dict | None:
    """读取原始数据缓存，失败返回 None。"""
    if not cache_path.exists():
        return None
    try:
        return json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("读取缓存失败：%s", exc)
        return None


# ═══════════════════════════════════════════════════════════════════
#  数据采集（单项，各自独立，供并行调用）
# ═══════════════════════════════════════════════════════════════════

def _quote_from_em(code: str) -> dict | None:
    """主源：东方财富全市场快照（数据最全，含市值/PE/PB/换手率等）。"""
    spot = _safe(lambda: ak.stock_zh_a_spot_em(), default=None, retries=EM_FAST_FAIL_RETRY)
    if spot is None or spot.empty:
        return None
    row = spot[spot["代码"] == code]
    if row.empty:
        return None
    r = row.iloc[0]
    return {
        "price":         _pct(r.get("最新价")),
        "change_pct":    _pct(r.get("涨跌幅")),
        "change_amt":    _pct(r.get("涨跌额")),
        "open":          _pct(r.get("今开")),
        "high":          _pct(r.get("最高")),
        "low":           _pct(r.get("最低")),
        "prev_close":    _pct(r.get("昨收")),
        "volume_hand":   _pct(r.get("成交量")),
        "turnover_pct":  _pct(r.get("换手率")),
        "amplitude_pct": _pct(r.get("振幅")),
        "volume_ratio":  _pct(r.get("量比")),
        "pe_ttm":        _pct(r.get("市盈率-动态")),
        "pb":            _pct(r.get("市净率")),
        "total_mktcap":  _pct(r.get("总市值")),
        "float_mktcap":  _pct(r.get("流通市值")),
        "d60_chg":       _pct(r.get("60日涨跌幅")),
        "ytd_chg":       _pct(r.get("年初至今涨跌幅")),
        "_source":       "eastmoney",
    }


def _quote_from_sina(code: str) -> dict | None:
    """
    兜底源：新浪财经实时行情 stock_bid_ask_em。
    若东财全市场拉取失败，至少能拿到价格、涨跌幅等关键字段。
    字段比东财少（无 PE/PB/市值），但保住最重要的当日行情。
    """
    market_prefix = "sh" if code.startswith("6") else "sz"
    sina_code = f"{market_prefix}{code}"
    df = _safe(lambda: ak.stock_zh_a_minute(symbol=sina_code, period="1", adjust="qfq"),
               default=None, retries=1)
    if df is None or df.empty:
        return None
    last = df.iloc[-1]
    open_p  = float(df.iloc[0]["open"])
    close_p = float(last["close"])
    return {
        "price":      _pct(close_p),
        "open":       _pct(open_p),
        "high":       _pct(df["high"].astype(float).max()),
        "low":        _pct(df["low"].astype(float).min()),
        "change_pct": _pct((close_p - open_p) / open_p * 100 if open_p else None),
        "_source":    "sina",
    }


def fetch_quote(code: str) -> dict:
    """采集实时行情快照。东财失败时回退新浪。失败时抛 RuntimeError。"""
    logger.info("[quote] 采集实时行情 ...")
    quote = _quote_from_em(code)
    if quote:
        return quote

    logger.warning("[quote] 东财源失败，尝试新浪兜底源 ...")
    quote = _quote_from_sina(code)
    if quote:
        logger.info("[quote] 已从新浪兜底获取（字段少于东财）")
        return quote

    raise RuntimeError("东财和新浪两个行情源均失败")


def _kline_from_em(code: str) -> list[dict] | None:
    """主源：东方财富日线（带前复权和振幅字段）。"""
    end   = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d")
    hist = _safe(
        lambda: ak.stock_zh_a_hist(symbol=code, period="daily",
                                   start_date=start, end_date=end, adjust="qfq"),
        default=None,
        retries=EM_FAST_FAIL_RETRY,
    )
    if hist is None or hist.empty:
        return None
    hist.columns = hist.columns.str.strip()
    return hist.to_dict(orient="records")


def _kline_from_sina(code: str) -> list[dict] | None:
    """兜底源：新浪日线 stock_zh_a_daily（字段名不同，需做映射）。"""
    market_prefix = "sh" if code.startswith("6") else "sz"
    sina_code = f"{market_prefix}{code}"
    end_dt   = datetime.now()
    start_dt = end_dt - timedelta(days=90)
    df = _safe(
        lambda: ak.stock_zh_a_daily(symbol=sina_code, adjust="qfq",
                                    start_date=start_dt.strftime("%Y%m%d"),
                                    end_date=end_dt.strftime("%Y%m%d")),
        default=None,
        retries=1,
    )
    if df is None or df.empty:
        return None
    df = df.copy()
    df.rename(columns={"date": "日期", "open": "开盘", "high": "最高",
                       "low":  "最低", "close": "收盘", "volume": "成交量"},
              inplace=True)
    if "最高" in df.columns and "最低" in df.columns and "收盘" in df.columns:
        prev_close = df["收盘"].shift(1)
        df["振幅"] = (df["最高"] - df["最低"]) / prev_close * 100
    return df.to_dict(orient="records")


def fetch_kline(code: str) -> list[dict]:
    """采集 90 日 K 线。东财失败时回退新浪。失败时抛 RuntimeError。"""
    logger.info("[kline] 采集 90 日历史 K 线 ...")
    rows = _kline_from_em(code)
    if rows:
        return rows

    logger.warning("[kline] 东财源失败，尝试新浪兜底源 ...")
    rows = _kline_from_sina(code)
    if rows:
        logger.info("[kline] 已从新浪兜底获取（共 %d 根）", len(rows))
        return rows

    raise RuntimeError("东财和新浪两个 K 线源均失败")


def fetch_fund_flow(code: str) -> list[dict]:
    """采集近 5 日资金流向原始数据。失败时抛 RuntimeError。"""
    logger.info("[flow] 采集资金流向 ...")
    market = "sz" if not code.startswith("6") else "sh"
    df = _safe(
        lambda: ak.stock_individual_fund_flow(stock=code, market=market),
        default=None,
    )
    if df is None or df.empty:
        raise RuntimeError("资金流向接口返回空数据")

    rows = []
    for _, r in df.tail(5).iterrows():
        rows.append({
            "date":          str(r.get("日期", "")),
            "close":         _pct(r.get("收盘价")),
            "change_pct":    _pct(r.get("涨跌幅")),
            "major_net":     _pct(r.get("主力净流入-净额")),
            "major_net_pct": _pct(r.get("主力净流入-净占比")),
            "super_net":     _pct(r.get("超大单净流入-净额")),
            "retail_net":    _pct(r.get("小单净流入-净额")),
        })
    return rows


def _info_from_em(code: str) -> dict | None:
    """主源：东方财富个股基本信息（item / value 两列结构）。"""
    df = _safe(lambda: ak.stock_individual_info_em(symbol=code),
               default=None, retries=EM_FAST_FAIL_RETRY)
    if df is None or df.empty:
        return None
    info = {}
    try:
        if "item" in df.columns and "value" in df.columns:
            for _, r in df.iterrows():
                info[str(r["item"])] = str(r["value"])
        else:
            info = {str(k): str(v) for k, v in df.iloc[0].to_dict().items()}
    except Exception:
        return None
    info["_source"] = "eastmoney"
    return info


def _info_from_xueqiu(code: str) -> dict | None:
    """兜底源：雪球个股基本信息（字段名英文，需做映射保持下游兼容）。"""
    market_prefix = "SH" if code.startswith("6") else "SZ"
    xq_code = f"{market_prefix}{code}"
    df = _safe(lambda: ak.stock_individual_basic_info_xq(symbol=xq_code),
               default=None, retries=1)
    if df is None or df.empty:
        return None
    try:
        kv = dict(zip(df["item"].astype(str), df["value"].astype(str)))
    except Exception:
        return None
    # 把雪球英文/中文字段统一为东财风格的中文 key，下游分析无感
    mapping = {
        "org_short_name_cn": "股票简称", "main_operation_business": "主营业务",
        "established_date":  "成立日期", "listed_date": "上市时间",
        "actual_controller": "实控人",   "affiliate_industry": "所属行业",
        "staff_num":         "员工人数", "reg_asset": "注册资本",
    }
    info = {mapping.get(k, k): v for k, v in kv.items()}
    info["_source"] = "xueqiu"
    return info


def fetch_individual_info(code: str) -> dict:
    """采集个股基本信息。东财失败时回退雪球。失败时抛 RuntimeError。"""
    logger.info("[info] 采集个股基本信息 ...")
    info = _info_from_em(code)
    if info:
        return info

    logger.warning("[info] 东财源失败，尝试雪球兜底源 ...")
    info = _info_from_xueqiu(code)
    if info:
        logger.info("[info] 已从雪球兜底获取")
        return info

    raise RuntimeError("东财和雪球两个基本信息源均失败")


# ═══════════════════════════════════════════════════════════════════
#  Phase 1：并行采集，保存原始缓存
# ═══════════════════════════════════════════════════════════════════

def fetch_all_parallel(code: str, cache_dir: Path) -> dict:
    """
    并行采集全部数据，无论成功失败都将已拿到的部分保存至缓存文件。
    返回原始数据 dict（含 quote / kline / flows / info）。
    """
    today = datetime.now().strftime("%Y-%m-%d")
    raw: dict = {
        "code":  code,
        "date":  today,
        "quote": {},
        "kline": [],
        "flows": [],
        "info":  {},
    }

    tasks: list[tuple[str, callable]] = [
        ("quote", lambda: fetch_quote(code)),
        ("kline", lambda: fetch_kline(code)),
        ("flows", lambda: fetch_fund_flow(code)),
        ("info",  lambda: fetch_individual_info(code)),
    ]
    MAX_WORKERS = 4

    logger.info("═══ Phase 1：并行采集（共 %d 个接口，并发 %d）═══",
                len(tasks), MAX_WORKERS)
    success, failed = 0, 0
    fetch_errors: dict[str, str] = {}   # 记录失败原因，写入缓存供事后排查

    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="fetch") as pool:
        future_map: dict[Future, str] = {
            pool.submit(fn): key for key, fn in tasks
        }

        # as_completed + per-task timeout：某个接口卡死不阻塞其余
        try:
            for future in as_completed(future_map, timeout=FETCH_TASK_TIMEOUT + 30):
                key = future_map[future]
                try:
                    result = future.result(timeout=FETCH_TASK_TIMEOUT)
                    # 各 fetch 函数失败时抛 RuntimeError，不再静默返回空值
                    # 走到这里 result 一定是有效数据
                    raw[key] = result
                    logger.info("  ✓ [%s] 采集成功", key)
                    success += 1
                except FuturesTimeout:
                    msg = f"超时（>{FETCH_TASK_TIMEOUT}s）"
                    logger.warning("  ✗ [%s] %s，已跳过", key, msg)
                    fetch_errors[key] = msg
                    failed += 1
                except Exception as exc:
                    msg = str(exc)
                    logger.warning("  ✗ [%s] %s", key, msg)
                    fetch_errors[key] = msg
                    failed += 1
        except FuturesTimeout:
            logger.warning("并行采集总超时，部分数据可能缺失")
            for f, key in future_map.items():
                if not f.done():
                    fetch_errors[key] = "总超时被终止"
                    failed += 1

    raw["_fetch_errors"] = fetch_errors   # 写入缓存便于复盘
    logger.info("采集完成：成功 %d / 失败 %d%s",
                success, failed,
                f"  [缺失：{', '.join(fetch_errors)}]" if fetch_errors else "")

    # 无论成功率如何，立即将已有数据持久化
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"{code}_{today.replace('-', '')}_raw.json"
    try:
        cache_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2, default=str),
                              encoding="utf-8")
        logger.info("原始数据缓存已保存：%s", cache_path.resolve())
    except Exception as exc:
        logger.warning("缓存保存失败：%s", exc)

    return raw


# ═══════════════════════════════════════════════════════════════════
#  Phase 2：离线计算（从原始缓存 dict 出发，纯 CPU，无网络）
# ═══════════════════════════════════════════════════════════════════

def compute_ma_and_stats(kline_records: list[dict]) -> dict:
    """从 K 线原始记录计算均线偏离和短期统计。"""
    if not kline_records:
        logger.warning("[分析] K 线数据为空，跳过均线计算")
        return {}

    hist = pd.DataFrame(kline_records)
    hist.columns = hist.columns.str.strip()
    close_col = "收盘" if "收盘" in hist.columns else hist.columns[4]
    amp_col   = "振幅" if "振幅" in hist.columns else None
    closes    = hist[close_col].astype(float)

    latest  = float(closes.iloc[-1])
    returns = closes.pct_change().dropna()

    result: dict = {
        "latest_close": round(latest, 2),
        "d5_chg":  round((closes.iloc[-1] / closes.iloc[-5]  - 1) * 100, 2) if len(closes) >= 5  else None,
        "d20_chg": round((closes.iloc[-1] / closes.iloc[-20] - 1) * 100, 2) if len(closes) >= 20 else None,
        "ann_vol_20d": round(returns.tail(20).std() * (252 ** 0.5) * 100, 2) if len(returns) >= 20 else None,
        "max_dd_60d": None,
        "ma": {},
    }

    for n in (5, 10, 20, 60):
        if len(closes) >= n:
            ma_val = float(closes.tail(n).mean())
            result["ma"][f"MA{n}"] = {
                "value":   round(ma_val, 2),
                "dev_pct": round((latest - ma_val) / ma_val * 100, 2),
            }

    mas = result["ma"]
    if len(mas) == 4:
        vals = [mas[f"MA{n}"]["value"] for n in (5, 10, 20, 60)]
        result["bull_aligned"] = all(vals[i] > vals[i + 1] for i in range(len(vals) - 1))
    else:
        result["bull_aligned"] = None

    if len(closes) >= 20:
        rolling_max = closes.cummax()
        dd = (closes - rolling_max) / rolling_max
        result["max_dd_60d"] = round(float(dd.min()) * 100, 2)

    if amp_col and amp_col in hist.columns:
        amps = hist[amp_col].astype(float)
        result["avg_amp_5d"] = round(float(amps.tail(5).mean()), 2)

    return result


def generate_signals(quote: dict, stats: dict, flows: list[dict]) -> list[str]:
    """基于当日数据生成可读性强的短句信号列表。"""
    signals: list[str] = []

    tr = quote.get("turnover_pct")
    if tr is not None:
        if tr >= 15:
            signals.append(f"换手率 {tr}% — 极度活跃，短期博弈氛围浓")
        elif tr >= 8:
            signals.append(f"换手率 {tr}% — 活跃，主力/游资参与明显")
        elif tr <= 2:
            signals.append(f"换手率 {tr}% — 低迷，观望情绪偏重")

    if stats.get("bull_aligned") is True:
        signals.append("均线多头排列（MA5>MA10>MA20>MA60），中期趋势向上")
    elif stats.get("bull_aligned") is False:
        signals.append("均线空头排列，中期趋势偏弱")

    ma20 = stats.get("ma", {}).get("MA20", {}).get("dev_pct")
    if ma20 is not None:
        if ma20 >= 15:
            signals.append(f"价格偏离 MA20 达 +{ma20}%，短期超涨风险较高")
        elif ma20 <= -10:
            signals.append(f"价格偏离 MA20 达 {ma20}%，存在超跌反弹机会")

    if flows:
        today_flow = flows[-1]
        major_net  = today_flow.get("major_net")
        if major_net is not None:
            wan = major_net / 10000
            if wan >= 3000:
                signals.append(f"今日主力净流入 {wan:.0f} 万 — 大资金积极买入")
            elif wan <= -3000:
                signals.append(f"今日主力净流出 {wan:.0f} 万 — 主力撤退信号")
            else:
                signals.append(f"今日主力净流入 {wan:+.0f} 万")

        if len(flows) >= 3:
            last3 = [f.get("major_net", 0) or 0 for f in flows[-3:]]
            if all(v > 0 for v in last3):
                signals.append("近 3 日主力持续净流入，资金面偏积极")
            elif all(v < 0 for v in last3):
                signals.append("近 3 日主力持续净流出，资金面偏谨慎")

    amp = quote.get("amplitude_pct")
    if amp is not None and amp >= 8:
        signals.append(f"今日振幅 {amp}%，盘中波动剧烈，注意风险控制")

    return signals


def analyze_from_raw(raw: dict) -> dict:
    """
    Phase 2：从原始缓存 dict 中进行离线计算，生成最终报告。
    纯 CPU 操作，无任何网络请求。
    """
    logger.info("═══ Phase 2：离线分析（读缓存计算）═══")
    stats = compute_ma_and_stats(raw.get("kline", []))
    quote = raw.get("quote", {})
    flows = raw.get("flows", [])

    return {
        "code":    raw.get("code", DEFAULT_CODE),
        "name":    DEFAULT_NAME,
        "date":    raw.get("date", datetime.now().strftime("%Y-%m-%d")),
        "quote":   quote,
        "stats":   stats,
        "flows":   flows,
        "info":    raw.get("info", {}),
        "signals": generate_signals(quote, stats, flows),
    }


# ═══════════════════════════════════════════════════════════════════
#  输出
# ═══════════════════════════════════════════════════════════════════

def save_json(data: dict, out_dir: Path = Path(".")) -> Path:
    """将分析结果保存为 JSON 日报文件。"""
    fname = out_dir / f"tcg_daily_{data['date'].replace('-', '')}.json"
    fname.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("日报已保存：%s", fname.resolve())
    return fname


# ═══════════════════════════════════════════════════════════════════
#  主流程
# ═══════════════════════════════════════════════════════════════════

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="太辰光每日自动分析")
    parser.add_argument("--code",        default=DEFAULT_CODE, help="股票代码（默认 300570）")
    parser.add_argument("--no-notify",   action="store_true",  help="跳过方糖推送")
    parser.add_argument("-o", "--output-dir", default=".",     help="日报 JSON 输出目录")
    parser.add_argument("--cache-dir",   default="raw_cache",  help="原始数据缓存目录（默认 raw_cache/）")
    parser.add_argument("--fetch-only",  action="store_true",  help="只采集并保存缓存，不生成日报")
    parser.add_argument("--skip-fetch",  action="store_true",  help="跳过采集，直接从缓存读取分析")
    args = parser.parse_args(argv)

    cache_dir  = Path(args.cache_dir)
    today      = datetime.now().strftime("%Y-%m-%d")
    cache_path = cache_dir / f"{args.code}_{today.replace('-', '')}_raw.json"

    # ── Phase 1：采集 ────────────────────────────────────────────
    if args.skip_fetch:
        logger.info("--skip-fetch：跳过网络采集，读取缓存 %s", cache_path)
        raw = _load_cache(cache_path)
        if raw is None:
            logger.error("缓存文件不存在或损坏，无法继续。请先去掉 --skip-fetch 运行一次采集。")
            return 1
    else:
        raw = fetch_all_parallel(args.code, cache_dir)

    if args.fetch_only:
        logger.info("--fetch-only：采集完毕，退出（不生成日报）。")
        return 0

    # ── Phase 2：离线分析 ────────────────────────────────────────
    data    = analyze_from_raw(raw)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = save_json(data, out_dir)

    # ── 推送通知 ─────────────────────────────────────────────────
    if not args.no_notify:
        try:
            from notifier import notify_tcg_daily
            notify_tcg_daily(json_path)
        except Exception as exc:
            logger.warning("通知推送异常（不影响主流程）：%s", exc)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
