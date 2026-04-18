#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
太辰光 (300570) 每日自动分析脚本
====================================
设计目标：在 GitHub Actions 上每个交易日定时运行，
采集当日行情、资金流向、均线偏离，生成 JSON 日报并推送微信通知。

运行方式：
    python analyze_tcg.py                     # 使用默认代码 300570
    python analyze_tcg.py --code 300570       # 显式指定
    python analyze_tcg.py --no-notify         # 跳过推送（调试用）
"""

import argparse
import json
import logging
import os
import sys
import time
import warnings
from datetime import datetime, timedelta
from pathlib import Path

# ── CI 环境 UTF-8 输出 + 清除代理 ─────────────────────────────────
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

warnings.filterwarnings("ignore")
for _k in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "all_proxy", "ALL_PROXY"):
    os.environ[_k] = ""

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


# ═══════════════════════════════════════════════════════════════════
#  工具函数
# ═══════════════════════════════════════════════════════════════════

def _safe(fn, default=None, retries: int = 2, delay: float = 2.0):
    """带重试的安全调用包装，任意异常返回 default 而非崩溃。"""
    for attempt in range(retries + 1):
        try:
            return fn()
        except Exception as exc:
            if attempt < retries:
                logger.warning("第 %d 次重试（%s）", attempt + 1, exc)
                time.sleep(delay)
            else:
                logger.warning("放弃（%s）", exc)
    return default


def _pct(v) -> float | None:
    """安全转为 float，失败返回 None。"""
    try:
        return round(float(v), 3)
    except (TypeError, ValueError):
        return None


# ═══════════════════════════════════════════════════════════════════
#  数据采集
# ═══════════════════════════════════════════════════════════════════

def fetch_quote(code: str) -> dict:
    """采集实时行情快照。"""
    logger.info("[1/4] 采集实时行情 ...")
    spot = _safe(lambda: ak.stock_zh_a_spot_em(), default=pd.DataFrame())
    if spot is None or spot.empty:
        logger.error("实时行情获取失败，返回空 dict")
        return {}

    row = spot[spot["代码"] == code]
    if row.empty:
        logger.error("未找到代码 %s", code)
        return {}

    r = row.iloc[0]
    return {
        "price":         _pct(r.get("最新价")),
        "change_pct":    _pct(r.get("涨跌幅")),
        "change_amt":    _pct(r.get("涨跌额")),
        "open":          _pct(r.get("今开")),
        "high":          _pct(r.get("最高")),
        "low":           _pct(r.get("最低")),
        "prev_close":    _pct(r.get("昨收")),
        "volume_hand":   _pct(r.get("成交量")),   # 手
        "turnover_pct":  _pct(r.get("换手率")),   # %
        "amplitude_pct": _pct(r.get("振幅")),     # %
        "volume_ratio":  _pct(r.get("量比")),
        "pe_ttm":        _pct(r.get("市盈率-动态")),
        "pb":            _pct(r.get("市净率")),
        "total_mktcap":  _pct(r.get("总市值")),
        "float_mktcap":  _pct(r.get("流通市值")),
        "d60_chg":       _pct(r.get("60日涨跌幅")),
        "ytd_chg":       _pct(r.get("年初至今涨跌幅")),
    }


def fetch_ma_and_stats(code: str) -> dict:
    """采集 60 日 K 线，计算均线偏离和短期统计。"""
    logger.info("[2/4] 采集 60 日历史 K 线 ...")
    end   = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d")  # 多取 30 日保证够 60 根

    hist = _safe(
        lambda: ak.stock_zh_a_hist(symbol=code, period="daily",
                                    start_date=start, end_date=end, adjust="qfq"),
        default=pd.DataFrame(),
    )
    if hist is None or hist.empty:
        logger.warning("历史 K 线获取失败")
        return {}

    hist.columns = hist.columns.str.strip()
    close_col = "收盘" if "收盘" in hist.columns else hist.columns[4]
    amp_col   = "振幅" if "振幅" in hist.columns else None
    closes = hist[close_col].astype(float)

    latest = float(closes.iloc[-1])
    returns = closes.pct_change().dropna()

    result: dict = {
        "latest_close": round(latest, 2),
        "d5_chg":       round((closes.iloc[-1] / closes.iloc[-5] - 1) * 100, 2) if len(closes) >= 5 else None,
        "d20_chg":      round((closes.iloc[-1] / closes.iloc[-20] - 1) * 100, 2) if len(closes) >= 20 else None,
        "ann_vol_20d":  round(returns.tail(20).std() * (252 ** 0.5) * 100, 2) if len(returns) >= 20 else None,
        "max_dd_60d":   None,
        "ma": {},
    }

    # 均线偏离
    for n in (5, 10, 20, 60):
        if len(closes) >= n:
            ma_val = float(closes.tail(n).mean())
            result["ma"][f"MA{n}"] = {
                "value": round(ma_val, 2),
                "dev_pct": round((latest - ma_val) / ma_val * 100, 2),
            }

    # 多头排列判断（MA5 > MA10 > MA20 > MA60）
    mas = result["ma"]
    if len(mas) == 4:
        vals = [mas[f"MA{n}"]["value"] for n in (5, 10, 20, 60)]
        result["bull_aligned"] = all(vals[i] > vals[i + 1] for i in range(len(vals) - 1))
    else:
        result["bull_aligned"] = None

    # 60 日最大回撤
    if len(closes) >= 20:
        rolling_max = closes.cummax()
        dd = (closes - rolling_max) / rolling_max
        result["max_dd_60d"] = round(float(dd.min()) * 100, 2)

    # 近 5 日振幅均值
    if amp_col and amp_col in hist.columns:
        amps = hist[amp_col].astype(float)
        result["avg_amp_5d"] = round(float(amps.tail(5).mean()), 2)

    return result


def fetch_fund_flow(code: str) -> list[dict]:
    """采集近 5 日资金流向。"""
    logger.info("[3/4] 采集资金流向 ...")
    market = "sz" if not code.startswith("6") else "sh"
    df = _safe(
        lambda: ak.stock_individual_fund_flow(stock=code, market=market),
        default=pd.DataFrame(),
    )
    if df is None or df.empty:
        return []

    rows = []
    for _, r in df.tail(5).iterrows():
        rows.append({
            "date":             str(r.get("日期", "")),
            "close":            _pct(r.get("收盘价")),
            "change_pct":       _pct(r.get("涨跌幅")),
            "major_net":        _pct(r.get("主力净流入-净额")),      # 元
            "major_net_pct":    _pct(r.get("主力净流入-净占比")),    # %
            "super_net":        _pct(r.get("超大单净流入-净额")),
            "retail_net":       _pct(r.get("小单净流入-净额")),
        })
    return rows


def fetch_individual_info(code: str) -> dict:
    """采集个股基本信息（行业、上市日期等）。"""
    logger.info("[4/4] 采集个股基本信息 ...")
    df = _safe(lambda: ak.stock_individual_info_em(symbol=code), default=pd.DataFrame())
    if df is None or df.empty:
        return {}

    info = {}
    # 返回格式通常是 item / value 两列
    try:
        if "item" in df.columns and "value" in df.columns:
            for _, r in df.iterrows():
                info[str(r["item"])] = str(r["value"])
        else:
            # 有时是转置格式
            info = df.iloc[0].to_dict()
    except Exception:
        pass
    return info


# ═══════════════════════════════════════════════════════════════════
#  信号生成
# ═══════════════════════════════════════════════════════════════════

def generate_signals(quote: dict, stats: dict, flows: list[dict]) -> list[str]:
    """基于当日数据生成可读性强的短句信号列表。"""
    signals: list[str] = []

    # ── 换手率 ────────────────────────────────────────────────────
    tr = quote.get("turnover_pct")
    if tr is not None:
        if tr >= 15:
            signals.append(f"换手率 {tr}% — 极度活跃，短期博弈氛围浓")
        elif tr >= 8:
            signals.append(f"换手率 {tr}% — 活跃，主力/游资参与明显")
        elif tr <= 2:
            signals.append(f"换手率 {tr}% — 低迷，观望情绪偏重")

    # ── 均线排列 ──────────────────────────────────────────────────
    if stats.get("bull_aligned") is True:
        signals.append("均线多头排列（MA5>MA10>MA20>MA60），中期趋势向上")
    elif stats.get("bull_aligned") is False:
        signals.append("均线空头排列，中期趋势偏弱")

    # ── MA20 偏离 ─────────────────────────────────────────────────
    ma20 = stats.get("ma", {}).get("MA20", {}).get("dev_pct")
    if ma20 is not None:
        if ma20 >= 15:
            signals.append(f"价格偏离 MA20 达 +{ma20}%，短期超涨风险较高")
        elif ma20 <= -10:
            signals.append(f"价格偏离 MA20 达 {ma20}%，存在超跌反弹机会")

    # ── 资金流向 ─────────────────────────────────────────────────
    if flows:
        today_flow = flows[-1]
        major_net = today_flow.get("major_net")
        if major_net is not None:
            wan = major_net / 10000
            if wan >= 3000:
                signals.append(f"今日主力净流入 {wan:.0f} 万 — 大资金积极买入")
            elif wan <= -3000:
                signals.append(f"今日主力净流出 {wan:.0f} 万 — 主力撤退信号")
            else:
                signals.append(f"今日主力净流入 {wan:+.0f} 万")

        # 连续流向判断（近 3 日）
        if len(flows) >= 3:
            last3 = [f.get("major_net", 0) or 0 for f in flows[-3:]]
            if all(v > 0 for v in last3):
                signals.append("近 3 日主力持续净流入，资金面偏积极")
            elif all(v < 0 for v in last3):
                signals.append("近 3 日主力持续净流出，资金面偏谨慎")

    # ── 振幅 ──────────────────────────────────────────────────────
    amp = quote.get("amplitude_pct")
    if amp is not None and amp >= 8:
        signals.append(f"今日振幅 {amp}%，盘中波动剧烈，注意风险控制")

    return signals


# ═══════════════════════════════════════════════════════════════════
#  主流程
# ═══════════════════════════════════════════════════════════════════

def analyze(code: str = DEFAULT_CODE) -> dict:
    """执行全流程分析，返回结构化 dict。"""
    today = datetime.now().strftime("%Y-%m-%d")
    result: dict = {
        "code":   code,
        "name":   DEFAULT_NAME,
        "date":   today,
        "quote":  {},
        "stats":  {},
        "flows":  [],
        "info":   {},
        "signals": [],
    }

    result["quote"]  = fetch_quote(code)
    result["stats"]  = fetch_ma_and_stats(code)
    result["flows"]  = fetch_fund_flow(code)
    result["info"]   = fetch_individual_info(code)
    result["signals"] = generate_signals(result["quote"], result["stats"], result["flows"])

    return result


def save_json(data: dict, out_dir: Path = Path(".")) -> Path:
    """将分析结果保存为 JSON 日报文件。"""
    fname = out_dir / f"tcg_daily_{data['date'].replace('-', '')}.json"
    fname.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("日报已保存：%s", fname.resolve())
    return fname


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="太辰光每日自动分析")
    parser.add_argument("--code",      default=DEFAULT_CODE, help="股票代码（默认 300570）")
    parser.add_argument("--no-notify", action="store_true",  help="跳过方糖推送")
    parser.add_argument("-o", "--output-dir", default=".",   help="日报 JSON 输出目录")
    args = parser.parse_args(argv)

    data    = analyze(args.code)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = save_json(data, out_dir)

    if not args.no_notify:
        try:
            from notifier import notify_tcg_daily
            notify_tcg_daily(json_path)
        except Exception as exc:
            logger.warning("通知推送异常（不影响主流程）：%s", exc)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
