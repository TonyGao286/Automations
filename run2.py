#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A 股深度价值选股脚本 v2（独立版）
====================================
选股条件（5 条，去掉 K 线回撤）：
  1. PE(TTM) <= 10
  2. 销售毛利率 >= 45%
  3. 股息率(TTM) >= 5%  OR  近三年平均分红率 >= 50%
  4. 最近连续 5 个完整年度经营现金流净额 > 0
  5. 最新期经营现金流净额 > 净利润（比值 > 1）

过滤条件（黑名单）：ST 股、北交所、上市不足 5 年

输出：2deep_value_pool_YYYYMMDD.csv
"""

# ── 必须在所有 import 之前清空代理，保证直连国内数据源 ──────────────────────────
import os
os.environ["http_proxy"]  = ""
os.environ["https_proxy"] = ""
os.environ["HTTP_PROXY"]  = ""
os.environ["HTTPS_PROXY"] = ""
os.environ["all_proxy"]   = ""
os.environ["ALL_PROXY"]   = ""

# ── 标准库 ───────────────────────────────────────────────────────────────────
import argparse
import http.client
import logging
import random
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, TypeVar

# ── 第三方 ───────────────────────────────────────────────────────────────────
import akshare as ak
import pandas as pd
import urllib3.exceptions


# ═══════════════════════════════════════════════════════════════════════════
#  全局阈值 / 节流参数
# ═══════════════════════════════════════════════════════════════════════════
PE_MAX             : float = 10.0   # PE(TTM) 上限
GROSS_MARGIN_MIN   : float = 45.0   # 销售毛利率下限（%）
DIV_YIELD_MIN      : float = 0.05   # 股息率下限（小数，0.05=5%）
PAYOUT_RATIO_MIN   : float = 50.0   # 近三年平均分红率下限（%）
LISTING_MIN_YEARS  : int   = 5      # 上市年限门槛（自然年近似）
CONSECUTIVE_OCF_YEARS: int = 5      # 连续正经营现金流年数

REQUEST_BASE_SLEEP : float = 0.40   # 每次请求后的基础休眠（秒）
REQUEST_JITTER     : float = 0.30   # 随机抖动上限（秒）
MAX_RETRIES        : int   = 6      # 最大重试次数（不含首次）
RETRY_BACKOFF_BASE : float = 1.8    # 指数退避基数（秒）
CONN_EXTRA_BASE    : float = 2.5    # 连接类错误额外等待基数（秒）
CONN_EXTRA_STEP    : float = 2.0    # 连接类错误额外等待步进（秒）

# ── 调试限速（None = 不限） ──────────────────────────────────────────────────
MAX_DEEP_CANDIDATES: int | None = None  # PE 初筛后最多进入财务漏斗的股票数


# ═══════════════════════════════════════════════════════════════════════════
#  日志
# ═══════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("run2")


# ═══════════════════════════════════════════════════════════════════════════
#  工具函数
# ═══════════════════════════════════════════════════════════════════════════
T = TypeVar("T")


def _is_transient(exc: BaseException) -> bool:
    """判断是否为可重试的临时网络错误。"""
    if isinstance(
        exc,
        (ConnectionError, TimeoutError,
         urllib3.exceptions.ProtocolError,
         http.client.RemoteDisconnected,
         http.client.IncompleteRead),
    ):
        return True
    text = f"{type(exc).__name__} {exc!s}".lower()
    return (
        "remote end closed" in text
        or "connection aborted" in text
        or "connection reset" in text
    )


def _sleep() -> None:
    """请求后节流：固定基础延迟 + 随机抖动。"""
    time.sleep(REQUEST_BASE_SLEEP + random.uniform(0.0, REQUEST_JITTER))


def retry_call(label: str, func: Callable[[], T], validate=None) -> T:
    """
    调用 func()，失败或校验不通过时按指数退避重试。

    :param label:    日志前缀（如股票代码+接口名）
    :param func:     无参可调用
    :param validate: 可选校验函数，返回 False 视为空数据失败
    """
    last: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            result = func()
            if validate is not None and not validate(result):
                raise ValueError(f"{label}: 数据为空或格式异常")
            _sleep()
            return result
        except Exception as exc:  # noqa: BLE001
            last = exc
            wait = RETRY_BACKOFF_BASE ** attempt + random.uniform(0, 0.6)
            if _is_transient(exc):
                wait += CONN_EXTRA_BASE + attempt * CONN_EXTRA_STEP
            logger.warning(
                "[%s] 第 %s/%s 次失败：%s；%.1f s 后%s",
                label, attempt, MAX_RETRIES, exc, wait,
                "放弃" if attempt >= MAX_RETRIES else "重试",
            )
            time.sleep(wait)
    assert last is not None
    raise last


def df_ok(df) -> bool:
    """校验 DataFrame 非空。"""
    return df is not None and not df.empty


# ═══════════════════════════════════════════════════════════════════════════
#  代码/市场工具
# ═══════════════════════════════════════════════════════════════════════════

def is_main_board(code: str) -> bool:
    """
    判断是否属于沪深主板/创业板/科创板。

    北交所代码以 43/83/87/92 开头，予以剔除。
    """
    c = str(code).zfill(6)
    return len(c) == 6 and c.isdigit() and not c.startswith(("43", "83", "87", "92"))


def is_st(name: str) -> bool:
    """名称中含 'ST' 即认定为 ST 股。"""
    return "ST" in str(name).upper()


def to_em_sec_code(code: str) -> str:
    """600519 -> 600519.SH；000001 -> 000001.SZ"""
    c = str(code).zfill(6)
    return f"{c}.SH" if c.startswith("6") else f"{c}.SZ"


def to_em_h10_code(code: str) -> str:
    """600519 -> SH600519；000001 -> SZ000001"""
    c = str(code).zfill(6)
    return f"SH{c}" if c.startswith("6") else f"SZ{c}"


# ═══════════════════════════════════════════════════════════════════════════
#  第 0 步：构建基础股票池
# ═══════════════════════════════════════════════════════════════════════════

def build_universe(as_of: date | None = None) -> pd.DataFrame:
    """
    拉取全市场实时行情快照（一次请求）+ 上市日期，
    过滤掉 ST、北交所、上市不足 LISTING_MIN_YEARS 年的股票。
    """
    as_of  = as_of or date.today()
    cutoff = as_of - timedelta(days=365 * LISTING_MIN_YEARS)

    logger.info("── Step 0：拉取全市场行情快照（东财）……")
    spot = retry_call("stock_zh_a_spot_em", ak.stock_zh_a_spot_em, validate=df_ok)

    logger.info("── Step 0：拉取上市日期（新浪）……")
    listing_raw = retry_call("stock_history_dividend", ak.stock_history_dividend, validate=df_ok)
    listing = listing_raw[["代码", "上市日期"]].copy()
    listing["代码"] = listing["代码"].astype(str).str.zfill(6)

    df = spot.copy()
    df["代码"] = df["代码"].astype(str).str.zfill(6)
    df = df.merge(listing, on="代码", how="left")

    # --- 过滤 ST ---
    mask_st = df["名称"].astype(str).apply(is_st)
    df = df.loc[~mask_st].copy()
    logger.info("  剔除 ST：%s 只", int(mask_st.sum()))

    # --- 过滤北交所 ---
    mask_board = df["代码"].apply(is_main_board)
    df = df.loc[mask_board].copy()
    logger.info("  剔除非沪深主板/创业/科创：%s 只", int((~mask_board).sum()))

    # --- 过滤上市年限 ---
    df["listing_date"] = pd.to_datetime(df["上市日期"], errors="coerce").dt.date
    mask_age = df["listing_date"].notna() & (df["listing_date"] <= cutoff)
    df = df.loc[mask_age].copy()
    logger.info("  剔除上市未满 %s 年：%s 只", LISTING_MIN_YEARS, int((~mask_age).sum()))

    df["最新价"]    = pd.to_numeric(df["最新价"],    errors="coerce")
    df["市盈率-动态"] = pd.to_numeric(df["市盈率-动态"], errors="coerce")

    logger.info("  基础池：剩余 %s 只", len(df))
    return df


# ═══════════════════════════════════════════════════════════════════════════
#  第 1 步：PE 初筛
# ═══════════════════════════════════════════════════════════════════════════

def pe_prefilter(df: pd.DataFrame) -> pd.DataFrame:
    """漏斗第一层：0 < PE(TTM) <= PE_MAX。"""
    pe   = df["市盈率-动态"]
    mask = pe.notna() & (pe > 0) & (pe <= PE_MAX)
    out  = df.loc[mask].copy()
    logger.info("── Step 1 PE 初筛（0<PE<=%s）：%s → %s 只", PE_MAX, len(df), len(out))
    return out


# ═══════════════════════════════════════════════════════════════════════════
#  第 2 步：财务漏斗（毛利率 / 五年经营现金流 / 经营现金流>净利润）
# ═══════════════════════════════════════════════════════════════════════════

def _latest_ind_row(ind: pd.DataFrame) -> pd.Series | None:
    """按报告期降序取最新一行。"""
    if ind.empty:
        return None
    d = ind.copy()
    d["_rd"] = pd.to_datetime(d["REPORT_DATE"], errors="coerce")
    return d.sort_values("_rd", ascending=False).iloc[0]


def _check_gross_margin(latest: pd.Series) -> tuple[bool, float | None]:
    """
    条件 2：销售毛利率 >= GROSS_MARGIN_MIN（%）。
    东财字段：XSMLL（百分数形式，如 48.5 表示 48.5%）。
    """
    if "XSMLL" not in latest.index:
        return False, None
    val = float(pd.to_numeric(latest["XSMLL"], errors="coerce"))
    if pd.isna(val):
        return False, None
    return val >= GROSS_MARGIN_MIN, val


def _check_ocf_vs_np(latest: pd.Series) -> tuple[bool, float | None]:
    """
    条件 5：经营现金流净额 > 净利润（比值 > 1）。
    东财字段：NCO_NETPROFIT（经营现金流/归属母公司净利润）。
    """
    if "NCO_NETPROFIT" not in latest.index:
        return False, None
    val = float(pd.to_numeric(latest["NCO_NETPROFIT"], errors="coerce"))
    if pd.isna(val):
        return False, None
    return val > 1.0, val


def _check_five_year_ocf(cfy: pd.DataFrame) -> bool:
    """
    条件 4：最近 5 个完整会计年度（12-31）经营现金流净额均 > 0。
    东财字段：NETCASH_OPERATE。
    """
    if "REPORT_DATE" not in cfy.columns or "NETCASH_OPERATE" not in cfy.columns:
        return False
    d = cfy.copy()
    d["_rd"] = pd.to_datetime(d["REPORT_DATE"], errors="coerce")
    annual = d[(d["_rd"].dt.month == 12) & (d["_rd"].dt.day == 31)].copy()
    annual = annual.sort_values("_rd", ascending=False).head(CONSECUTIVE_OCF_YEARS)
    if len(annual) < CONSECUTIVE_OCF_YEARS:
        return False
    vals = pd.to_numeric(annual["NETCASH_OPERATE"], errors="coerce")
    return bool((vals > 0).all())


def screen_financials(row: pd.Series) -> dict | None:
    """
    对单只股票做财务深度筛选（条件 2、4、5）。

    :param row: 至少包含 代码、名称、最新价、市盈率-动态
    :returns: 通过时返回结果字典（含 indicator_df 缓存）；未通过返回 None
    """
    code   = str(row["代码"]).zfill(6)
    sec    = to_em_sec_code(code)
    em_h10 = to_em_h10_code(code)

    # ── 主要财务指标（东财综合指标表） ──────────────────────────────────────
    try:
        ind = retry_call(
            f"{code}:indicator",
            lambda s=sec: ak.stock_financial_analysis_indicator_em(symbol=s),
            validate=df_ok,
        )
    except Exception:
        logger.exception("[%s] 财务指标拉取失败，跳过", code)
        return None

    latest = _latest_ind_row(ind)
    if latest is None:
        return None

    ok_gm, gm   = _check_gross_margin(latest)
    ok_ocf, ocf = _check_ocf_vs_np(latest)
    if not (ok_gm and ok_ocf):
        logger.debug("[%s] 毛利率/经营现金流比值未达标（毛利率=%.1f, OCF/NP比=%.2f）",
                     code, gm or -1, ocf or -1)
        return None

    # ── 年度现金流量表（校验五年连续正经营现金流） ──────────────────────────
    try:
        cfy = retry_call(
            f"{code}:cashflow_yearly",
            lambda h=em_h10: ak.stock_cash_flow_sheet_by_yearly_em(symbol=h),
            validate=df_ok,
        )
    except Exception:
        logger.exception("[%s] 年度现金流量表拉取失败，跳过", code)
        return None

    if not _check_five_year_ocf(cfy):
        logger.debug("[%s] 五年经营现金流未全部为正，跳过", code)
        return None

    return {
        "代码"         : code,
        "名称"         : row["名称"],
        "最新价"        : float(row["最新价"]),
        "市盈率-动态"   : float(row["市盈率-动态"]),
        "sec_code"     : sec,
        "销售毛利率_pct": round(gm, 2),
        "OCF_NP比"     : round(ocf, 4),
        "indicator_df" : ind,          # 分红阶段复用，省一次请求
    }


# ═══════════════════════════════════════════════════════════════════════════
#  第 3 步：分红漏斗（股息率 or 三年平均分红率）
# ═══════════════════════════════════════════════════════════════════════════

def _latest_div_yield(fh: pd.DataFrame) -> float | None:
    """取「已实施」方案中、报告期最新的股息率（小数）。"""
    if "方案进度" not in fh.columns:
        return None
    done = fh[fh["方案进度"].astype(str).str.contains("实施", na=False)].copy()
    if done.empty:
        return None
    done["_rd"] = pd.to_datetime(done["报告期"], errors="coerce")
    done = done.sort_values("_rd", ascending=False)
    for _, r in done.iterrows():
        val = pd.to_numeric(r.get("现金分红-股息率"), errors="coerce")
        if pd.notna(val):
            return float(val)
    return None


def _three_year_payout(fh: pd.DataFrame, ind: pd.DataFrame) -> float | None:
    """
    近三年年报「已实施」方案的平均分红率（%）。

    公式：每年 分红总额 / 归属母公司净利润 × 100，取最近三年均值。
    """
    if "方案进度" not in fh.columns:
        return None
    done = fh[fh["方案进度"].astype(str).str.contains("实施", na=False)].copy()
    if done.empty:
        return None
    done["_rd"] = pd.to_datetime(done["报告期"], errors="coerce")
    annual = done[(done["_rd"].dt.month == 12) & (done["_rd"].dt.day == 31)].copy()
    annual = annual.sort_values("_rd", ascending=False)

    ind2 = ind.copy()
    ind2["_rd"] = pd.to_datetime(ind2["REPORT_DATE"], errors="coerce")

    payouts: list[float] = []
    for _, fr in annual.iterrows():
        if len(payouts) >= 3:
            break
        rd = fr["_rd"]
        if pd.isna(rd):
            continue
        # 同一自然年的 12 月利润表对齐（兼容 12-30/12-31 披露差异）
        hit = ind2[
            (ind2["_rd"].dt.year == rd.year) & (ind2["_rd"].dt.month == 12)
        ].sort_values("_rd", ascending=False)
        if hit.empty or "PARENTNETPROFIT" not in hit.columns:
            continue
        np_v     = float(pd.to_numeric(hit.iloc[0]["PARENTNETPROFIT"], errors="coerce"))
        cash_p10 = float(pd.to_numeric(fr.get("现金分红-现金分红比例"), errors="coerce"))
        shares   = float(pd.to_numeric(fr.get("总股本"), errors="coerce"))
        if np_v <= 0 or shares <= 0 or cash_p10 <= 0:
            continue
        payouts.append(cash_p10 / 10.0 * shares / np_v * 100.0)

    if len(payouts) < 3:
        return None
    return round(sum(payouts[:3]) / 3.0, 2)


def screen_dividend(fin: dict) -> dict | None:
    """
    条件 3：股息率(TTM)>=5%  或  近三年平均分红率>=50%。

    :param fin: 财务漏斗输出字典，必须含 indicator_df
    :returns: 通过时返回扩展后的字典（去掉 indicator_df），否则 None
    """
    code = str(fin["代码"]).zfill(6)
    ind  = fin.get("indicator_df")
    if ind is None or ind.empty:
        return None

    try:
        fh = retry_call(
            f"{code}:fhps",
            lambda c=code: ak.stock_fhps_detail_em(symbol=c),
            validate=df_ok,
        )
    except Exception:
        logger.exception("[%s] 分红数据拉取失败，跳过", code)
        return None

    dv         = _latest_div_yield(fh)
    avg_payout = _three_year_payout(fh, ind)

    ok_yield  = dv is not None and dv >= DIV_YIELD_MIN
    ok_payout = avg_payout is not None and avg_payout >= PAYOUT_RATIO_MIN

    if not (ok_yield or ok_payout):
        logger.debug("[%s] 分红条件未达标（股息率=%.2f%%, 三年分红率=%.1f%%）",
                     code,
                     (dv or 0) * 100,
                     avg_payout or 0)
        return None

    out = {k: v for k, v in fin.items() if k != "indicator_df"}
    out["股息率_pct"]      = round(float(dv) * 100, 3) if dv is not None else None
    out["三年平均分红率_pct"] = avg_payout
    out["分红达标说明"]     = (
        ("股息率≥5%" if ok_yield else "")
        + ("；" if ok_yield and ok_payout else "")
        + ("三年分红率≥50%" if ok_payout else "")
    )
    return out


# ═══════════════════════════════════════════════════════════════════════════
#  主流程
# ═══════════════════════════════════════════════════════════════════════════

def run(max_deep: int | None = None) -> pd.DataFrame:
    """
    执行完整漏斗（PE → 财务 → 分红），返回结果 DataFrame。

    漏斗顺序说明：
    ┌──────────────┐
    │  全市场快照   │ ~5000 只
    └──────┬───────┘
           │ Step 0：剔除 ST / 北交所 / 新股
    ┌──────▼───────┐
    │  基础池       │ ~3000 只
    └──────┬───────┘
           │ Step 1：PE(TTM) <= 10
    ┌──────▼───────┐
    │  PE 初筛池    │ ~100-200 只
    └──────┬───────┘
           │ Step 2：毛利率 / 五年 OCF / OCF > NP
    ┌──────▼───────┐
    │  财务通过池   │ ~10-30 只
    └──────┬───────┘
           │ Step 3：股息率 or 三年分红率
    ┌──────▼───────┐
    │  最终标的池   │ ~0-10 只
    └─────────────┘
    """
    # ── Step 0 + 1 ──────────────────────────────────────────────────────────
    universe = build_universe()
    pe_pool  = pe_prefilter(universe)

    if max_deep is not None:
        pe_pool = pe_pool.head(max_deep).copy()
        logger.info("  调试模式：财务漏斗仅处理前 %s 只", max_deep)

    total_pe = len(pe_pool)

    # ── Step 2：财务漏斗 ──────────────────────────────────────────────────────
    logger.info("── Step 2：开始财务指标筛选（共 %s 只）……", total_pe)
    fin_passed: list[dict] = []
    for i, (_, row) in enumerate(pe_pool.iterrows(), start=1):
        code = str(row["代码"]).zfill(6)
        logger.info("  财务筛选 [%s/%s] %s %s …", i, total_pe, code, row["名称"])
        result = screen_financials(row)
        if result is not None:
            fin_passed.append(result)

    logger.info("── Step 2 完成：财务通过 %s / %s 只", len(fin_passed), total_pe)

    if not fin_passed:
        logger.warning("  财务漏斗后无标的，提前结束。")
        return pd.DataFrame()

    # ── Step 3：分红漏斗 ──────────────────────────────────────────────────────
    total_fin = len(fin_passed)
    logger.info("── Step 3：开始分红条件筛选（共 %s 只）……", total_fin)
    finals: list[dict] = []
    for i, fin in enumerate(fin_passed, start=1):
        code = str(fin["代码"]).zfill(6)
        logger.info("  分红筛选 [%s/%s] %s %s …", i, total_fin, code, fin["名称"])
        res = screen_dividend(fin)
        if res is not None:
            finals.append(res)

    logger.info("── Step 3 完成：最终入选 %s / %s 只", len(finals), total_fin)
    return pd.DataFrame(finals)


# ═══════════════════════════════════════════════════════════════════════════
#  CSV 输出
# ═══════════════════════════════════════════════════════════════════════════

SHOW_COLS = [
    "代码", "名称", "最新价", "市盈率-动态",
    "销售毛利率_pct", "OCF_NP比",
    "股息率_pct", "三年平均分红率_pct", "分红达标说明",
]


def save_result(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cols = [c for c in SHOW_COLS if c in df.columns]
    rest = [c for c in df.columns if c not in cols and c not in ("sec_code",)]
    df_out = df[cols + rest].drop(columns=["sec_code"], errors="ignore")
    df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("结果已保存：%s  （%s 行）", out_path.resolve(), len(df_out))


# ═══════════════════════════════════════════════════════════════════════════
#  命令行入口
# ═══════════════════════════════════════════════════════════════════════════

def _default_out() -> Path:
    today = date.today().strftime("%Y%m%d")
    return Path(f"2deep_value_pool_{today}.csv")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="A 股深度价值选股 v2（无 K 线回撤条件）")
    parser.add_argument(
        "-o", "--output", type=str, default=str(_default_out()),
        help="输出 CSV 路径（默认 2deep_value_pool_YYYYMMDD.csv）",
    )
    parser.add_argument(
        "--max-deep", type=int, default=None,
        help="PE 初筛后最多进入财务漏斗的数量（调试用）",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="输出 DEBUG 日志")
    args = parser.parse_args(argv)

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    max_deep = args.max_deep or MAX_DEEP_CANDIDATES

    df = run(max_deep=max_deep)

    out_path = Path(args.output).resolve()

    empty_cols = ["代码", "名称", "最新价", "市盈率-动态",
                  "销售毛利率_pct", "OCF_NP比",
                  "股息率_pct", "三年平均分红率_pct", "分红达标说明"]
    if df.empty:
        logger.warning("未筛出符合所有条件的股票；写入空表头文件：%s", out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=empty_cols).to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"\n未筛出标的，已生成空文件：{out_path}")
        return 0

    # 控制台摘要
    cols = [c for c in SHOW_COLS if c in df.columns]
    print("\n═══ 最终入选股票 ═══")
    print(df[cols].to_string(index=False))

    save_result(df, out_path)
    print(f"\n已保存：{out_path}  （共 {len(df)} 行）")

    try:
        from notifier import notify_run2_pool
        notify_run2_pool(out_path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("通知推送异常（不影响主流程）：%s", exc)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
