"""
阶段 0（Tushare 副源）：从 Tushare Pro 构建基础股票池，输出与 AKShare 版相同的标准列。

用途：
  1. 独立获取全市场每日 PE(TTM)、收盘价、上市日期。
  2. 在漏斗第一步与 AKShare 结果进行「交叉核对」，发现因接口差异导致的遗漏/误收。

所需接口（Tushare Pro）：
  - ``stock_basic``：获取股票代码、名称、上市日期、市场、交易所、st 标识。
  - ``daily_basic``：获取最新交易日的 PE(TTM)、收盘价、总市值、流通市值等。

积分要求：daily_basic 需要至少 2000 积分。
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta

import pandas as pd

from deep_value_funnel import config
from deep_value_funnel.symbols import is_star_or_main_board_a, is_st_name

logger = logging.getLogger(__name__)


def _get_ts_api():
    """延迟初始化 Tushare Pro API，避免 token 未配置时 import 就报错。"""
    import tushare as ts  # noqa: PLC0415

    token = getattr(config, "TUSHARE_TOKEN", "") or ""
    if not token:
        raise ValueError(
            "未配置 TUSHARE_TOKEN，请在 deep_value_funnel/config.py 中设置 "
            "TUSHARE_TOKEN = '你的token字符串'"
        )
    ts.set_token(token)
    return ts.pro_api()


def _latest_trade_date() -> str:
    """
    推算最近的交易日（简单算法：若当天 16:00 之后用今天，否则用前一个工作日）。
    这只是为了给 daily_basic 传一个有效的 trade_date，并非严格日历，遇到假日时
    会自动向前找最近有数据的日期（见 _fetch_daily_basic 中的回退逻辑）。
    """
    now = datetime.now()
    if now.weekday() >= 5:
        days_back = now.weekday() - 4
        d = now.date() - timedelta(days=days_back)
    elif now.hour < 16:
        d = now.date() - timedelta(days=1)
        if d.weekday() >= 5:
            d -= timedelta(days=d.weekday() - 4)
    else:
        d = now.date()
    return d.strftime("%Y%m%d")


def _fetch_stock_basic(pro) -> pd.DataFrame:
    """stock_basic：获取全部正常上市 A 股（排除北交所 BSE）的基础信息。"""
    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            df = pro.stock_basic(
                list_status="L",
                fields="ts_code,symbol,name,list_date,market,exchange",
            )
            if df is not None and not df.empty:
                return df
        except Exception as exc:  # noqa: BLE001
            wait = config.RETRY_BACKOFF_BASE ** attempt
            logger.warning("[ts:stock_basic] 第 %s 次失败：%s；%.1f 秒后重试", attempt, exc, wait)
            time.sleep(wait)
    raise RuntimeError("Tushare stock_basic 多次失败")


def _fetch_daily_basic(pro, trade_date: str, retries: int = 5) -> pd.DataFrame:
    """
    daily_basic：获取指定日期全市场每日指标。

    若指定日期无数据（节假日），自动向前最多 10 个自然日重试。
    """
    d = datetime.strptime(trade_date, "%Y%m%d").date()
    for backward in range(11):
        td = (d - timedelta(days=backward)).strftime("%Y%m%d")
        for attempt in range(1, retries + 1):
            try:
                df = pro.daily_basic(
                    trade_date=td,
                    fields="ts_code,trade_date,close,pe_ttm,total_mv,circ_mv",
                )
                if df is not None and not df.empty:
                    logger.info("[ts:daily_basic] 使用日期 %s，%s 行", td, len(df))
                    return df
                # 有数据但为空 -> 非交易日，再往前
                break
            except Exception as exc:  # noqa: BLE001
                wait = config.RETRY_BACKOFF_BASE ** attempt
                logger.warning(
                    "[ts:daily_basic(%s)] 第 %s 次失败：%s；%.1f s 后重试", td, attempt, exc, wait
                )
                time.sleep(wait)
    raise RuntimeError(f"Tushare daily_basic 在 {trade_date} 前后 10 天均无有效数据")


def _ts_code_to_6digit(ts_code: str) -> str:
    """``600519.SH`` / ``000001.SZ`` → ``600519`` / ``000001``。"""
    return ts_code.split(".")[0]


def build_tushare_universe(as_of: date | None = None) -> pd.DataFrame:
    """
    返回与 ``universe.build_base_universe`` 相同列名的清洗后股票池：

    ``代码、名称、最新价、市盈率-TTM、总市值、流通市值、listing_date``

    注意：
    - 使用 ``pe_ttm`` 字段（严格 TTM 口径），对应 AKShare 的「市盈率-动态」。
    - 输出列名刻意与 AKShare 版保持一致（``市盈率-动态`` 列），方便对照合并。
    """
    as_of = as_of or date.today()
    cutoff = as_of - timedelta(days=365 * config.LISTING_MIN_YEARS)

    pro = _get_ts_api()
    basic = _fetch_stock_basic(pro)
    trade_date = _latest_trade_date()
    daily = _fetch_daily_basic(pro, trade_date)

    basic["代码"] = basic["symbol"].astype(str).str.zfill(6)
    daily["代码"] = daily["ts_code"].apply(_ts_code_to_6digit).str.zfill(6)

    # --- 合并 ---
    df = daily.merge(
        basic[["代码", "ts_code", "name", "list_date", "market", "exchange"]],
        on="代码",
        how="inner",
    )
    df = df.rename(columns={
        "name": "名称",
        "close": "最新价",
        "pe_ttm": "市盈率-动态",  # 与 AKShare 列名统一
        "total_mv": "总市值_万元",
        "circ_mv": "流通市值_万元",
    })

    # --- 过滤北交所 ---
    mask_bse = df["exchange"].astype(str).str.upper() == "BSE"
    n_bse = int(mask_bse.sum())
    df = df.loc[~mask_bse].copy()
    logger.info("[Tushare] 剔除北交所：%s 只", n_bse)

    # --- 代码段进一步过滤（与 AKShare 版保持一致） ---
    mask_board = df["代码"].apply(is_star_or_main_board_a)
    df = df.loc[mask_board].copy()

    # --- 剔除 ST ---
    mask_st = df["名称"].astype(str).apply(is_st_name)
    n_st = int(mask_st.sum())
    df = df.loc[~mask_st].copy()
    logger.info("[Tushare] 剔除 ST：%s 只", n_st)

    # --- 上市年限 ---
    df["listing_date"] = pd.to_datetime(df["list_date"], format="%Y%m%d", errors="coerce").dt.date
    mask_age = df["listing_date"].notna() & (df["listing_date"] <= cutoff)
    n_young = int((~mask_age).sum())
    df = df.loc[mask_age].copy()
    logger.info("[Tushare] 剔除上市未满 %s 年：%s 只", config.LISTING_MIN_YEARS, n_young)

    # --- 数值清洗 ---
    df["最新价"] = pd.to_numeric(df["最新价"], errors="coerce")
    df["市盈率-动态"] = pd.to_numeric(df["市盈率-动态"], errors="coerce")

    logger.info("[Tushare] 基础池构建完成：%s 只（统计日 %s）", len(df), as_of.isoformat())
    return df


def apply_pe_prefilter_tushare(df: pd.DataFrame) -> pd.DataFrame:
    """
    与 ``universe.apply_pe_prefilter`` 相同过滤逻辑，作用于 Tushare 版基础池。

    PE 使用 ``市盈率-动态`` 列（已对齐为 pe_ttm 口径）。
    """
    pe = df["市盈率-动态"]
    mask = pe.notna() & (pe > 0) & (pe <= config.PE_MAX)
    out = df.loc[mask].copy()
    logger.info(
        "[Tushare] PE 初筛（0<PE<=%s）：%s -> %s 只",
        config.PE_MAX,
        len(df),
        len(out),
    )
    return out
