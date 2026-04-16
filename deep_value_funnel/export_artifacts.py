"""
中间态 CSV 落盘：便于核对漏斗各阶段效率与策略逻辑。

文件与最终 ``deep_value_pool_*.csv`` 同目录（由调用方传入 ``artifact_dir``）。
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from deep_value_funnel.stage_dividend import get_dividend_metrics_for_export

logger = logging.getLogger(__name__)

# ── step1 对照表列名 ──────────────────────────────────────────────────────────
COMPARE_COLS: list[str] = [
    "代码",
    "名称",
    # AKShare
    "ak_最新价",
    "ak_市盈率动态",
    "ak_总市值",
    # Tushare
    "ts_最新价",
    "ts_PE_TTM",
    "ts_总市值_万元",
    # 对照结论
    "ak_入选",
    "ts_入选",
    "仅AK入选",
    "仅TS入选",
    "双源均入选",
    "PE差值_AK减TS",
]

# step1：优先输出列（其余列按原表顺序追加在后，便于审计）
STEP1_PRIORITY_COLS: list[str] = [
    "代码",
    "名称",
    "最新价",
    "涨跌幅",
    "市盈率-动态",
    "市净率",
    "总市值",
    "流通市值",
    "上市日期",
    "listing_date",
]


def _column_order_for_step1(df: pd.DataFrame) -> list[str]:
    front = [c for c in STEP1_PRIORITY_COLS if c in df.columns]
    tail = [c for c in df.columns if c not in front]
    return front + tail


def build_step1_comparison(
    ak_pool: pd.DataFrame,
    ts_pool: pd.DataFrame,
) -> pd.DataFrame:
    """
    将 AKShare 与 Tushare 两版 PE 初筛结果合并为一张对照表。

    对照维度：
    - 各自入选的股票代码集合（并集 + 归属标记）。
    - 两源的最新价、PE 值、总市值（便于肉眼核查数据一致性）。
    - ``PE差值_AK减TS``：两源 PE 的差值，绝对值 >1 时可能存在口径/数据差异。

    :param ak_pool: AKShare PE 初筛后的 DataFrame（含 ``代码``、``名称``、``最新价``、``市盈率-动态``、``总市值``）。
    :param ts_pool: Tushare PE 初筛后的 DataFrame（含 ``代码``、``名称``、``最新价``、``市盈率-动态``、``总市值_万元``）。
    :returns: 以「代码 + 名称」为行、对照字段为列的 DataFrame。
    """
    ak = ak_pool[["代码", "名称", "最新价", "市盈率-动态"]].copy()
    ak = ak.rename(columns={"最新价": "ak_最新价", "市盈率-动态": "ak_市盈率动态"})
    if "总市值" in ak_pool.columns:
        ak["ak_总市值"] = pd.to_numeric(ak_pool["总市值"], errors="coerce")
    else:
        ak["ak_总市值"] = None
    ak["ak_入选"] = True

    ts = ts_pool[["代码", "名称", "最新价", "市盈率-动态"]].copy()
    ts = ts.rename(columns={"最新价": "ts_最新价", "市盈率-动态": "ts_PE_TTM"})
    if "总市值_万元" in ts_pool.columns:
        ts["ts_总市值_万元"] = pd.to_numeric(ts_pool["总市值_万元"], errors="coerce")
    else:
        ts["ts_总市值_万元"] = None
    ts["ts_入选"] = True

    merged = ak.merge(ts, on=["代码", "名称"], how="outer")
    merged["ak_入选"] = merged["ak_入选"].fillna(False).astype(bool)
    merged["ts_入选"] = merged["ts_入选"].fillna(False).astype(bool)
    merged["仅AK入选"] = merged["ak_入选"] & ~merged["ts_入选"]
    merged["仅TS入选"] = merged["ts_入选"] & ~merged["ak_入选"]
    merged["双源均入选"] = merged["ak_入选"] & merged["ts_入选"]
    merged["PE差值_AK减TS"] = (
        pd.to_numeric(merged["ak_市盈率动态"], errors="coerce")
        - pd.to_numeric(merged["ts_PE_TTM"], errors="coerce")
    ).round(4)

    ordered = [c for c in COMPARE_COLS if c in merged.columns]
    rest = [c for c in merged.columns if c not in ordered]
    return merged[ordered + rest].sort_values(
        ["双源均入选", "代码"], ascending=[False, True]
    ).reset_index(drop=True)


def save_step1_comparison(
    ak_pool: pd.DataFrame,
    ts_pool: pd.DataFrame,
    path: Path,
) -> pd.DataFrame:
    """
    生成并写出 step1 双源对照表 → ``step1_comparison.csv``。

    :returns: 对照表 DataFrame（便于调用方直接打印摘要）。
    """
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    df = build_step1_comparison(ak_pool, ts_pool)
    df.to_csv(path, index=False, encoding="utf-8-sig")

    n_both = int(df["双源均入选"].sum())
    n_ak_only = int(df["仅AK入选"].sum())
    n_ts_only = int(df["仅TS入选"].sum())
    logger.info(
        "step1 对照：双源均入选 %s 只 | 仅 AKShare %s 只 | 仅 Tushare %s 只 → 已写 %s",
        n_both, n_ak_only, n_ts_only, path,
    )
    return df


def save_step1_basic_pool(pe_pool: pd.DataFrame, path: Path) -> None:
    """PE 等基础初筛后的股票池 → ``step1_basic_pool.csv``。"""
    path = path.resolve()
    if pe_pool.empty:
        out = pd.DataFrame(columns=STEP1_PRIORITY_COLS)
    else:
        cols = _column_order_for_step1(pe_pool)
        out = pe_pool.loc[:, cols].copy()
    path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(path, index=False, encoding="utf-8-sig")
    logger.info("已落盘 step1：%s （%s 行）", path, len(out))


def build_step2_export_rows(fin_passed: list[dict]) -> list[dict]:
    """
    在财务通过列表上补充分红展示字段（东财 fhps），生成可写 CSV 的字典行（不含 ``indicator_df``）。
    """
    rows: list[dict] = []
    total = len(fin_passed)
    for i, fin in enumerate(fin_passed, start=1):
        code = str(fin["代码"]).zfill(6)
        logger.info("step2 落盘：补充分红展示字段 [%s/%s] %s …", i, total, code)
        ind = fin.get("indicator_df")
        if ind is None or not isinstance(ind, pd.DataFrame) or ind.empty:
            div = {
                "股息率_东财最近实施_小数": None,
                "股息率_最近实施_pct": None,
                "近三年平均分红率_pct": None,
                "分红数据备注": "缺少 indicator_df",
            }
        else:
            div = get_dividend_metrics_for_export(code, ind)
        base = {k: v for k, v in fin.items() if k != "indicator_df"}
        base.update(div)
        rows.append(base)
    return rows


def save_step2_finance_pool(export_rows: list[dict], path: Path) -> None:
    """
    财务漏斗通过后的股票池 → ``step2_finance_pool.csv``。

    ``export_rows`` 须已不含 ``indicator_df``（见 ``build_step2_export_rows``）。
    """
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    if not export_rows:
        empty_cols = [
            "代码",
            "名称",
            "最新价",
            "市盈率-动态",
            "sec_code",
            "销售毛利率_最近一期pct",
            "经营现金流净额_净利润比_最近一期",
            "股息率_东财最近实施_小数",
            "股息率_最近实施_pct",
            "近三年平均分红率_pct",
            "分红数据备注",
        ]
        pd.DataFrame(columns=empty_cols).to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("已落盘 step2（空）：%s", path)
        return

    df = pd.DataFrame(export_rows)
    priority = [
        "代码",
        "名称",
        "最新价",
        "市盈率-动态",
        "sec_code",
        "销售毛利率_最近一期pct",
        "经营现金流净额_净利润比_最近一期",
        "股息率_东财最近实施_小数",
        "股息率_最近实施_pct",
        "近三年平均分红率_pct",
        "分红数据备注",
    ]
    ordered = [c for c in priority if c in df.columns]
    rest = [c for c in df.columns if c not in ordered]
    df = df[ordered + rest]
    df.to_csv(path, index=False, encoding="utf-8-sig")
    logger.info("已落盘 step2：%s （%s 行）", path, len(df))
