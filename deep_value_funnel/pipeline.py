"""
总控流水线：串联「基础池 → PE → 财务 → 日K回撤 → 分红」，并输出 CSV。

漏斗顺序将 **日 K（最易触封）** 置于财务条件之后，显著减少 ``stock_zh_a_hist``
（及备用源）请求次数。控制台日志使用标准 ``logging``。
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

import pandas as pd

from deep_value_funnel import config
from deep_value_funnel.export_artifacts import (
    build_step2_export_rows,
    save_step1_basic_pool,
    save_step1_comparison,
    save_step2_finance_pool,
)
from deep_value_funnel.stage_dividend import screen_dividend
from deep_value_funnel.stage_financial import screen_financials
from deep_value_funnel.stage_market import screen_drawdown_stage
from deep_value_funnel.symbols import to_em_sec_code
from deep_value_funnel.universe import apply_pe_prefilter, build_base_universe

logger = logging.getLogger(__name__)


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )


def run_screening(
    *,
    max_hist: int | None = None,
    max_deep: int | None = None,
    verbose: bool = False,
    artifact_dir: Path | None = None,
) -> pd.DataFrame:
    """
    执行完整筛选并返回结果表（可能为空表）。

    :param max_hist: 覆盖 ``config.MAX_HIST_CANDIDATES``（财务通过后、进入 K 线前截断）。
    :param max_deep: 覆盖 ``config.MAX_DEEP_CANDIDATES``（PE 通过后、进入财务前截断）。
    :param artifact_dir: 中间态 ``step1_*.csv`` / ``step2_*.csv`` 输出目录；默认当前工作目录。
    """
    _setup_logging(verbose)
    if max_hist is not None:
        config.MAX_HIST_CANDIDATES = max_hist
    if max_deep is not None:
        config.MAX_DEEP_CANDIDATES = max_deep

    out_dir = (artifact_dir or Path.cwd()).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    base = build_base_universe()
    pe_pool = apply_pe_prefilter(base)

    if config.MAX_DEEP_CANDIDATES is not None:
        pe_pool = pe_pool.head(config.MAX_DEEP_CANDIDATES).copy()
        logger.info("调试模式：财务漏斗仅处理 PE 初筛后的前 %s 只股票", config.MAX_DEEP_CANDIDATES)

    save_step1_basic_pool(pe_pool, out_dir / "step1_basic_pool.csv")

    # ── Tushare 副源：构建基础池并与 AKShare 对照 ─────────────────────────────
    _run_tushare_comparison(pe_pool, out_dir)

    fin_passed: list[dict] = []
    total_pe = len(pe_pool)
    for i, (_, row) in enumerate(pe_pool.iterrows(), start=1):
        code = str(row["代码"]).zfill(6)
        logger.info("财务漏斗 [%s/%s] %s %s …", i, total_pe, code, row["名称"])

        row_ext = row.copy()
        row_ext["sec_code"] = to_em_sec_code(code)

        fin = screen_financials(row_ext)
        if fin is None:
            continue
        fin_passed.append(fin)

    step2_rows = build_step2_export_rows(fin_passed)
    save_step2_finance_pool(step2_rows, out_dir / "step2_finance_pool.csv")

    if not fin_passed:
        logger.warning("财务漏斗后无标的，提前结束（不会请求日 K）。")
        return pd.DataFrame()

    draw_ok = screen_drawdown_stage(fin_passed)
    if not draw_ok:
        logger.warning("日 K / 回撤过滤后无标的，提前结束。")
        return pd.DataFrame()

    finals: list[dict] = []
    total_dd = len(draw_ok)
    for i, fin in enumerate(draw_ok, start=1):
        code = str(fin["代码"]).zfill(6)
        logger.info("分红深度 [%s/%s] %s …", i, total_dd, code)
        res = screen_dividend(fin)
        if res is None:
            continue
        finals.append(res)

    return pd.DataFrame(finals)


def _run_tushare_comparison(ak_pe_pool: pd.DataFrame, out_dir: Path) -> None:
    """
    可选副流程：用 Tushare Pro 构建基础池 + PE 初筛，并与 AKShare 结果对照。

    若 ``config.ENABLE_TUSHARE_COMPARE`` 为 False 或 token 为空，则跳过并打印提示。
    生成文件：
    - ``step1_tushare_pool.csv``：Tushare 版 PE 初筛结果。
    - ``step1_comparison.csv``  ：双源对照表（并集 + 入选标记 + PE 差值）。
    """
    if not getattr(config, "ENABLE_TUSHARE_COMPARE", False):
        logger.info("Tushare 对照已关闭（ENABLE_TUSHARE_COMPARE=False），跳过。")
        return
    token = getattr(config, "TUSHARE_TOKEN", "")
    if not token:
        logger.warning(
            "未检测到 TUSHARE_TOKEN，跳过 Tushare 对照。"
            "请在 config.py 或环境变量中设置 TUSHARE_TOKEN。"
        )
        return

    try:
        from deep_value_funnel.universe_tushare import (  # noqa: PLC0415
            apply_pe_prefilter_tushare,
            build_tushare_universe,
        )

        logger.info("═══ Tushare 副源：开始拉取基础池 ═══")
        ts_base = build_tushare_universe()
        ts_pe_pool = apply_pe_prefilter_tushare(ts_base)

        # 落盘 Tushare 版 step1
        save_step1_basic_pool(ts_pe_pool, out_dir / "step1_tushare_pool.csv")

        # 双源对照
        cmp = save_step1_comparison(ak_pe_pool, ts_pe_pool, out_dir / "step1_comparison.csv")

        # 控制台摘要
        n_both = int(cmp["双源均入选"].sum())
        n_ak_only = int(cmp["仅AK入选"].sum())
        n_ts_only = int(cmp["仅TS入选"].sum())
        print(
            f"\n── step1 双源 PE 初筛对照 ──\n"
            f"  双源均入选（共识）：{n_both} 只\n"
            f"  仅 AKShare 入选  ：{n_ak_only} 只\n"
            f"  仅 Tushare 入选  ：{n_ts_only} 只\n"
            f"  对照表已写入：{out_dir / 'step1_comparison.csv'}\n"
        )
        logger.info("═══ Tushare 副源：对照完成 ═══")

    except Exception as exc:  # noqa: BLE001
        logger.warning("Tushare 对照流程失败（不影响主筛选）：%s", exc)


def _default_out_path() -> Path:
    today = date.today().isoformat().replace("-", "")
    return Path(f"deep_value_pool_{today}.csv")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="A 股深度价值 + 宽护城河漏斗选股")
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default=str(_default_out_path()),
        help="输出 CSV 路径（默认 deep_value_pool_YYYYMMDD.csv）",
    )
    parser.add_argument(
        "--max-hist",
        type=int,
        default=None,
        help="限制「财务通过后」进入日 K/回撤阶段的最大数量（调试）",
    )
    parser.add_argument(
        "--max-deep",
        type=int,
        default=None,
        help="限制「PE 初筛后」进入财务漏斗的最大数量（调试）",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="DEBUG 日志")
    args = parser.parse_args(argv)

    out_path = Path(args.output).resolve()
    artifact_dir = out_path.parent

    df = run_screening(
        max_hist=args.max_hist,
        max_deep=args.max_deep,
        verbose=args.verbose,
        artifact_dir=artifact_dir,
    )

    for step_name in (
        "step1_basic_pool.csv",
        "step1_tushare_pool.csv",
        "step1_comparison.csv",
        "step2_finance_pool.csv",
    ):
        p = artifact_dir / step_name
        if p.exists():
            print(f"中间态已写入：{p}")

    empty_cols = [
        "代码",
        "名称",
        "市盈率-动态",
        "最新价",
        "回撤幅度_pct",
        "销售毛利率_最近一期pct",
        "经营现金流净额_净利润比_最近一期",
        "股息率_pct",
        "近三年平均分红率_pct",
        "分红条件说明",
    ]
    if df.empty:
        logger.warning("最终无完全符合条件的股票；仍写入空表头文件：%s", out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=empty_cols).to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"未筛出标的，已生成空文件：{out_path.resolve()}")
        return 0

    df = df.copy()
    df["回撤幅度_pct"] = (pd.to_numeric(df["drawdown"], errors="coerce") * 100).round(3)
    df["股息率_pct"] = (pd.to_numeric(df["股息率_东财最近实施_小数"], errors="coerce") * 100).round(
        3
    )

    # 控制台摘要（人类可读）
    show_cols = [
        "代码",
        "名称",
        "市盈率-动态",
        "销售毛利率_最近一期pct",
        "回撤幅度_pct",
        "股息率_pct",
        "近三年平均分红率_pct",
    ]
    cols = [c for c in show_cols if c in df.columns]
    print(df[cols].to_string(index=False))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    dfc = df.drop(columns=[c for c in ("drawdown", "sec_code") if c in df.columns], errors="ignore")
    dfc.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n已保存：{out_path.resolve()}  （共 {len(df)} 行）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
