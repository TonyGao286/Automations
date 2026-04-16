"""
电力行业深度定制排雷模块
========================
针对电力设备 / 新能源 / 储能集成行业（以海博思创 HyperStrong 为核心大客户链条）
的专属财务预警体系，包含四个核心检测维度：

  1. KA 依赖与账期错配
     — 应收账款周转天数 + 应收/合同资产增速 vs 营收增速
  2. 「发出商品 vs 合同负债」勾稽关系（存货积压/验收风险）[*]核心[*]
     — 以合同资产 YoY 变化 vs 合同负债 YoY 变化作为代理指标
  3. 研发支出资本化水分
     — (开发支出期末-期初) / 净利润
  4. 上下游博弈能力
     — (应付账款+应付票据) / (应收账款+应收票据)

每个维度满分 25 分，合计「电力行业专属评分」满分 100 分。
同时输出原始计算值、风险等级（RED / YELLOW / GREEN）及说明文字。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import akshare as ak
import pandas as pd

from deep_value_funnel.http_utils import call_with_retry, df_nonempty
from deep_value_funnel.symbols import to_em_h10_code, to_em_sec_code

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
#  阈值常量（可在调用方覆盖）
# ═══════════════════════════════════════════════════════════════════════════

AR_DAYS_WARN: float = 180.0     # 应收账款周转天数预警阈值（天）
AR_DAYS_DANGER: float = 240.0   # 应收账款周转天数危险阈值（天）

# AR 增速 vs 营收增速倍数：超过该倍数触发预警
AR_REV_GROWTH_RATIO_WARN: float = 1.5

# 合同资产占总资产比例：超过该比例触发合同资产异常预警
CONTRACT_ASSET_RATIO_WARN: float = 0.15  # 15%

# 合同资产 YoY 增幅（%）：超过此值且合同负债下降时触发积压预警
GOODS_SHIPPED_GROWTH_WARN: float = 30.0  # 30%

# 研发资本化率（占净利润）预警阈值
RD_CAP_RATIO_WARN: float = 0.20   # 20%

# 上下游博弈能力：AP/AR 比率低于此值表示弱势
BARGAIN_POWER_WARN: float = 0.50

# 海博思创在公告中的关键词（含简称/全称/英文）
HYPERSTRONG_KEYWORDS: list[str] = [
    "海博思创", "HyperStrong", "hyperstrong"
]


# ═══════════════════════════════════════════════════════════════════════════
#  数据容器
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class DimResult:
    """单个排雷维度的结果容器。"""
    name: str                          # 维度名称
    score: float                       # 得分（0-25）
    risk: str = "GREEN"                # RED / YELLOW / GREEN
    raw_value: Optional[float] = None  # 核心指标原始值
    detail: str = ""                   # 人类可读说明
    sub_metrics: dict = field(default_factory=dict)  # 辅助指标明细


@dataclass
class PowerIndustryScore:
    """四维评分结果汇总。"""
    code: str
    name: str
    total_score: float = 0.0

    dim_ka: Optional[DimResult] = None
    dim_goods_shipped: Optional[DimResult] = None
    dim_rd_cap: Optional[DimResult] = None
    dim_bargain: Optional[DimResult] = None

    has_hyperstrong_flag: bool = False   # 是否检测到海博思创关联
    manual_obs_weight: float = 1.0       # 内部销售观察权重因子（0.5-1.5）
    final_score: float = 0.0             # total_score × manual_obs_weight（上限 100）
    summary: str = ""


# ═══════════════════════════════════════════════════════════════════════════
#  数据获取
# ═══════════════════════════════════════════════════════════════════════════

def _fetch_balance_sheet(em_h10: str) -> pd.DataFrame:
    """拉取资产负债表（按报告期，东财接口）。"""
    def _go() -> pd.DataFrame:
        return ak.stock_balance_sheet_by_report_em(symbol=em_h10)
    return call_with_retry(f"{em_h10}:balance_sheet", _go, validate=df_nonempty)


def _fetch_income_sheet(em_h10: str) -> pd.DataFrame:
    """拉取利润表（按报告期，东财接口）。"""
    def _go() -> pd.DataFrame:
        return ak.stock_profit_sheet_by_report_em(symbol=em_h10)
    return call_with_retry(f"{em_h10}:income_sheet", _go, validate=df_nonempty)


def _fetch_top_customers(sec_code: str) -> str | None:
    """
    尝试从东财公告文本中检索前五大客户信息。

    受限于公开接口，仅做关键词扫描（非完整 PDF 解析）；
    若接口失败则静默返回 None，不影响主流程。
    """
    try:
        df = call_with_retry(
            f"{sec_code}:disclosure",
            lambda: ak.stock_zh_a_disclosure(symbol=sec_code, keyword="前五大客户"),
            max_retries=2,
        )
        if df is None or df.empty:
            return None
        # 将公告摘要合并为字符串，供后续关键词扫描
        text_cols = [c for c in df.columns if df[c].dtype == object]
        return " ".join(df[text_cols].fillna("").values.flatten().tolist())
    except Exception:  # noqa: BLE001
        return None


# ═══════════════════════════════════════════════════════════════════════════
#  工具函数
# ═══════════════════════════════════════════════════════════════════════════

def _sort_by_report_date(df: pd.DataFrame, date_col: str = "REPORT_DATE") -> pd.DataFrame:
    """按报告期降序排序并重置索引。"""
    d = df.copy()
    d["_rd"] = pd.to_datetime(d[date_col], errors="coerce")
    return d.sort_values("_rd", ascending=False).reset_index(drop=True)


def _get_field(row: pd.Series, *candidates: str) -> float | None:
    """按候选字段名顺序取第一个可用的数值，否则返回 None。"""
    for col in candidates:
        if col in row.index:
            v = pd.to_numeric(row[col], errors="coerce")
            if pd.notna(v):
                return float(v)
    return None


def _yoy_growth_pct(current: float, prior: float) -> float | None:
    """计算同比增速（%），分母接近 0 时返回 None。"""
    if prior is None or abs(prior) < 1e-6:
        return None
    return (current - prior) / abs(prior) * 100.0


def _risk_from_score(score: float, max_score: float = 25.0) -> str:
    ratio = score / max_score
    if ratio >= 0.70:
        return "GREEN"
    if ratio >= 0.35:
        return "YELLOW"
    return "RED"


# ═══════════════════════════════════════════════════════════════════════════
#  维度 1：KA 依赖与账期错配
# ═══════════════════════════════════════════════════════════════════════════

def _dim_ka_dependency(
    bs_df: pd.DataFrame,
    income_df: pd.DataFrame,
) -> DimResult:
    """
    检测应收账款周转天数与大客户账期错配风险。

    评分逻辑（满分 25）：
    - AR 周转天数 < 120 天  → 20 分基础
    - AR 周转天数 120-180 天 → 12 分基础
    - AR 周转天数 > 180 天   → 0 分基础
    - AR 增速 / 营收增速 < 1.5 → +5 分
    - 合同资产占总资产 < 15%  → 加减 0（合理区间）
    """
    bs = _sort_by_report_date(bs_df)
    inc = _sort_by_report_date(income_df)

    if len(bs) < 2 or len(inc) < 2:
        return DimResult(
            name="KA依赖与账期错配",
            score=10.0,
            risk="YELLOW",
            detail="数据期数不足，无法完成同比计算（至少需要两期）",
        )

    row0 = bs.iloc[0]
    row1 = bs.iloc[1]
    inc0 = inc.iloc[0]
    inc1 = inc.iloc[1]

    # 应收账款 + 合同资产（代表全部"已交付未收款"敞口）
    ar0 = _get_field(row0, "ACCOUNTS_RECE", "NOTE_ACCOUNTS_RECE") or 0.0
    ca0 = _get_field(row0, "CONTRACT_ASSET") or 0.0
    total_ar0 = ar0 + ca0

    ar1 = _get_field(row1, "ACCOUNTS_RECE", "NOTE_ACCOUNTS_RECE") or 0.0
    ca1 = _get_field(row1, "CONTRACT_ASSET") or 0.0
    total_ar1 = ar1 + ca1

    # 营业收入（优先年化：用最近四个季度推算不现实，直接用报告期单值）
    rev0 = _get_field(inc0, "TOTAL_OPERATE_INCOME", "OPERATE_INCOME", "REVENUE")
    rev1 = _get_field(inc1, "TOTAL_OPERATE_INCOME", "OPERATE_INCOME", "REVENUE")

    sub: dict = {
        "应收账款_最新期_元": ar0,
        "合同资产_最新期_元": ca0,
        "营业收入_最新期_元": rev0,
    }

    # ── 计算 AR 周转天数 ─────────────────────────────────────────────────
    ar_days: float | None = None
    if rev0 and rev0 > 0 and total_ar0 > 0:
        ar_days = total_ar0 / rev0 * 365.0
    sub["应收周转天数"] = ar_days

    # ── AR 增速 vs 营收增速 ──────────────────────────────────────────────
    ar_growth = _yoy_growth_pct(total_ar0, total_ar1)
    rev_growth = _yoy_growth_pct(rev0 or 0, rev1 or 0)
    ar_rev_ratio: float | None = None
    if ar_growth is not None and rev_growth is not None and abs(rev_growth) > 1e-3:
        ar_rev_ratio = ar_growth / rev_growth
    sub["应收增速_pct"] = ar_growth
    sub["营收增速_pct"] = rev_growth
    sub["应收_营收增速比"] = ar_rev_ratio

    # ── 合同资产占总资产 ─────────────────────────────────────────────────
    total_assets = _get_field(row0, "TOTAL_ASSETS")
    ca_ratio: float | None = None
    if total_assets and total_assets > 0 and ca0 > 0:
        ca_ratio = ca0 / total_assets
    sub["合同资产占总资产比"] = ca_ratio

    # ── 打分 ─────────────────────────────────────────────────────────────
    score = 0.0
    notes: list[str] = []

    if ar_days is None:
        score += 12.0
        notes.append("应收周转天数无法计算（数据缺失），给予中性分")
    elif ar_days < 120:
        score += 20.0
        notes.append(f"应收周转 {ar_days:.0f} 天 [OK]（<120天，账期健康）")
    elif ar_days < AR_DAYS_WARN:
        score += 12.0
        notes.append(f"应收周转 {ar_days:.0f} 天 [!]（120-180天，需关注）")
    else:
        score += 0.0
        notes.append(
            f"应收周转 {ar_days:.0f} 天 [NG]（>{AR_DAYS_WARN:.0f}天，账期严重偏长，"
            f"典型大客户压款特征）"
        )

    if ar_rev_ratio is None:
        score += 3.0
        notes.append("无法计算应收/营收增速比（数据缺失）")
    elif ar_rev_ratio < AR_REV_GROWTH_RATIO_WARN:
        score += 5.0
        notes.append(f"应收/营收增速比 {ar_rev_ratio:.2f} [OK]（<1.5，正常扩张）")
    else:
        score += 0.0
        notes.append(
            f"应收/营收增速比 {ar_rev_ratio:.2f} [NG]（>{AR_REV_GROWTH_RATIO_WARN}，"
            f"应收增速远超营收，可能存在虚假收入或大额呆账风险）"
        )

    if ca_ratio is not None and ca_ratio > CONTRACT_ASSET_RATIO_WARN:
        notes.append(
            f"合同资产占总资产 {ca_ratio:.1%} [!]（>{CONTRACT_ASSET_RATIO_WARN:.0%}，"
            f"大量未验收交付物堆积）"
        )

    return DimResult(
        name="KA依赖与账期错配",
        score=min(score, 25.0),
        risk=_risk_from_score(score, 25.0),
        raw_value=ar_days,
        detail=" | ".join(notes),
        sub_metrics=sub,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  维度 2：发出商品 vs 合同负债勾稽关系 [*] 核心逻辑 [*]
# ═══════════════════════════════════════════════════════════════════════════

def _dim_goods_shipped_vs_contract_liab(
    bs_df: pd.DataFrame,
    income_df: pd.DataFrame,
) -> DimResult:
    """
    发出商品 vs 合同负债勾稽关系检测。

    背景：
    - 「发出商品」= 已发货但客户未完成验收的存货。在新准则下多以「合同资产」体现。
    - 「合同负债」= 客户预付款，反映下游对公司的认可度与支付意愿。
    - 若「合同资产（发出商品代理）」大幅增长，而「合同负债」同步下降，
      意味着：① 产品堆在客户仓库未被验收；② 客户预付意愿下降——双重积压信号。

    数据层次（优先级递减）：
    ① GOODS_SEND / GOODS_ISSUED（发出商品直接字段，部分接口有）
    ② CONTRACT_ASSET（合同资产，最佳代理）
    ③ INVENTORY YoY 变化（兜底）

    评分逻辑（满分 25）：
    - 无积压信号  → 25 分（GREEN）
    - 轻度预警    → 13 分（YELLOW）：合同资产增速>30% 但合同负债未下降
    - 严重预警    → 0 分（RED）：合同资产增速>30% 且合同负债下降
    """
    bs = _sort_by_report_date(bs_df)
    inc = _sort_by_report_date(income_df)

    if len(bs) < 2:
        return DimResult(
            name="发出商品/合同负债勾稽",
            score=12.0,
            risk="YELLOW",
            detail="数据期数不足，无法进行同比分析",
        )

    row0 = bs.iloc[0]
    row1 = bs.iloc[1]

    sub: dict = {}

    # ── Step 1：获取「发出商品」数值（优先直接字段，退而使用合同资产） ────────

    goods_sent_0 = _get_field(row0, "GOODS_SEND", "GOODS_ISSUED", "GOODS_IN_TRANSIT")
    goods_sent_1 = _get_field(row1, "GOODS_SEND", "GOODS_ISSUED", "GOODS_IN_TRANSIT")

    using_direct_field = goods_sent_0 is not None
    proxy_label = "发出商品（直接字段）"

    if not using_direct_field:
        # 退后到合同资产作为代理
        goods_sent_0 = _get_field(row0, "CONTRACT_ASSET")
        goods_sent_1 = _get_field(row1, "CONTRACT_ASSET")
        proxy_label = "合同资产（发出商品代理）"

    sub["指标类型"] = proxy_label
    sub[f"{proxy_label}_最新期_元"] = goods_sent_0
    sub[f"{proxy_label}_上期_元"] = goods_sent_1

    # ── Step 2：合同负债（新准则）/ 预收账款（旧准则）─────────────────────────
    # 新准则：CONTRACT_LIAB；旧准则：ADVANCE_RECEIVABLES
    cl0 = _get_field(row0, "CONTRACT_LIAB", "ADVANCE_RECEIVABLES", "ADVANCE_RECEIPT")
    cl1 = _get_field(row1, "CONTRACT_LIAB", "ADVANCE_RECEIVABLES", "ADVANCE_RECEIPT")

    sub["合同负债_最新期_元"] = cl0
    sub["合同负债_上期_元"] = cl1

    # ── Step 3：营业收入用于归一化 ─────────────────────────────────────────
    inc0 = inc.iloc[0]
    inc1 = inc.iloc[1] if len(inc) > 1 else None
    rev0 = _get_field(inc0, "TOTAL_OPERATE_INCOME", "OPERATE_INCOME", "REVENUE")
    rev1 = _get_field(inc1, "TOTAL_OPERATE_INCOME", "OPERATE_INCOME", "REVENUE") if inc1 is not None else None

    # ── Step 4：计算同比增速 ─────────────────────────────────────────────────
    gs_yoy: float | None = _yoy_growth_pct(
        goods_sent_0 or 0.0, goods_sent_1 or 0.0
    ) if goods_sent_0 is not None and goods_sent_1 is not None else None

    cl_yoy: float | None = _yoy_growth_pct(
        cl0 or 0.0, cl1 or 0.0
    ) if cl0 is not None and cl1 is not None else None

    # 合同资产/营收比（绝对积压程度）
    gs_rev_ratio: float | None = None
    if goods_sent_0 and rev0 and rev0 > 0:
        gs_rev_ratio = goods_sent_0 / rev0

    sub[f"{proxy_label}_同比增速_pct"] = gs_yoy
    sub["合同负债_同比增速_pct"] = cl_yoy
    sub[f"{proxy_label}_占营收比"] = gs_rev_ratio

    # ── Step 5：勾稽判断核心逻辑 ─────────────────────────────────────────────
    #
    # 关键判断矩阵：
    #
    #              │  合同负债 ↑      │  合同负债 ↓/平
    # ─────────────┼──────────────────┼────────────────────
    # 发出商品 ↑↑  │ YELLOW（正常扩张）│ RED（积压+客户撤退）
    # 发出商品 ↑   │ GREEN             │ YELLOW
    # 发出商品 ↓/平│ GREEN（健康回款）  │ GREEN
    # ─────────────────────────────────────────────────────
    #
    # 附加条件：合同资产/营收比 > 40% 无论如何都提升一档预警

    notes: list[str] = []
    score = 25.0

    # 数据缺失处理
    if gs_yoy is None and cl_yoy is None:
        return DimResult(
            name="发出商品/合同负债勾稽",
            score=12.0,
            risk="YELLOW",
            detail=f"关键字段（{proxy_label}、合同负债）均无法获取，无法评估",
            sub_metrics=sub,
        )

    # 判断方向
    gs_rising_fast = gs_yoy is not None and gs_yoy > GOODS_SHIPPED_GROWTH_WARN
    gs_rising_moderate = gs_yoy is not None and 0 < gs_yoy <= GOODS_SHIPPED_GROWTH_WARN
    cl_declining = cl_yoy is not None and cl_yoy < -5.0    # 下降超 5% 视为有意义下降
    cl_stable_or_rising = cl_yoy is None or cl_yoy >= -5.0

    gs_rev_high = gs_rev_ratio is not None and gs_rev_ratio > 0.40

    if gs_rising_fast and cl_declining:
        # [*] 最高危信号 [*]
        score = 0.0
        notes.append(
            f"【RED [!]积压+客户撤退[!]】{proxy_label} 同比 +{gs_yoy:.1f}%（超过"
            f"{GOODS_SHIPPED_GROWTH_WARN:.0f}%预警线），同时合同负债同比 {cl_yoy:.1f}%"
            f"（下降）。产品可能堆积在客户仓库且客户预付意愿下降，存在重大减值风险。"
        )
    elif gs_rising_fast and cl_stable_or_rising:
        score = 13.0
        notes.append(
            f"【YELLOW 扩张中的积压风险】{proxy_label} 同比 +{gs_yoy:.1f}%，"
            f"合同负债尚稳定（{cl_yoy:.1f}%），整体为正常业务扩张，"
            f"但需跟踪验收进度与客户付款意愿。"
        )
    elif gs_rising_moderate and cl_declining:
        score = 13.0
        notes.append(
            f"【YELLOW 负债收缩】{proxy_label} 温和增长 +{gs_yoy:.1f}%，"
            f"但合同负债下降 {cl_yoy:.1f}%，客户预付意愿减弱，需关注后续回款质量。"
        )
    else:
        score = 25.0
        gs_desc = f"+{gs_yoy:.1f}%" if gs_yoy is not None else "数据缺失"
        cl_desc = f"{cl_yoy:+.1f}%" if cl_yoy is not None else "数据缺失"
        notes.append(
            f"【GREEN 勾稽健康】{proxy_label} 同比 {gs_desc}，"
            f"合同负债 {cl_desc}，无存货积压/客户撤退信号。"
        )

    # 附加：合同资产/营收比过高时提升警示
    if gs_rev_high:
        extra = (
            f"[!] {proxy_label}占营收比 {gs_rev_ratio:.1%}（>40%），"
            f"绝对积压规模偏大，即使暂无变化趋势也需持续关注。"
        )
        notes.append(extra)
        score = max(0.0, score - 5.0)  # 额外扣分

    return DimResult(
        name="发出商品/合同负债勾稽",
        score=min(score, 25.0),
        risk=_risk_from_score(score, 25.0),
        raw_value=gs_yoy,
        detail=" | ".join(notes),
        sub_metrics=sub,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  维度 3：研发支出资本化水分
# ═══════════════════════════════════════════════════════════════════════════

def _dim_rd_capitalization(
    bs_df: pd.DataFrame,
    income_df: pd.DataFrame,
) -> DimResult:
    """
    检测通过研发费用资本化虚增利润的风险。

    指标：(开发支出本期增加额) / 净利润
    阈值：> 20% 视为水分过大（电力设备/数字化行业基准）。

    资本化金额 = 资产负债表「开发支出」期末 - 期初
    （注：转为无形资产的部分已从「开发支出」中剔除，该差值为净资本化投入）
    净利润来自利润表。
    """
    bs = _sort_by_report_date(bs_df)
    inc = _sort_by_report_date(income_df)

    sub: dict = {}

    row0 = bs.iloc[0] if len(bs) >= 1 else None
    row1 = bs.iloc[1] if len(bs) >= 2 else None
    inc0 = inc.iloc[0] if len(inc) >= 1 else None

    if row0 is None or inc0 is None:
        return DimResult(
            name="研发资本化水分",
            score=12.0,
            risk="YELLOW",
            detail="财务数据不足，无法评估",
        )

    # 开发支出（期末 vs 期初）
    dev_cur = _get_field(row0, "DEV_EXPENDITURE", "DEVELOPMENT_EXPENDITURE")
    dev_prior = _get_field(row1, "DEV_EXPENDITURE", "DEVELOPMENT_EXPENDITURE") if row1 is not None else None

    # 本期资本化增量（若无上期数据，用期末值作为保守估计）
    if dev_cur is not None and dev_prior is not None:
        cap_rd_delta = dev_cur - dev_prior
    elif dev_cur is not None:
        cap_rd_delta = dev_cur  # 保守：视整个余额为本期投入
    else:
        cap_rd_delta = None

    # 净利润（归属母公司）
    net_profit = _get_field(
        inc0,
        "PARENT_NETPROFIT", "PARENTNETPROFIT",
        "NET_PROFIT", "NETPROFIT",
    )

    sub["开发支出_期末_元"] = dev_cur
    sub["开发支出_期初_元"] = dev_prior
    sub["本期资本化增量_元"] = cap_rd_delta
    sub["净利润_元"] = net_profit

    if cap_rd_delta is None or net_profit is None:
        return DimResult(
            name="研发资本化水分",
            score=15.0,
            risk="YELLOW",
            detail="开发支出或净利润字段缺失，无法计算资本化率",
            sub_metrics=sub,
        )

    # 若净利润为负，任何资本化都是水分
    if net_profit <= 0:
        if cap_rd_delta > 0:
            sub["资本化率"] = float("inf")
            return DimResult(
                name="研发资本化水分",
                score=0.0,
                risk="RED",
                raw_value=float("inf"),
                detail=(
                    f"净利润为负（{net_profit:.0f}元）但开发支出仍增加"
                    f"（+{cap_rd_delta:.0f}元），通过资本化掩盖亏损迹象极为明显。"
                ),
                sub_metrics=sub,
            )
        else:
            sub["资本化率"] = 0.0
            return DimResult(
                name="研发资本化水分",
                score=20.0,
                risk="GREEN",
                raw_value=0.0,
                detail="净利润为负但未新增开发支出资本化，无利润操纵迹象。",
                sub_metrics=sub,
            )

    cap_ratio = cap_rd_delta / net_profit
    sub["资本化率（增量/净利润）"] = cap_ratio

    notes: list[str] = []
    if cap_ratio <= 0:
        score = 25.0
        notes.append(
            f"【GREEN】本期无新增资本化研发（开发支出 {cap_rd_delta:.0f}元），"
            f"研发支出全部费用化，利润质量高。"
        )
    elif cap_ratio < RD_CAP_RATIO_WARN:
        score = 18.0
        notes.append(
            f"【GREEN 轻度资本化】本期资本化/净利润 = {cap_ratio:.1%}（"
            f"<{RD_CAP_RATIO_WARN:.0%}），处于合理范围。"
        )
    elif cap_ratio < 0.40:
        score = 8.0
        notes.append(
            f"【YELLOW】资本化/净利润 = {cap_ratio:.1%}（>{RD_CAP_RATIO_WARN:.0%}），"
            f"需核查开发项目真实性，是否利用研发资本化平滑利润。"
        )
    else:
        score = 0.0
        notes.append(
            f"【RED [*]高度水分[*]】资本化/净利润 = {cap_ratio:.1%}（>40%），"
            f"研发支出资本化规模已接近或超过净利润，严重怀疑利润操纵。"
        )

    return DimResult(
        name="研发资本化水分",
        score=score,
        risk=_risk_from_score(score, 25.0),
        raw_value=cap_ratio,
        detail=" | ".join(notes),
        sub_metrics=sub,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  维度 4：上下游博弈能力
# ═══════════════════════════════════════════════════════════════════════════

def _dim_bargaining_power(bs_df: pd.DataFrame) -> DimResult:
    """
    检测公司在供应链中的话语权（上下游资金占用比）。

    指标：(应付账款 + 应付票据) / (应收账款 + 应收票据)

    如果 < 0.5，说明公司占用供应商资金不足，反而被下游大客户（如海博思创）严重占用，
    自身造血能力受损；
    如果 > 1.0，说明公司在链条中处于强势地位，可延压供应商、缩短回款周期。
    """
    bs = _sort_by_report_date(bs_df)
    row0 = bs.iloc[0] if not bs.empty else None

    if row0 is None:
        return DimResult(
            name="上下游博弈能力",
            score=12.0,
            risk="YELLOW",
            detail="资产负债表数据缺失",
        )

    ap  = _get_field(row0, "ACCOUNTS_PAYABLE") or 0.0
    np_ = _get_field(row0, "NOTES_PAYABLE", "BILL_PAYABLE") or 0.0
    ar  = _get_field(row0, "ACCOUNTS_RECE", "NOTE_ACCOUNTS_RECE") or 0.0
    nr  = _get_field(row0, "NOTES_RECE", "BILL_RECEIVABLE") or 0.0

    payable_total  = ap + np_
    receivable_total = ar + nr

    sub = {
        "应付账款_元": ap,
        "应付票据_元": np_,
        "应收账款_元": ar,
        "应收票据_元": nr,
        "应付合计_元": payable_total,
        "应收合计_元": receivable_total,
    }

    if receivable_total < 1e-6:
        sub["博弈指数_AP_AR"] = None
        return DimResult(
            name="上下游博弈能力",
            score=20.0,
            risk="GREEN",
            raw_value=None,
            detail="应收账款极小（接近零），公司几乎无客户赊销敞口，占款问题不显著。",
            sub_metrics=sub,
        )

    ratio = payable_total / receivable_total
    sub["博弈指数_AP_AR"] = ratio

    notes: list[str] = []
    if ratio >= 1.0:
        score = 25.0
        notes.append(
            f"【GREEN 强势地位】AP/AR = {ratio:.2f}（≥1.0），公司占用供应商资金 ≥"
            f" 被客户占用资金，供应链地位强。"
        )
    elif ratio >= BARGAIN_POWER_WARN:
        score = 15.0
        notes.append(
            f"【YELLOW 中性】AP/AR = {ratio:.2f}（0.5-1.0），链条地位居中，"
            f"被下游占用资金略多于对上游的占用。"
        )
    else:
        score = 0.0
        notes.append(
            f"【RED [*]弱势链条[*]】AP/AR = {ratio:.2f}（<{BARGAIN_POWER_WARN}），"
            f"公司严重依赖海博思创等大客户，被大量占用资金，"
            f"自身造血能力存疑，短期偿债能力需关注。"
        )

    return DimResult(
        name="上下游博弈能力",
        score=score,
        risk=_risk_from_score(score, 25.0),
        raw_value=ratio,
        detail=" | ".join(notes),
        sub_metrics=sub,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  主入口：compute_power_score
# ═══════════════════════════════════════════════════════════════════════════

def compute_power_score(
    code: str,
    name: str = "",
    *,
    manual_obs: dict | None = None,
) -> PowerIndustryScore:
    """
    对单只股票执行全部四维电力行业排雷评分。

    :param code:        股票代码（6 位，如 '688558'）
    :param name:        股票名称（用于日志与报告）
    :param manual_obs:  内部销售观察字典，影响最终加权评分。
                        支持的键：
                          'ka_project_slowdown'   : bool   大客户项目进度放缓
                          'ka_is_hyperstrong'      : bool   确认海博思创为大客户
                          'channel_inventory_high' : bool   内部知悉渠道库存高企
                          'obs_weight_override'    : float  直接覆盖权重因子（0.5-1.5）
    :returns: PowerIndustryScore 对象，含四维得分与汇总信息
    """
    code = str(code).zfill(6)
    label = f"{code}({name or '?'})"
    logger.info("[%s] 开始电力行业专属评分……", label)

    sec  = to_em_sec_code(code)
    h10  = to_em_h10_code(code)

    result = PowerIndustryScore(code=code, name=name)

    # ── 拉取原始数据 ──────────────────────────────────────────────────────
    try:
        bs_df = _fetch_balance_sheet(h10)
    except Exception:
        logger.exception("[%s] 资产负债表拉取失败，电力行业评分中止", label)
        result.summary = "资产负债表获取失败，无法评分"
        return result

    try:
        inc_df = _fetch_income_sheet(h10)
    except Exception:
        logger.exception("[%s] 利润表拉取失败，使用空表继续", label)
        inc_df = pd.DataFrame()

    # ── 四维评分 ──────────────────────────────────────────────────────────
    if not inc_df.empty:
        result.dim_ka            = _dim_ka_dependency(bs_df, inc_df)
        result.dim_goods_shipped = _dim_goods_shipped_vs_contract_liab(bs_df, inc_df)
        result.dim_rd_cap        = _dim_rd_capitalization(bs_df, inc_df)
    else:
        result.dim_ka            = DimResult("KA依赖与账期错配",    10.0, "YELLOW", detail="利润表缺失")
        result.dim_goods_shipped = DimResult("发出商品/合同负债勾稽", 10.0, "YELLOW", detail="利润表缺失")
        result.dim_rd_cap        = DimResult("研发资本化水分",       10.0, "YELLOW", detail="利润表缺失")

    result.dim_bargain = _dim_bargaining_power(bs_df)

    total = (
        result.dim_ka.score
        + result.dim_goods_shipped.score
        + result.dim_rd_cap.score
        + result.dim_bargain.score
    )
    result.total_score = round(total, 2)

    # ── 海博思创关联检测 ──────────────────────────────────────────────────
    if manual_obs and manual_obs.get("ka_is_hyperstrong"):
        result.has_hyperstrong_flag = True
        logger.info("[%s] 手动标记：海博思创为大客户", label)
    else:
        raw_text = _fetch_top_customers(sec)
        if raw_text:
            for kw in HYPERSTRONG_KEYWORDS:
                if kw in raw_text:
                    result.has_hyperstrong_flag = True
                    logger.info("[%s] 公告中检测到海博思创关键词：%s", label, kw)
                    break

    # ── 手动销售观察权重因子 ──────────────────────────────────────────────
    obs = manual_obs or {}
    if "obs_weight_override" in obs:
        weight = float(obs["obs_weight_override"])
    else:
        weight = 1.0
        if obs.get("ka_project_slowdown"):
            weight -= 0.20
            logger.info("[%s] 内部观察：大客户项目进度放缓 → 权重 -0.20", label)
        if obs.get("channel_inventory_high"):
            weight -= 0.15
            logger.info("[%s] 内部观察：渠道库存高企 → 权重 -0.15", label)
        if result.has_hyperstrong_flag and result.dim_ka.risk == "RED":
            weight -= 0.15
            logger.info("[%s] 海博思创大客户 + AR 高风险 → 额外权重 -0.15", label)
        weight = max(0.40, min(1.50, weight))

    result.manual_obs_weight = round(weight, 3)
    result.final_score = round(min(result.total_score * weight, 100.0), 2)

    # ── 汇总说明 ──────────────────────────────────────────────────────────
    red_dims = [
        d.name for d in [
            result.dim_ka, result.dim_goods_shipped,
            result.dim_rd_cap, result.dim_bargain,
        ]
        if d is not None and d.risk == "RED"
    ]
    result.summary = (
        f"总分 {result.total_score}/100 × 权重{weight:.2f} = 最终得分 {result.final_score}"
        + (f"；红色预警维度：{'、'.join(red_dims)}" if red_dims else "；无红色预警")
        + ("；[!] 检测到海博思创大客户关联" if result.has_hyperstrong_flag else "")
    )

    logger.info("[%s] 电力行业评分完成：%s", label, result.summary)
    return result


# ═══════════════════════════════════════════════════════════════════════════
#  批量导出为 DataFrame
# ═══════════════════════════════════════════════════════════════════════════

def scores_to_dataframe(scores: list[PowerIndustryScore]) -> pd.DataFrame:
    """将多只股票的评分结果展平为 DataFrame，便于写入 Excel。"""
    rows = []
    for s in scores:
        row: dict = {
            "代码":            s.code,
            "名称":            s.name,
            "电力行业专属评分": s.final_score,
            "基础总分":        s.total_score,
            "观察权重因子":    s.manual_obs_weight,
            "海博思创关联":    "是" if s.has_hyperstrong_flag else "否",
            "综合说明":        s.summary,
        }
        for dim in [s.dim_ka, s.dim_goods_shipped, s.dim_rd_cap, s.dim_bargain]:
            if dim is None:
                continue
        # 维度详情
        if s.dim_ka:
            row["KA账期_得分"] = s.dim_ka.score
            row["KA账期_风险"] = s.dim_ka.risk
            row["应收周转天数"] = s.dim_ka.sub_metrics.get("应收周转天数")
            row["应收_营收增速比"] = s.dim_ka.sub_metrics.get("应收_营收增速比")
            row["KA账期_说明"] = s.dim_ka.detail

        if s.dim_goods_shipped:
            row["发出商品_得分"]    = s.dim_goods_shipped.score
            row["发出商品_风险"]    = s.dim_goods_shipped.risk
            row["指标类型"]         = s.dim_goods_shipped.sub_metrics.get("指标类型")
            row["合同资产同比增速"] = s.dim_goods_shipped.sub_metrics.get(
                "合同资产（发出商品代理）_同比增速_pct",
                s.dim_goods_shipped.sub_metrics.get("发出商品（直接字段）_同比增速_pct"),
            )
            row["合同负债同比增速"] = s.dim_goods_shipped.sub_metrics.get("合同负债_同比增速_pct")
            row["发出商品_说明"]    = s.dim_goods_shipped.detail

        if s.dim_rd_cap:
            row["研发资本化_得分"]      = s.dim_rd_cap.score
            row["研发资本化_风险"]      = s.dim_rd_cap.risk
            row["资本化率_增量净利润比"] = s.dim_rd_cap.sub_metrics.get("资本化率（增量/净利润）")
            row["研发资本化_说明"]      = s.dim_rd_cap.detail

        if s.dim_bargain:
            row["博弈能力_得分"]  = s.dim_bargain.score
            row["博弈能力_风险"]  = s.dim_bargain.risk
            row["博弈指数_AP_AR"] = s.dim_bargain.sub_metrics.get("博弈指数_AP_AR")
            row["博弈能力_说明"]  = s.dim_bargain.detail

        rows.append(row)
    return pd.DataFrame(rows)
