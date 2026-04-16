"""
全局阈值与网络节奏配置。

说明：
- ``PE_MAX`` 对应东财行情里的「市盈率-动态」，与财报口径的 PE(TTM) 接近，
  用于大规模初筛；若需严格 TTM 口径，可在深度阶段改用估值序列接口二次校验。
"""

from __future__ import annotations

# --- 选股硬条件（与用户策略一致，可在此调参） ---
PE_MAX: float = 10.0
GROSS_MARGIN_MIN: float = 45.0  # 销售毛利率下限（%）
DRAWDOWN_MIN: float = 0.50  # 相对近 250 日最高价的最大回撤下限（50% 表示现价不高于高点的 50%）
DIV_YIELD_MIN: float = 0.05  # 股息率下限：东财分红详情里为小数（0.05 = 5%）
PAYOUT_RATIO_MIN: float = 50.0  # 近三年平均分红率（净利润口径）下限（%）
LISTING_MIN_YEARS: int = 5  # 上市满多少年才纳入（自然年近似）
HIST_WINDOW: int = 250  # 计算回撤的行情窗口（交易日）

# --- 防封禁 / 节流 ---
REQUEST_BASE_SLEEP: float = 0.35  # 每次远程请求后的基础休眠（秒）
REQUEST_JITTER: float = 0.25  # 额外随机抖动上限（秒），避免固定节拍被识别
MAX_RETRIES: int = 6  # 单接口默认最大重试次数（不含首次请求）
RETRY_BACKOFF_BASE: float = 1.8  # 指数退避基数（秒）
# 东财日 K 易出现 RemoteDisconnected / Connection aborted，对连接类错误额外加长的等待（秒）
CONNECTION_RETRY_EXTRA_BASE: float = 2.5
CONNECTION_RETRY_EXTRA_STEP: float = 2.0
# 日 K：多数据源顺序。默认「腾讯优先、东财兜底」，避免东财 push2his 长时间断连时全挂。
HIST_PROVIDER_ORDER: tuple[str, ...] = ("tencent", "eastmoney")
# 单个数据源内部的 call_with_retry 次数（不含首次请求）
HIST_PROVIDER_RETRIES: int = 4
# 主源失败切换到下一数据源前的停顿（秒）
HIST_FALLBACK_PAUSE: float = 1.2
# 东财单独拉 K 时（若仍直接调用）可保留；行情阶段已改走 hist_fetch 多源
HIST_MAX_RETRIES: int = 8
HIST_INTER_STOCK_SLEEP: float = 0.85  # 每处理完一只后的间隔（秒），最后一只不睡

# --- 运行控制（便于本机调试） ---
# PE 初筛之后、进入「财务漏斗」之前的最大数量（限制指标/现金流量表请求次数）
MAX_DEEP_CANDIDATES: int | None = None
# 财务通过之后、进入「日 K / 回撤」之前的最大数量（限制最易风控的 K 线请求次数）
MAX_HIST_CANDIDATES: int | None = None

# ---------------------------------------------------------------------------
# Tushare 副源配置
# ---------------------------------------------------------------------------
# 将此处替换为你的 Tushare Pro token（https://tushare.pro/user/token）
# 可以用环境变量覆盖：export TUSHARE_TOKEN=xxxxx
import os as _os  # noqa: E402  pylint: disable=wrong-import-position,wrong-import-order
TUSHARE_TOKEN: str = _os.environ.get("TUSHARE_TOKEN", "fa7feec12dec40e9f67ba6247f10d6dbb11a3f0a7a4eb0ea7ad70100")

# True = 同时运行 Tushare 副源并与 AKShare 结果对照；False = 跳过 Tushare（无 token 时自动降为 False）
ENABLE_TUSHARE_COMPARE: bool = True
