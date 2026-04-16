"""
A 股「深度价值 + 宽护城河」漏斗式选股框架。

对外主入口建议使用同目录下的 ``run_deep_value_funnel.py``，
或 ``python -m deep_value_funnel.pipeline``。
"""

from .pipeline import run_screening

__all__ = ["run_screening"]
