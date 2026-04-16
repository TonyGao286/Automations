"""股票代码与市场前缀工具。"""

from __future__ import annotations


def is_star_or_main_board_a(code: str) -> bool:
    """
    判断是否属于沪深主板 / 创业板 / 科创板等常见 A 股代码段。

    北交所股票代码通常以 43、83、87、92 等开头（6 位数字），此处予以剔除。
    """
    c = str(code).zfill(6)
    if len(c) != 6 or not c.isdigit():
        return False
    if c.startswith(("43", "83", "87", "92")):
        return False
    return True


def to_em_sec_code(code: str) -> str:
    """
    转为东方财富 ``SECUCODE`` 形式，如 ``600519.SH``、``000001.SZ``。
    """
    c = str(code).zfill(6)
    if c.startswith("6"):
        return f"{c}.SH"
    return f"{c}.SZ"


def to_em_h10_code(code: str) -> str:
    """
    转为东方财富 H10 财报接口常用的 ``SH600519`` / ``SZ000001`` 形式。
    """
    c = str(code).zfill(6)
    if c.startswith("6"):
        return f"SH{c}"
    return f"SZ{c}"


def is_st_name(name: str) -> bool:
    """名称层面识别 ST/*ST。"""
    n = str(name).upper()
    return "ST" in n
