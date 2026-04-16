#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import os
import sys
from datetime import date
from pathlib import Path

# 强制清空当前 Python 进程的代理环境变量，实现国内接口直连
os.environ["http_proxy"] = ""
os.environ["https_proxy"] = ""
os.environ["HTTP_PROXY"] = ""
os.environ["HTTPS_PROXY"] = ""
os.environ["all_proxy"] = ""
os.environ["ALL_PROXY"] = ""

# 入口脚本：在项目根目录执行
#   python run_deep_value_funnel.py
# 或带参数（--max-deep 限制进财务漏斗数量；--max-hist 限制进日 K/回撤数量）：
#   python run_deep_value_funnel.py --max-deep 50 --max-hist 20 -o my_pool.csv

from deep_value_funnel.pipeline import main


def _resolve_deep_value_csv_path(argv: list[str]) -> Path:
    """与 ``deep_value_funnel.pipeline.main`` 中 ``-o/--output`` 默认规则一致。"""
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("-o", "--output", type=str, default=None)
    args, _ = p.parse_known_args(argv)
    if args.output:
        return Path(args.output).resolve()
    today = date.today().isoformat().replace("-", "")
    return Path(f"deep_value_pool_{today}.csv").resolve()


if __name__ == "__main__":
    argv = sys.argv[1:]
    out_csv = _resolve_deep_value_csv_path(argv)
    rc = main(argv)
    if rc == 0:
        from notifier import notify_deep_value_pool

        notify_deep_value_pool(out_csv)
    raise SystemExit(rc)
