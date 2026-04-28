def _load_spot_universe() -> pd.DataFrame:
    """
    改为拉重新浪 A 股实时行情，并映射字段名以适配后续逻辑。
    """

    def _fetch() -> pd.DataFrame:
        # 使用新浪接口作为替代
        df_sina = ak.stock_zh_a_spot_sina()
        
        # 核心：将新浪字段名映射为原代码需要的东财字段名
        # 新浪返回列参考：symbol, code, name, trade, pricechange, changepercent, buy, sell, settlement, open, high, low, volume, amount, ticktime, per, pb, mktcap, nmc
        column_map = {
            "code": "代码",
            "name": "名称",
            "trade": "最新价",
            "per": "市盈率-动态",  # 新浪的 per 对应动态市盈率
            "amount": "成交额",
            "volume": "成交量",
            "mktcap": "总市值",
            "nmc": "流通市值"
        }
        
        # 重命名列
        df_mapped = df_sina.rename(columns=column_map)
        
        # 确保代码是 6 位字符串（新浪 code 列通常已经是，但保险起见做一下处理）
        df_mapped["代码"] = df_mapped["代码"].str.replace("sh", "").str.replace("sz", "")
        
        return df_mapped

    # 注意：这里调用名改成了 sina，方便日志区分
    df = call_with_retry("stock_zh_a_spot_sina", _fetch, validate=df_nonempty)
    return df
