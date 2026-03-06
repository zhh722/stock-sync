import pandas as pd
import baostock as bs


def fetch_baostock_data(code, start, end, freq="daily"):
    """从 Baostock 获取股票数据（支持日线/周线，前复权）"""

    code_bs = f"sh.{code}" if code.startswith(('6', '9')) else f"sz.{code}"
    frequency = "d" if freq == "daily" else "w"

    # 根据频率选择字段（周线不支持 preclose 等）
    if freq == "daily":
        fields = (
            "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,"
            "tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"
        )
    else:  # weekly or monthly
        fields = "date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg"
    bs.login()
    rs = bs.query_history_k_data_plus(
        code_bs,
        fields,
        start_date=start,
        end_date=end,
        frequency=frequency,
        adjustflag="2"  # 1：后复权；2：前复权； 3: 不复权。
    )

    if rs.error_code != '0':
        print(f"Baostock query failed for {code}: {rs.error_msg}")
        return pd.DataFrame()

    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    bs.logout()

    if not data_list:
        return pd.DataFrame()

    df = pd.DataFrame(data_list, columns=rs.fields)

    # 类型转换
    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
    if freq == "daily":
        numeric_cols.extend(['preclose', 'turn', 'pctChg', 'peTTM', 'pbMRQ', 'psTTM', 'pcfNcfTTM'])

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 清理 code 字段（去掉 sh./sz.）
    df['code'] = df['code'].str.replace('sh.', '', regex=False).str.replace('sz.', '', regex=False)
    df['date'] = pd.to_datetime(df['date'])
    return df


if __name__ == "__main__":
    code = "688318"
    fetch_baostock_data(code, "2025-08-01", "2026-03-05").to_csv(f"{code}_daily.csv", index=False)