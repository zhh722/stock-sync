import argparse
from datetime import datetime, timedelta

import baostock as bs
import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(description="获取所有科创板股票代码")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="查询日期，格式 YYYY-MM-DD；默认从今天开始向前寻找最近交易日",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=30,
        help="向前回溯的最大天数，默认 30",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="kechuang_code.csv",
        help="仅股票代码输出文件，默认 kechuang_code.csv",
    )
    parser.add_argument(
        "--full-output",
        type=str,
        default="kechuang_full_code.csv",
        help="完整信息输出文件，默认 kechuang_full_code.csv",
    )
    return parser.parse_args()


def validate_date(date_str):
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def candidate_dates(start_date, lookback_days):
    for offset in range(lookback_days + 1):
        yield start_date - timedelta(days=offset)


def query_all_stocks(target_date):
    rs = bs.query_all_stock(target_date)
    if rs.error_code != "0":
        print(f"{target_date} 获取股票列表失败: {rs.error_msg}")
        return pd.DataFrame()
    return rs.get_data()


def main():
    args = parse_args()

    if args.date and not validate_date(args.date):
        raise SystemExit("日期格式错误，请使用 YYYY-MM-DD")

    start_date = (
        datetime.strptime(args.date, "%Y-%m-%d").date()
        if args.date
        else datetime.today().date()
    )

    lg = bs.login()
    if lg.error_code != "0":
        raise SystemExit(f"登录失败: {lg.error_msg}")

    try:
        all_stocks = pd.DataFrame()
        used_date = None

        for curr_date in candidate_dates(start_date, args.lookback_days):
            target_date = curr_date.strftime("%Y-%m-%d")
            print(f"正在获取 {target_date} 的全市场股票列表...")
            all_stocks = query_all_stocks(target_date)
            if not all_stocks.empty:
                used_date = target_date
                break

        if used_date is None:
            raise SystemExit(
                f"最近 {args.lookback_days} 天内未获取到股票列表，请指定交易日后重试。"
            )

        kechuang_df = all_stocks[
            all_stocks["code"].str.startswith("sh.688", na=False)
        ].copy()

        if kechuang_df.empty:
            raise SystemExit(f"{used_date} 未找到科创板股票，请检查查询日期。")

        kechuang_df = kechuang_df.sort_values(by="code").reset_index(drop=True)
        kechuang_df["plain_code"] = kechuang_df["code"].str.replace(
            "sh.", "", regex=False
        )

        full_df = kechuang_df[["plain_code", "code", "code_name"]].rename(
            columns={
                "plain_code": "code",
                "code": "baostock_code",
                "code_name": "name",
            }
        )
        full_df["query_date"] = used_date

        full_df[["code"]].to_csv(args.output, index=False, encoding="utf-8-sig")
        full_df.to_csv(args.full_output, index=False, encoding="utf-8-sig")

        print(f"已找到 {len(full_df)} 只科创板股票")
        print(f"实际使用的查询日期: {used_date}")
        print(f"股票代码已保存到: {args.output}")
        print(f"完整信息已保存到: {args.full_output}")
    finally:
        bs.logout()


if __name__ == "__main__":
    main()
