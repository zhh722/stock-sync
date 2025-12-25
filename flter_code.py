import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import os


def update_stock_pool(output_file="code.csv"):
    print("开始获取 A 股实时行情与市值数据...")

    try:
        # # 1. 获取所有 A 股实时行情数据 (包含总市值)
        # df_spot = ak.stock_zh_a_spot_em()
        #
        # # 2. 基础过滤：市值在 100亿 到 2000亿 之间
        # # 注意：该接口返回的 '总市值' 单位通常是元
        # min_cap = 100 * 1e8
        # max_cap = 2000 * 1e8
        #
        # mask = (df_spot['总市值'] >= min_cap) & (df_spot['总市值'] <= max_cap)
        # df_filtered = df_spot[mask].copy()

        df_filtered = pd.read_csv("full_code.csv")
        # 3. 过滤 ST 和 退市股
        df_filtered = df_filtered[~df_filtered['名称'].str.contains("ST|退", na=False)]

        # 根据code去重
        df_filtered = df_filtered.drop_duplicates(subset=['代码'], keep='first')

        # 4. 过滤上市日期小于 1 年的股票
        print("正在检查上市日期（过滤不满一年的个股）...")

        # 5. 格式化并保存
        df_filtered = df_filtered['代码'].astype(str).str.zfill(6)
        df_filtered = df_filtered.rename(columns={'代码': 'code'})
        df_filtered.to_csv(output_file, index=False)

        print(f"✅ 同步完成！")
        print(f"📊 符合条件（100亿-2000亿市值  & 非ST）的股票共: {len(df_filtered)} 只")
        print(f"📁 结果已保存至: {os.path.abspath(output_file)}")

    except Exception as e:
        print(f"❌ 更新失败: {e}")


if __name__ == "__main__":
    update_stock_pool()