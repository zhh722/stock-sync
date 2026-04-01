import baostock as bs
import pandas as pd
import time
import re

# 登录
lg = bs.login()
if lg.error_code != '0':
    print(f"登录失败: {lg.error_msg}")
    exit()

target_date = "2026-02-27"
fields = "date,code,amount,turn,tradestatus,isST"

print(f"正在获取 {target_date} 的全市场股票列表...")
stock_rs = bs.query_all_stock(target_date)
if stock_rs.error_code != '0':
    print(f"获取股票列表失败: {stock_rs.error_msg}")
    bs.logout()
    exit()

all_stocks = stock_rs.get_data()

# --- 核心修改开始 ---
# 方法1: 使用更严谨的正则 (涵盖 300, 301 开头的创业板 和 688 开头的科创板)
# 解释:
# ^sz\.30\d{4}$  -> 匹配 sz.300000 到 sz.301999
# ^sh\.688\d{3}$ -> 匹配 sh.688000 到 sh.688999
pattern = re.compile(r'^sz\.30\d{4}$|^sh\.688\d{3}$')

# 应用过滤
dual_board_df = all_stocks[all_stocks['code'].apply(lambda x: bool(pattern.match(x)))]

# 方法2 (备选，更快): 直接使用字符串前缀判断，无需正则
# is_target = all_stocks['code'].str.startswith('sz.30') | all_stocks['code'].str.startswith('sh.688')
# dual_board_df = all_stocks[is_target]
# --- 核心修改结束 ---

count_total = len(dual_board_df)
print(f"筛选完成。目标双板总数: {count_total} (包含300/301创业板及688科创板)")

if count_total == 0:
    print("未找到符合条件的股票，请检查日期是否为交易日。")
    bs.logout()
    exit()

filtered_results = []
failed_codes = []

print(f"开始执行详细数据筛选...")

for index, row in dual_board_df.iterrows():
    curr_code = row['code']
    curr_name = row['code_name']

    # 获取历史K线数据
    rs = bs.query_history_k_data_plus(
        curr_code,
        fields,
        start_date=target_date,
        end_date=target_date,
        frequency="d",
        adjustflag="2"  # 前复权
    )

    if rs.error_code != '0':
        failed_codes.append({'代码': curr_code, '名称': curr_name, '原因': f'接口报错:{rs.error_msg}'})
        continue

    has_data = False
    while rs.next():
        has_data = True
        res = rs.get_row_data()
        # 字段顺序对应 fields: date, code, amount, turn, tradestatus, isST
        # res[2] = amount (成交额，单位：元)
        # res[3] = turn (换手率，单位：%)

        amount_str = res[2]
        turn_str = res[3]
        trade_status = res[4]

        # 额外判断：如果交易状态不是空或者特定标记，可能停牌 (Baostock有时停牌日也有记录但量为0)
        if not amount_str or not turn_str:
            failed_codes.append({'代码': curr_code, '名称': curr_name, '原因': '数据缺失'})
            continue

        try:
            amount = float(amount_str)
            turn_rate = float(turn_str)

            # 过滤停牌：换手率为0或成交额为0
            if turn_rate == 0 or amount == 0:
                failed_codes.append({'代码': curr_code, '名称': curr_name, '原因': '停牌或无交易(换手0)'})
                continue

            # 计算流通市值: 成交额 / (换手率/100)
            # 注意：Baostock的turn是百分比数值(如2.5表示2.5%)
            mcap_float = amount / (turn_rate / 100.0)

            # 筛选条件: 30亿 <= 流通市值 <= 300亿
            # 3e9  = 3,000,000,000 (30亿)
            # 3e10 = 60,000,000,000 (600亿)
            if 3e9 <= mcap_float <= 6e10:
                filtered_results.append({
                    '代码': curr_code,
                    '名称': curr_name,
                    '流通市值(亿)': round(mcap_float / 1e8, 2),
                    '成交额(万)': round(amount / 10000, 2),
                    '换手率(%)': round(turn_rate, 2),
                    '日期': target_date
                })
        except Exception as e:
            failed_codes.append({'代码': curr_code, '名称': curr_name, '原因': f'计算异常:{str(e)}'})

    if not has_data:
        failed_codes.append({'代码': curr_code, '名称': curr_name, '原因': '未返回行数据(可能非交易日)'})

    # 降频以防被封IP，0.2秒通常足够，原代码0.5秒较保守

    if (len(failed_codes) + len(filtered_results)) % 100 == 0:
        print(f"已处理 {len(failed_codes) + len(filtered_results)} / {count_total} 只股票...")
        time.sleep(30)
    else:
        time.sleep(0.2)

# 保存结果
if filtered_results:
    result_df = pd.DataFrame(filtered_results)
    # 按流通市值排序
    result_df = result_df.sort_values(by='流通市值(亿)', ascending=True)
    result_df.to_csv("calculated_float_mcap.csv", index=False, encoding="utf-8-sig")

    code_df = result_df[["代码"]].rename(columns={"代码":"code"}).copy()
    code_df['code'] = code_df['code'].str.replace(r'^sh\.|^sz\.', '', regex=True)
    code_df['priority'] = code_df['code'].str.startswith('688').map(lambda is_688: 0 if is_688 else 1)
    code_df = code_df.sort_values(by=['priority', 'code'], ascending=[True, True]).drop(columns=['priority'])
    code_df.to_csv("code.csv", index=False)

    print(f"\n成功筛选 {len(result_df)} 只股票，已保存至 calculated_float_mcap.csv")
else:
    print("\n未筛选到符合市值条件的股票。")

if failed_codes:
    failed_df = pd.DataFrame(failed_codes)
    failed_df.to_csv("failed_stocks_log.csv", index=False, encoding="utf-8-sig")
    print(f"异常/停牌记录 {len(failed_df)} 条，已保存至 failed_stocks_log.csv")

bs.logout()
print("程序执行完毕。")
