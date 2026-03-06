# sync_daily.py
import os
import sys
import time
import logging
import argparse
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import baostock as bs
from sync_to_mysql import fetch_baostock_data, upsert, get_latest

# ================== 配置 ==================
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "InsightOne123456")
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DB = os.getenv("MYSQL_DB", "stock_db_qfq")
CODE_CSV_PATH = os.getenv("CODE_CSV_PATH", "./code.csv")

# ================== 日志 ==================
log_dir = "./logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "sync_daily_baostock.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def parse_arguments():
    parser = argparse.ArgumentParser(description='同步股票日线数据（Baostock版）')
    parser.add_argument('--date', type=str, help='指定同步日期，格式：YYYY-MM-DD')
    parser.add_argument('--start-date', type=str, help='开始日期，格式：YYYY-MM-DD')
    parser.add_argument('--end-date', type=str, help='结束日期，格式：YYYY-MM-DD')
    return parser.parse_args()

def validate_date(date_str):
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def fetch_with_relogin(code, start_date, end_date, freq="daily", max_retries=3):
    """带自动重新登录的数据获取"""
    for attempt in range(max_retries):
        df = fetch_baostock_data(code, start_date, end_date, freq)
        # 检查是否因会话超时导致空数据（通过重新登录验证）
        if df.empty:
            # 尝试重新登录
            bs.logout()
            time.sleep(1)
            lg = bs.login()
            if lg.error_code != '0':
                logger.warning(f"重新登录失败: {lg.error_msg}，等待5s后重试")
                time.sleep(5)
                continue
            # 重新登录成功后再次获取数据
            df = fetch_baostock_data(code, start_date, end_date, freq)
        return df
    return pd.DataFrame()

def sync_single_date(engine, codes, target_date):
    logger.info(f"正在同步指定日期数据（{target_date}）")
    cnt = 1
    for code in codes:
        time.sleep(0.5)
        df = fetch_with_relogin(code, target_date, target_date, "daily")
        if not df.empty:
            upsert(df, "stock_daily", engine, "date")
            logger.info(f"✅ {code} 同步 {cnt}/{len(codes)} 条 {target_date} 数据")
        else:
            logger.info(f"ℹ️ {code} 在 {target_date} 无数据")
        cnt += 1

def sync_date_range(engine, codes, start_date, end_date):
    logger.info(f"正在同步日期范围数据（{start_date} 到 {end_date}）")
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    if start_dt > end_dt:
        logger.error("❌ 开始日期不能晚于结束日期")
        return
    current_dt = start_dt
    while current_dt <= end_dt:
        current_date_str = current_dt.strftime("%Y-%m-%d")
        sync_single_date(engine, codes, current_date_str)
        current_dt += timedelta(days=1)

def sync_latest(engine, codes):
    today = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"正在同步最新日线数据（到{today}为止）")
    cnt = 1
    for code in codes:
        time.sleep(0.2)
        latest_date = get_latest(engine, code, "stock_daily", "date")
        if latest_date:
            start_date = (datetime.strptime(latest_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            start_date = "2010-01-01"
        if start_date <= today:
            try:
                df = fetch_with_relogin(code, start_date, today, "daily")
            except Exception as e:
                logger.warning(f"⚠️ {code} 日线同步失败: {e}，等待30s重试")
                time.sleep(30)
                bs.logout()
                bs.login()
                df = fetch_baostock_data(code, start_date, today, "daily")
            if not df.empty:
                upsert(df, "stock_daily", engine, "date")
                logger.info(f"✅ {code} 同步 {cnt}/{len(codes)} 条日线数据")
            else:
                logger.info(f"ℹ️ {code} 无新数据")
        else:
            logger.info(f"ℹ️ {code} 数据已是最新")
        cnt += 1

def main():
    args = parse_arguments()
    uri = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
    engine = create_engine(uri, pool_pre_ping=True)

    lg = bs.login()
    if lg.error_code != '0':
        logger.error(f"Baostock login failed: {lg.error_msg}")
        return

    try:
        codes = []
        with open(CODE_CSV_PATH, "r") as f:
            for line in f:
                code = line.strip()
                if code and not code.startswith("#"):
                    codes.append(code.zfill(6))

        if args.date:
            if not validate_date(args.date):
                logger.error("❌ 日期格式错误，请使用 YYYY-MM-DD 格式")
                return
            sync_single_date(engine, codes, args.date)
        elif args.start_date and args.end_date:
            if not validate_date(args.start_date) or not validate_date(args.end_date):
                logger.error("❌ 日期格式错误，请使用 YYYY-MM-DD 格式")
                return
            sync_date_range(engine, codes, args.start_date, args.end_date)
        else:
            sync_latest(engine, codes)
        logger.info("✅ 日线数据同步完成")
    except Exception as e:
        logger.exception(f"同步失败: {e}")
    finally:
        bs.logout()
        logger.info("✅ 日线同步任务结束")

if __name__ == "__main__":
    '''
    同步单日数据: python sync_daily.py --date 2024-12-07
    同步日期范围: python sync_daily.py --start-date 2024-12-01 --end-date 2024-12-07
    自动同步最新: python sync_daily.py
    '''
    s_time = time.time()
    main()
    print(f"同步耗时 {time.time()-s_time}s")