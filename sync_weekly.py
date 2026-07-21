# sync_weekly.py
import os
import sys
import time
import logging
import pandas as pd
import socket
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
SOCKET_TIMEOUT = 15

socket.setdefaulttimeout(SOCKET_TIMEOUT)

# ================== 日志 ==================
log_dir = "./logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "sync_weekly_baostock.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def login_with_retry(max_retries=3):
    for attempt in range(1, max_retries + 1):
        lg = bs.login()
        if lg.error_code == '0':
            return True
        logger.warning(f"Baostock 登录失败 {attempt}/{max_retries}: {lg.error_msg}")
        if attempt < max_retries:
            time.sleep(5)
    return False


def fetch_with_relogin(code, start_date, end_date, freq="weekly", max_retries=3):
    """带自动重新登录的数据获取"""
    for attempt in range(1, max_retries + 1):
        try:
            return fetch_baostock_data(code, start_date, end_date, freq)
        except Exception as e:
            logger.warning(f"{code} 第 {attempt}/{max_retries} 次周线请求异常: {e}")
            if attempt >= max_retries:
                break

            try:
                bs.logout()
            except Exception as logout_error:
                logger.warning(f"{code} 登出异常，继续重新登录: {logout_error}")

            time.sleep(1)
            lg = bs.login()
            if lg.error_code != '0':
                logger.warning(f"重新登录失败: {lg.error_msg}，等待5s后重试")
                time.sleep(5)
    return pd.DataFrame()


def main():
    uri = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
    engine = create_engine(uri, pool_pre_ping=True)

    if not login_with_retry():
        logger.error("Baostock login failed")
        return

    try:
        today = datetime.now()
        week_start = (today - timedelta(days=today.weekday())).strftime("%Y-%m-%d")
        week_end = today.strftime("%Y-%m-%d")
        logger.info(f"正在同步周线数据（{week_start} 至 {week_end}）")

        codes = []
        with open(CODE_CSV_PATH, "r") as f:
            for line in f:
                code = line.strip()
                if code and not code.startswith("#") and code.lower() != "code":
                    codes.append(code.zfill(6))
        synced_count = 0
        total = len(codes)
        for index, code in enumerate(codes, start=1):
            time.sleep(0.2)
            latest_date = get_latest(engine, code, "stock_weekly", "date")
            if latest_date:
                start_date = (datetime.strptime(latest_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                start_date = "2010-01-01"

            if start_date > week_end:
                logger.info(f"ℹ️ {code} 周线已是最新 {index}/{total}，最新日期 {latest_date}")
                continue

            try:
                df = fetch_with_relogin(code, start_date, week_end, "weekly")
            except Exception as e:
                logger.warning(f"⚠️ {code} 周线同步失败: {e}，等待30s重试")
                time.sleep(30)
                bs.logout()
                bs.login()
                df = fetch_baostock_data(code, start_date, week_end, "weekly")
            if not df.empty:
                upsert(df, "stock_weekly", engine, "date")
                synced_count += 1
                logger.info(f"✅ {code} 同步 {index}/{total} 条周线数据")
            else:
                logger.info(f"ℹ️ {code} 无新数据 {index}/{total}")
        logger.info(f"✅ 周线数据同步完成，本次写入 {synced_count} 只股票")
    except Exception as e:
        logger.exception(f"同步失败: {e}")
    finally:
        bs.logout()
        logger.info("✅ 周线同步任务结束")


if __name__ == "__main__":
    main()
