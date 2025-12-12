# sync_weekly.py

import os
import random
import sys
import time
import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sync_to_mysql import fetch_ak_data, upsert, get_latest

# ================== 配置 ==================
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "InsightOne123456")
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DB = os.getenv("MYSQL_DB", "stock_db")
CODE_CSV_PATH = os.getenv("CODE_CSV_PATH", "./code.csv")

# ================== 日志 ==================
log_dir = "/app/logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "sync_weekly.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# ================== 主逻辑 ==================
def main():
    uri = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
    engine = create_engine(uri, pool_pre_ping=True)

    try:
        # 获取本周起始日期（周一）
        today = datetime.now()
        week_start = (today - timedelta(days=today.weekday())).strftime("%Y%m%d")
        week_end = today.strftime("%Y%m%d")

        logger.info(f"正在同步周线数据（{week_start} 至 {week_end}）")

        # 从CSV加载股票代码
        codes = []
        with open(CODE_CSV_PATH, "r") as f:
            for line in f:
                code = line.strip()
                if code and not code.startswith("#"):
                    codes.append(code.zfill(6))

        # 分批处理股票
        batch_size = 10
        for i in range(0, len(codes), batch_size):
            batch_codes = codes[i:i + batch_size]

            for code in batch_codes:
                try:
                    time.sleep(random.uniform(30, 60))
                    # 获取最新周线日期
                    latest_date = get_latest(engine, code, "stock_weekly", "date")
                    if latest_date:
                        start_date = (datetime.strptime(latest_date, "%Y-%m-%d") + timedelta(days=7)).strftime("%Y%m%d")
                    else:
                        start_date = "20100101"  # 初次部署

                    # 获取数据（✅ 使用正确字段：amount和turnover_rate）
                    df = fetch_ak_data(code, start_date, week_end, "weekly")
                    if not df.empty:
                        upsert(df, "stock_weekly", engine, "date")
                        logger.info(f"✅ {code} 同步 {len(df)} 条周线数据")
                    else:
                        logger.info(f"ℹ️ {code} 无新数据")

                except Exception as e:
                    logger.error(f"❌ {code} 同步失败: {e}")

        logger.info("✅ 周线数据同步完成")

    except Exception as e:
        logger.exception(f"同步失败: {e}")
    finally:
        logger.info("✅ 周线同步任务结束")


if __name__ == "__main__":
    main()