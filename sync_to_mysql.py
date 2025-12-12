# sync_to_mysql.py

import os
import sys
import time as pytime
import logging
import random
import pandas as pd
import akshare as ak
import requests
from datetime import datetime, timedelta, time
from sqlalchemy import create_engine, text
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ================== 配置 ==================
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "InsightOne123456")
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DB = os.getenv("MYSQL_DB", "stock_db")
CODE_CSV_PATH = os.getenv("CODE_CSV_PATH", "./code.csv")
MAX_INCREMENTAL_DAYS = int(os.getenv("MAX_INCREMENTAL_DAYS", "5"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "40"))
PAUSE_MINUTES_MIN = int(os.getenv("PAUSE_MINUTES_MIN", "30"))
PAUSE_MINUTES_MAX = int(os.getenv("PAUSE_MINUTES_MAX", "50"))

# 全量数据起始日期（2010年1月1日）
FULL_DATA_START_DATE = "20100101"

# ================== 日志 ==================
log_dir = "./logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "sync_ak.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# ================== 核心数据处理函数 ==================
# 修改后的 fetch_ak_data 函数核心部分
def fetch_ak_data(code, start, end, freq="daily"):
    """从AKShare获取股票数据（支持日线/周线）"""

    def _fetch_segment(s, e):
        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=3, max=10),
            retry=retry_if_exception_type((requests.exceptions.RequestException, ConnectionError)),
            reraise=True
        )
        def _inner():
            user_agent = random.choice([
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
            ])
            original_get = requests.get

            def patched_get(url, **kwargs):
                if 'headers' not in kwargs:
                    kwargs['headers'] = {}
                kwargs['headers']['User-Agent'] = user_agent
                kwargs['proxies'] = {'http': None, 'https': None}
                return original_get(url, **kwargs)

            requests.get = patched_get
            try:
                if freq == "daily":
                    df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=s, end_date=e, adjust="qfq")
                elif freq == "weekly":
                    df = ak.stock_zh_a_hist(symbol=code, period="weekly", start_date=s, end_date=e, adjust="qfq")
                else:
                    return pd.DataFrame()

                if df.empty:
                    return df

                # ✅ 关键修正：正确映射所有字段（包含成交额和换手率）
                rename_map = {
                    '日期': 'date',
                    '开盘': 'open',
                    '最高': 'high',
                    '最低': 'low',
                    '收盘': 'close',
                    '成交量': 'volume',
                    '成交额': 'amount',  # ✅ 成交额 -> amount
                    '涨跌幅': 'pct_change',
                    '涨跌额': 'change',
                    '换手率': 'turnover_rate'  # ✅ 换手率 -> turnover_rate
                }
                df = df.rename(columns=rename_map)[[
                    'date', 'open', 'high', 'low', 'close',
                    'volume', 'amount', 'pct_change', 'change', 'turnover_rate'
                ]].copy()

                # ✅ 统一处理所有数值字段（含amount和turnover_rate）
                # 先将所有字段转换为字符串，再处理逗号
                for col in ['open', 'high', 'low', 'close', 'volume', 'amount', 'pct_change', 'change',
                            'turnover_rate']:
                    # 将非字符串类型转换为字符串
                    if df[col].dtype != 'object':
                        df[col] = df[col].astype(str)

                    # 移除逗号
                    df[col] = df[col].str.replace(',', '', regex=False)

                    # 转换为数值类型
                    df[col] = pd.to_numeric(df[col], errors='coerce')

                df['code'] = code
                df['date'] = pd.to_datetime(df['date'])
                return df
            finally:
                requests.get = original_get

        return _inner()

    start_dt = datetime.strptime(start, "%Y%m%d")
    end_dt = datetime.strptime(end, "%Y%m%d")
    all_dfs = []
    current = start_dt

    while current <= end_dt:
        next_seg = current.replace(year=current.year + 2)
        seg_end = min(next_seg - timedelta(days=1), end_dt)
        s_str = current.strftime("%Y%m%d")
        e_str = seg_end.strftime("%Y%m%d")

        try:
            df_seg = _fetch_segment(s_str, e_str)
            if not df_seg.empty:
                all_dfs.append(df_seg)
        except Exception as e:
            logger.warning(f"⚠️ 片段失败 ({code} {s_str}-{e_str}): {e}")
            pytime.sleep(random.uniform(1.0, 1.8))

        current = next_seg

    if all_dfs:
        result = pd.concat(all_dfs, ignore_index=True)
        result = result.drop_duplicates(subset=['date']).sort_values('date').reset_index(drop=True)
        return result
    else:
        return pd.DataFrame()


def upsert(df, table, engine, date_col):
    """批量更新/插入数据（删除旧数据后插入新数据）"""
    if df.empty:
        return

    codes = df['code'].unique().tolist()
    dates = df[date_col].dt.strftime('%Y-%m-%d').unique().tolist()

    with engine.connect() as conn:
        placeholders_c = ','.join([f"'{c}'" for c in codes])
        placeholders_d = ','.join([f"'{d}'" for d in dates])

        delete_sql = f"""
        DELETE FROM `{table}` WHERE `code` IN ({placeholders_c}) AND `{date_col}` IN ({placeholders_d})
        """
        conn.execute(text(delete_sql))
        conn.commit()

        df.to_sql(table, con=engine, if_exists='append', index=False, method='multi')


def get_latest(engine, code, table, col):
    """获取表中指定股票的最新日期"""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT MAX(`{col}`) FROM `{table}` WHERE `code` = :c"),
                {"c": code}
            ).scalar()
            return result.strftime("%Y-%m-%d") if result else None
    except Exception as e:
        logger.debug(f"获取最新日期失败 ({code}): {e}")
        return None


def create_tables(engine):
    """初始化数据库表结构（包含所有字段）"""
    with engine.connect() as conn:
        # ✅ 日线表（包含amount和turnover_rate）
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS `stock_daily` (
            `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
            `code` VARCHAR(20) NOT NULL,
            `date` DATE NOT NULL,
            `open` DECIMAL(10,3),
            `high` DECIMAL(10,3),
            `low` DECIMAL(10,3),
            `close` DECIMAL(10,3),
            `volume` BIGINT,
            `amount` DECIMAL(15,2),  -- ✅ 成交额字段
            `pct_change` DECIMAL(10,2),
            `change` DECIMAL(10,2),
            `turnover_rate` DECIMAL(10,2),  -- ✅ 换手率字段
            UNIQUE KEY `uk_code_date` (`code`, `date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))

        # ✅ 周线表（包含所有字段）
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS `stock_weekly` (
            `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
            `code` VARCHAR(20) NOT NULL,
            `date` DATE NOT NULL,
            `open` DECIMAL(10,3),
            `high` DECIMAL(10,3),
            `low` DECIMAL(10,3),
            `close` DECIMAL(10,3),
            `volume` BIGINT,
            `amount` DECIMAL(15,2),  -- ✅ 成交额
            `pct_change` DECIMAL(10,2),
            `change` DECIMAL(10,2),
            `turnover_rate` DECIMAL(10,2),  -- ✅ 换手率
            UNIQUE KEY `uk_code_date` (`code`, `date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))

        conn.commit()
        logger.info("✅ 数据表已初始化（包含成交额和换手率字段）")


# ================== 主逻辑 ==================
def main():
    uri = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
    engine = create_engine(uri, pool_pre_ping=True)
    create_tables(engine)

    try:
        all_codes = load_codes()
        if not all_codes:
            logger.warning("⚠️ 未加载到任何股票代码，请检查 code.csv")
            return

        logger.info(f"共 {len(all_codes)} 只股票 | 批大小: {BATCH_SIZE}")

        now = datetime.now()
        end_date_str = now.strftime("%Y%m%d")

        # 15:30后同步当天数据，否则同步前一天
        if now.time() >= time(15, 30):
            end_date_str = now.strftime("%Y%m%d")
        else:
            end_date_str = (now - timedelta(days=1)).strftime("%Y%m%d")

        logger.info(f"同步截止日期: {end_date_str}")

        failed_list = []
        total = len(all_codes)
        cnt = 0
        for code in all_codes:
            cnt += 1
            logger.info(f"正在同步{cnt}/{total}")
            pytime.sleep(random.uniform(30, 60))
            try:
                # 同步日线数据
                latest_daily = get_latest(engine, code, "stock_daily", "date")
                if latest_daily:
                    start_str = (datetime.strptime(latest_daily, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y%m%d")
                else:
                    start_str = FULL_DATA_START_DATE

                df_d = fetch_ak_data(code, start_str, end_date_str, "daily")
                if not df_d.empty:
                    upsert(df_d, "stock_daily", engine, "date")

                # 同步周线数据
                latest_weekly = get_latest(engine, code, "stock_weekly", "date")
                if latest_weekly:
                    start_str = (datetime.strptime(latest_weekly, "%Y-%m-%d") + timedelta(days=7)).strftime(
                        "%Y%m%d")
                else:
                    start_str = FULL_DATA_START_DATE

                df_w = fetch_ak_data(code, start_str, end_date_str, "weekly")
                if not df_w.empty:
                    upsert(df_w, "stock_weekly", engine, "date")

            except Exception as e:
                logger.error(f"💥 {code} 同步崩溃: {e}", exc_info=True)
                failed_list.append(code)
            logger.info(f"同步完成{cnt}/{total}")

        # 保存失败列表
        if failed_list:
            fail_file = os.path.join(log_dir, "failed_codes.txt")
            with open(fail_file, "w") as f:
                f.write("\n".join(failed_list))
            logger.warning(f"❌ {len(failed_list)} 只股票同步失败，已保存至: {fail_file}")
        else:
            logger.info("🎉 所有股票同步成功！")

    except Exception as e:
        logger.exception(f"主程序异常: {e}")
    finally:
        logger.info("✅ 同步任务结束")


if __name__ == "__main__":
    # 从CSV加载股票代码
    def load_codes():
        df = pd.read_csv(CODE_CSV_PATH, dtype={"code": str})
        codes = []
        for c in df["code"].dropna().str.strip():
            if c:
                codes.append(c.zfill(6))
        return codes


    main()