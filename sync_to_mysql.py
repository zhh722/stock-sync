# sync_to_mysql.py
import os
import sys
import time as pytime
import logging
import pandas as pd
import baostock as bs
from datetime import datetime, timedelta, time
from sqlalchemy import create_engine, text

# ================== 配置 ==================
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "InsightOne123456")
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DB = os.getenv("MYSQL_DB", "stock_db")
CODE_CSV_PATH = os.getenv("CODE_CSV_PATH", "./code.csv")

# ================== 日志 ==================
log_dir = "./logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "sync_baostock.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


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
        fields = "date,code,open,high,low,close,volume,amount,adjustflag"

    rs = bs.query_history_k_data_plus(
        code_bs,
        fields,
        start_date=start,
        end_date=end,
        frequency=frequency,
        adjustflag="3"  # 前复权
    )

    if rs.error_code != '0':
        logger.warning(f"Baostock query failed for {code}: {rs.error_msg}")
        return pd.DataFrame()

    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())

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

def upsert(df, table, engine, date_col):
    """批量更新/插入数据"""
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
    """获取最新日期"""
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


def load_codes():
    """从 CSV 加载股票代码"""
    codes = []
    with open(CODE_CSV_PATH, "r") as f:
        for line in f:
            code = line.strip()
            if code and not code.startswith("#"):
                codes.append(code.zfill(6))
    return codes


def main():
    uri = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
    engine = create_engine(uri, pool_pre_ping=True)

    # 登录 Baostock
    lg = bs.login()
    if lg.error_code != '0':
        logger.error(f"❌ Baostock login failed: {lg.error_msg}")
        return

    try:
        all_codes = load_codes()
        if not all_codes:
            logger.warning("⚠️ 未加载到任何股票代码，请检查 code.csv")
            return
        logger.info(f"共 {len(all_codes)} 只股票")

        now = datetime.now()
        # 15:30 后包含当天，否则截止到昨天
        if now.time() >= time(15, 30):
            end_date_str = now.strftime("%Y-%m-%d")
        else:
            end_date_str = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        logger.info(f"同步截止日期: {end_date_str}")

        failed_list = []
        total = len(all_codes)
        for i, code in enumerate(all_codes, 1):
            logger.info(f"正在同步 {i}/{total}: {code}")
            pytime.sleep(0.5)  # 防限流

            try:
                # 同步日线
                latest_daily = get_latest(engine, code, "stock_daily", "date")
                start_str = (datetime.strptime(latest_daily, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d") \
                    if latest_daily else "2010-01-01"
                df_d = fetch_baostock_data(code, start_str, end_date_str, "daily")
                if not df_d.empty:
                    upsert(df_d, "stock_daily", engine, "date")

                # 同步周线
                latest_weekly = get_latest(engine, code, "stock_weekly", "date")
                start_str = (datetime.strptime(latest_weekly, "%Y-%m-%d") + timedelta(days=7)).strftime("%Y-%m-%d") \
                    if latest_weekly else "2010-01-01"
                df_w = fetch_baostock_data(code, start_str, end_date_str, "weekly")
                if not df_w.empty:
                    upsert(df_w, "stock_weekly", engine, "date")

            except Exception as e:
                logger.error(f"💥 {code} 同步崩溃: {e}", exc_info=True)
                failed_list.append(code)

        if failed_list:
            fail_file = os.path.join(log_dir, "failed_codes_baostock.txt")
            with open(fail_file, "w") as f:
                f.write("\n".join(failed_list))
            logger.warning(f"❌ {len(failed_list)} 只股票同步失败，已保存至: {fail_file}")
        else:
            logger.info("🎉 所有股票同步成功！")

    finally:
        bs.logout()
        logger.info("✅ 同步任务结束")


if __name__ == "__main__":
    main()