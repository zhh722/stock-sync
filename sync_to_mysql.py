# sync_to_mysql.py
import os
import sys
import logging
import pandas as pd
import baostock as bs
from datetime import datetime, timedelta, time
from sqlalchemy import create_engine, text

# ================== 配置 ==================
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "InsightOne123456")
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")  # ✅ 适配 Docker
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DB = os.getenv("MYSQL_DB", "stock_db")
CODE_CSV_PATH = os.getenv("CODE_CSV_PATH", "./code.csv")
INIT_DAYS = int(os.getenv("INIT_DAYS", "365"))
MAX_INCREMENTAL_DAYS = int(os.getenv("MAX_INCREMENTAL_DAYS", "5"))

# ================== 日志 ==================
log_dir = "./logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "sync.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# ================== 工具函数 ==================
def load_codes():
    df = pd.read_csv(CODE_CSV_PATH, dtype={"code": str})
    codes = []
    for c in df["code"].dropna().str.strip():
        if c:
            prefix = "sh." if c[0] in "6597" else "sz."
            codes.append(prefix + c)
    return codes


def fetch_k_data(code, start, end, freq="d"):
    fields = "date,open,high,low,close,volume"
    rs = bs.query_history_k_data_plus(code, fields, start, end, freq, "2")

    if rs.error_code != '0':
        logger.warning(f"Baostock 查询失败 ({code}): {rs.error_msg}")
        return pd.DataFrame()

    data = []
    while (rs.error_code == '0') and rs.next():
        data.append(rs.get_row_data())

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data, columns=fields.split(","))
    df['code'] = code
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['date'] = pd.to_datetime(df['date'])
    return df


def upsert(df, table, engine, date_col):
    if df.empty:
        return

    codes = df['code'].unique().tolist()
    dates = df[date_col].dt.strftime('%Y-%m-%d').unique().tolist()

    with engine.connect() as conn:
        placeholders_c = ','.join([f"'{c}'" for c in codes])
        placeholders_d = ','.join([f"'{d}'" for d in dates])
        delete_sql = f"""
            DELETE FROM `{table}`
            WHERE `code` IN ({placeholders_c}) AND `{date_col}` IN ({placeholders_d})
        """
        conn.execute(text(delete_sql))
        conn.commit()

    df.to_sql(table, con=engine, if_exists='append', index=False, method='multi')


def get_latest(engine, code, table, col):
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
    """自动创建数据表（幂等操作）"""
    with engine.connect() as conn:
        # 日线表
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
                UNIQUE KEY `uk_code_date` (`code`, `date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))

        # 周线表
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS `stock_weekly` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `code` VARCHAR(20) NOT NULL,
                `week_start` DATE NOT NULL,
                `open` DECIMAL(10,3),
                `high` DECIMAL(10,3),
                `low` DECIMAL(10,3),
                `close` DECIMAL(10,3),
                `volume` BIGINT,
                UNIQUE KEY `uk_code_week` (`code`, `week_start`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))
        conn.commit()
    logger.info("✅ 数据表已初始化（如不存在）")


# ================== 主逻辑 ==================
def main():
    # 移除 auth_plugin 以提高兼容性（MySQL 5.7 / 8.0 均可）
    uri = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
    engine = create_engine(uri, pool_pre_ping=True)

    # 登录 Baostock
    login_result = bs.login()
    if login_result.error_code != '0':
        logger.error(f"❌ Baostock 登录失败: {login_result.error_msg}")
        sys.exit(1)
    logger.info("✅ Baostock 登录成功")

    # 自动建表
    create_tables(engine)

    try:
        codes = load_codes()
        if not codes:
            logger.warning("⚠️ 未加载到任何股票代码，请检查 code.csv")
            return
        logger.info(f"共 {len(codes)} 只股票")

        now = datetime.now()
        # 若当前时间晚于 18:30，则包含当天；否则截止到昨天
        end_date = now.strftime("%Y-%m-%d") if now.time() >= time(18, 30) else (now - timedelta(days=1)).strftime("%Y-%m-%d")
        logger.info(f"同步截止日期: {end_date}")

        for i, code in enumerate(codes):
            try:
                logger.info(f"[{i + 1}/{len(codes)}] {code}")

                # ===== 同步日线（增量）=====
                latest = get_latest(engine, code, "stock_daily", "date")
                if latest:
                    start = max(
                        (datetime.strptime(latest, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d"),
                        (now - timedelta(days=MAX_INCREMENTAL_DAYS)).strftime("%Y-%m-%d")
                    )
                else:
                    start = (now - timedelta(days=INIT_DAYS)).strftime("%Y-%m-%d")
                    logger.info(f"🆕 首次同步日线：{start} ~ {end_date}")

                df_d = fetch_k_data(code, start, end_date, "d")
                upsert(df_d, "stock_daily", engine, "date")

                # ===== 同步周线（全量覆盖最近 INIT_DAYS 天）=====
                start_w = (now - timedelta(days=INIT_DAYS)).strftime("%Y-%m-%d")
                df_w = fetch_k_data(code, start_w, end_date, "w")
                if not df_w.empty:
                    df_w.rename(columns={"date": "week_start"}, inplace=True)
                    upsert(df_w, "stock_weekly", engine, "week_start")

            except Exception as e:
                logger.error(f"❌ {code} 同步失败: {e}", exc_info=True)

    finally:
        bs.logout()
        logger.info("✅ 同步完成")


if __name__ == "__main__":
    main()