import argparse
import csv
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd
from sqlalchemy import create_engine, text

# ================== 配置 ==================
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "InsightOne123456")
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
# MYSQL_HOST = os.getenv("MYSQL_HOST", "43.138.124.143")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DB = os.getenv("MYSQL_DB", "stock_db_qfq")
CODE_CSV_PATH = os.getenv("CODE_CSV_PATH", "./code.csv")
EASTMONEY_URL = "https://push2.eastmoney.com/api/qt/ulist.np/get"
REQUEST_TIMEOUT = 10

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Referer": "https://finance.sina.com.cn/",
}

EASTMONEY_FIELD_MAP = {
    "f2": "latest_price",
    "f3": "pct_chg",
    "f4": "chg_amount",
    "f5": "volume_hand",
    "f6": "amount_yuan",
    "f8": "turnover_rate_pct",
    "f9": "pe_dynamic",
    "f10": "volume_ratio",
    "f12": "code",
    "f13": "market_no",
    "f14": "name",
    "f15": "high",
    "f16": "low",
    "f17": "open",
    "f18": "preclose",
    "f20": "total_mv_yuan",
    "f21": "float_mv_yuan",
    "f124": "updated_ts",
}

PRICE_FIELDS = {
    "latest_price",
    "chg_amount",
    "high",
    "low",
    "open",
    "preclose",
}

# ================== 日志 ==================
log_dir = "./logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "sync_realtime_daily.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
    force=True,
)
logger = logging.getLogger(__name__)

DAILY_COLUMNS = [
    "code",
    "date",
    "open",
    "high",
    "low",
    "close",
    "preclose",
    "volume",
    "amount",
    "adjustflag",
    "turn",
    "tradestatus",
    "pctChg",
    "peTTM",
    "pbMRQ",
    "psTTM",
    "pcfNcfTTM",
    "isST",
]


def normalize_code(code: str) -> str:
    code = str(code).strip().lower()
    if "." in code:
        _, code = code.split(".", 1)
    return code.zfill(6)


def get_market_prefix(code: str) -> str:
    if code.startswith(("5", "6", "9")):
        return "sh"
    if code.startswith(("4", "8")):
        return "bj"
    return "sz"


def get_secid(code: str) -> str:
    prefix = get_market_prefix(code)
    market_no = {"sh": "1", "sz": "0", "bj": "0"}[prefix]
    return f"{market_no}.{code}"


def to_float(value):
    if value in (None, "", "-", "--"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value):
    if value in (None, "", "-", "--"):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def round_if_needed(value, digits=4):
    if value is None:
        return None
    return round(float(value), digits)


def format_timestamp(ts_value):
    ts_value = to_int(ts_value)
    if ts_value is None or ts_value <= 0:
        return None
    return datetime.fromtimestamp(ts_value).strftime("%Y-%m-%d %H:%M:%S")


def http_get_json(url: str, params=None) -> dict:
    query = urlencode(params or ())
    full_url = f"{url}?{query}" if query else url
    req = Request(full_url, headers=HEADERS)
    with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        import json

        return json.loads(resp.read().decode("utf-8"))


def parse_eastmoney_row(row: dict) -> dict:
    result = {}
    for field, alias in EASTMONEY_FIELD_MAP.items():
        result[alias] = row.get(field)

    for field in PRICE_FIELDS:
        result[field] = to_float(result[field])
    for field in ("pct_chg", "turnover_rate_pct", "volume_ratio", "pe_dynamic"):
        result[field] = to_float(result[field])
    for field in ("volume_hand", "amount_yuan", "total_mv_yuan", "float_mv_yuan"):
        result[field] = to_float(result[field])

    result["market"] = {
        "1": "SH",
        "0": "SZ/BJ",
    }.get(str(result["market_no"]), str(result["market_no"]))
    result["updated_time"] = format_timestamp(result["updated_ts"])
    result["amount_wan"] = (
        round_if_needed(result["amount_yuan"] / 10000, 2)
        if result["amount_yuan"]
        else None
    )
    result["total_mv_yi"] = (
        round_if_needed(result["total_mv_yuan"] / 1e8, 2)
        if result["total_mv_yuan"]
        else None
    )
    result["float_mv_yi"] = (
        round_if_needed(result["float_mv_yuan"] / 1e8, 2)
        if result["float_mv_yuan"]
        else None
    )
    return result


def fetch_eastmoney_quote(code: str) -> dict:
    code = normalize_code(code)
    fields = ",".join(EASTMONEY_FIELD_MAP.keys())
    params = (
        ("OSVersion", "14.3"),
        ("appVersion", "6.3.8"),
        ("fields", fields),
        ("fltt", "2"),
        ("plat", "Iphone"),
        ("product", "EFund"),
        ("secids", get_secid(code)),
        ("serverVersion", "6.3.6"),
        ("version", "6.3.8"),
    )
    payload = http_get_json(EASTMONEY_URL, params=params)
    rows = payload.get("data", {}).get("diff") or []
    if not rows:
        raise ValueError(f"东财实时行情返回为空: {code}")
    return parse_eastmoney_row(rows[0])


def upsert(df, table, engine, date_col):
    if df.empty:
        return

    df = df.where(pd.notnull(df), None)
    df = df.replace("", pd.NA)

    codes = df["code"].unique().tolist()
    dates = pd.to_datetime(df[date_col]).dt.strftime("%Y-%m-%d").unique().tolist()

    with engine.connect() as conn:
        with conn.begin():
            placeholders_c = ",".join([f"'{code}'" for code in codes])
            placeholders_d = ",".join([f"'{day}'" for day in dates])
            delete_sql = f"""
            DELETE FROM `{table}` WHERE `code` IN ({placeholders_c}) AND `{date_col}` IN ({placeholders_d})
            """
            conn.execute(text(delete_sql))
            df.to_sql(table, con=conn, if_exists="append", index=False, method="multi")

def parse_arguments():
    parser = argparse.ArgumentParser(description="将盘中实时行情写入 stock_daily")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="目标交易日，格式 YYYY-MM-DD，默认使用当天日期",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help="实时抓取并发数，默认 16",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=500,
        help="每批写库的股票数量，默认 500",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="仅处理前 N 只股票，便于测试",
    )
    parser.add_argument(
        "--code",
        action="append",
        default=None,
        help="指定单只或多只股票代码，可重复传入，如 --code 600519 --code 000001",
    )
    return parser.parse_args()


def validate_date(date_str):
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def normalize_plain_code(raw_code):
    if raw_code is None:
        return None
    text_value = str(raw_code).strip()
    if not text_value or text_value.startswith("#"):
        return None
    lowered = text_value.lower()
    if lowered in {"code", "股票代码", "ts_code"}:
        return None
    if "," in text_value:
        text_value = text_value.split(",", 1)[0]
    digits = "".join(ch for ch in text_value if ch.isdigit())
    if not digits:
        return None
    return normalize_code(digits)


def load_codes(code_csv_path, selected_codes=None, limit=None):
    if selected_codes:
        codes = []
        seen = set()
        for raw_code in selected_codes:
            code = normalize_plain_code(raw_code)
            if code and code not in seen:
                seen.add(code)
                codes.append(code)
        if limit is not None:
            return codes[:limit]
        return codes

    codes = []
    seen = set()
    with open(code_csv_path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if not row:
                continue
            code = normalize_plain_code(row[0])
            if code and code not in seen:
                seen.add(code)
                codes.append(code)

    if limit is not None:
        return codes[:limit]
    return codes


def chunked(seq, size):
    for idx in range(0, len(seq), size):
        yield seq[idx: idx + size]


def parse_quote_date(updated_time):
    if not updated_time:
        return None
    try:
        return datetime.strptime(updated_time, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
    except ValueError:
        return None


def fetch_inherited_rows(engine, codes, target_date):
    result = {}
    if not codes:
        return result

    with engine.connect() as conn:
        for code_batch in chunked(codes, 500):
            params = {"target_date": target_date}
            placeholders = []
            for idx, code in enumerate(code_batch):
                key = f"code_{idx}"
                placeholders.append(f":{key}")
                params[key] = code

            sql = f"""
            SELECT d.`code`, d.`date`, d.`open`, d.`high`, d.`low`, d.`close`, d.`preclose`,
                   d.`volume`, d.`amount`, d.`adjustflag`, d.`turn`, d.`tradestatus`,
                   d.`pctChg`, d.`peTTM`, d.`pbMRQ`, d.`psTTM`, d.`pcfNcfTTM`, d.`isST`
            FROM `stock_daily` d
            JOIN (
                SELECT `code`, MAX(`date`) AS max_date
                FROM `stock_daily`
                WHERE `date` <= :target_date AND `code` IN ({",".join(placeholders)})
                GROUP BY `code`
            ) latest
                ON latest.`code` = d.`code` AND latest.max_date = d.`date`
            """
            rows = conn.execute(text(sql), params).mappings().all()
            for row in rows:
                mapped = dict(row)
                mapped["date"] = mapped["date"].strftime("%Y-%m-%d")
                result[mapped["code"]] = mapped
    return result


def build_daily_row(code, target_date, quote, inherited_row):
    quote_date = parse_quote_date(quote.get("updated_time"))
    if quote_date and quote_date != target_date:
        return None, f"实时日期不匹配: target={target_date}, quote={quote_date}"

    inherited_date = inherited_row.get("date") if inherited_row else None
    same_day_inherited = inherited_date == target_date

    latest_price = quote.get("latest_price")
    open_price = quote.get("open")
    high_price = quote.get("high")
    low_price = quote.get("low")
    preclose_price = quote.get("preclose")
    amount_yuan = quote.get("amount_yuan")
    volume_hand = quote.get("volume_hand")
    turn_pct = quote.get("turnover_rate_pct")
    pct_chg = quote.get("pct_chg")
    pe_dynamic = quote.get("pe_dynamic")
    has_trading = any(
        value not in (None, 0, 0.0)
        for value in (amount_yuan, volume_hand, turn_pct)
    )

    if latest_price is None:
        if same_day_inherited and inherited_row.get("close") is not None:
            latest_price = inherited_row.get("close")
        else:
            return None, "实时最新价为空"

    if preclose_price is None and inherited_row:
        if same_day_inherited:
            preclose_price = inherited_row.get("preclose")
        else:
            preclose_price = inherited_row.get("close")

    row = {
        "code": code,
        "date": pd.to_datetime(target_date),
        "open": open_price if open_price is not None else (inherited_row.get("open") if same_day_inherited else None),
        "high": high_price if high_price is not None else (inherited_row.get("high") if same_day_inherited else None),
        "low": low_price if low_price is not None else (inherited_row.get("low") if same_day_inherited else None),
        "close": latest_price,
        "preclose": preclose_price,
        "volume": int(volume_hand * 100) if volume_hand is not None else (inherited_row.get("volume") if same_day_inherited else None),
        "amount": amount_yuan if amount_yuan is not None else (inherited_row.get("amount") if same_day_inherited else None),
        "adjustflag": inherited_row.get("adjustflag") if inherited_row and inherited_row.get("adjustflag") is not None else 2,
        "turn": turn_pct if turn_pct is not None else (inherited_row.get("turn") if same_day_inherited else None),
        "tradestatus": 1 if has_trading else (inherited_row.get("tradestatus") if same_day_inherited else 0),
        "pctChg": pct_chg if pct_chg is not None else (inherited_row.get("pctChg") if same_day_inherited else None),
        "peTTM": pe_dynamic if pe_dynamic is not None else (inherited_row.get("peTTM") if inherited_row else None),
        "pbMRQ": inherited_row.get("pbMRQ") if inherited_row else None,
        "psTTM": inherited_row.get("psTTM") if inherited_row else None,
        "pcfNcfTTM": inherited_row.get("pcfNcfTTM") if inherited_row else None,
        "isST": inherited_row.get("isST") if inherited_row else None,
    }
    return row, None


def fetch_quote_safely(code):
    try:
        return code, fetch_eastmoney_quote(code), None
    except Exception as exc:
        return code, None, str(exc)


def main():
    args = parse_arguments()

    if args.date and not validate_date(args.date):
        logger.error("❌ 日期格式错误，请使用 YYYY-MM-DD")
        return

    target_date = args.date or datetime.now().strftime("%Y-%m-%d")
    uri = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
    engine = create_engine(uri, pool_pre_ping=True)
    started_at = time.time()

    try:
        codes = load_codes(CODE_CSV_PATH, selected_codes=args.code, limit=args.limit)
        if not codes:
            logger.error("❌ 未加载到任何股票代码，请检查 code.csv 或 --code 参数")
            return

        inherited_map = fetch_inherited_rows(engine, codes, target_date)
        logger.info(f"目标交易日: {target_date}")
        logger.info(f"待处理股票数: {len(codes)}")
        logger.info(f"并发数: {args.workers}, 写库批次: {args.chunk_size}")

        prepared_rows = []
        failed_items = []

        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
            futures = {executor.submit(fetch_quote_safely, code): code for code in codes}
            for idx, future in enumerate(as_completed(futures), 1):
                code, quote, error = future.result()
                if error:
                    failed_items.append({"code": code, "reason": error})
                else:
                    row, build_error = build_daily_row(code, target_date, quote, inherited_map.get(code))
                    if build_error:
                        failed_items.append({"code": code, "reason": build_error})
                    else:
                        prepared_rows.append(row)

                if idx % 200 == 0 or idx == len(codes):
                    logger.info(f"已抓取 {idx}/{len(codes)}，成功 {len(prepared_rows)}，失败 {len(failed_items)}")

        if prepared_rows:
            prepared_df = pd.DataFrame(prepared_rows, columns=DAILY_COLUMNS)
            for batch in chunked(prepared_df.to_dict("records"), max(1, args.chunk_size)):
                upsert(pd.DataFrame(batch, columns=DAILY_COLUMNS), "stock_daily", engine, "date")
            logger.info(f"✅ 已写入 {len(prepared_df)} 条盘中日线快照")
        else:
            logger.warning("⚠️ 没有可写入的实时日线数据")

        if failed_items:
            failed_df = pd.DataFrame(failed_items)
            fail_file = os.path.join(log_dir, f"failed_realtime_daily_{target_date}.csv")
            failed_df.to_csv(fail_file, index=False, encoding="utf-8-sig")
            logger.warning(f"⚠️ {len(failed_df)} 只股票写入失败，详情已保存到 {fail_file}")

    except Exception as exc:
        logger.exception(f"💥 盘中日线写入失败: {exc}")
        raise
    finally:
        logger.info(f"总耗时: {round(time.time() - started_at, 2)}s")


if __name__ == "__main__":
    main()
