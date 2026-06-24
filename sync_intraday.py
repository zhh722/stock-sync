#!/usr/bin/env python3
"""批量同步 code.csv 中股票的 1 分钟分时数据到 MySQL。"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from fetch_intraday_one import (
    fetch_intraday_trends,
    filter_rows_by_date,
    normalize_code,
    parse_trends,
)

MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "InsightOne123456")
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DB = os.getenv("MYSQL_DB", "stock_db_qfq")
CODE_CSV_PATH = os.getenv("CODE_CSV_PATH", "./code.csv")

TABLE_NAME = "stock_intraday_1m"
SOURCE_NAME = "tencent_mkline_m1"

LOG_DIR = Path("./logs")
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "sync_intraday.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
  `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
  `code` VARCHAR(20) NOT NULL COMMENT '6位股票代码',
  `name` VARCHAR(64) DEFAULT NULL COMMENT '股票名称',
  `trade_date` DATE NOT NULL COMMENT '交易日期',
  `trade_time` TIME NOT NULL COMMENT '分钟时间',
  `trade_datetime` DATETIME NOT NULL COMMENT '分钟时间戳',
  `open` DECIMAL(10,4) DEFAULT NULL,
  `close` DECIMAL(10,4) DEFAULT NULL,
  `high` DECIMAL(10,4) DEFAULT NULL,
  `low` DECIMAL(10,4) DEFAULT NULL,
  `volume_hand` DECIMAL(20,4) DEFAULT NULL COMMENT '成交量（手）',
  `turnover_rate_pct` DECIMAL(12,6) DEFAULT NULL COMMENT '换手率（%）',
  `source` VARCHAR(32) NOT NULL DEFAULT '{SOURCE_NAME}',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `uk_code_trade_datetime` (`code`, `trade_datetime`),
  KEY `idx_trade_date` (`trade_date`),
  KEY `idx_code_trade_date` (`code`, `trade_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

UPSERT_SQL = f"""
INSERT INTO `{TABLE_NAME}` (
  `code`, `name`, `trade_date`, `trade_time`, `trade_datetime`,
  `open`, `close`, `high`, `low`, `volume_hand`, `turnover_rate_pct`, `source`
) VALUES (
  :code, :name, :trade_date, :trade_time, :trade_datetime,
  :open, :close, :high, :low, :volume_hand, :turnover_rate_pct, :source
)
ON DUPLICATE KEY UPDATE
  `name` = VALUES(`name`),
  `trade_date` = VALUES(`trade_date`),
  `trade_time` = VALUES(`trade_time`),
  `open` = VALUES(`open`),
  `close` = VALUES(`close`),
  `high` = VALUES(`high`),
  `low` = VALUES(`low`),
  `volume_hand` = VALUES(`volume_hand`),
  `turnover_rate_pct` = VALUES(`turnover_rate_pct`),
  `source` = VALUES(`source`),
  `updated_at` = CURRENT_TIMESTAMP;
"""


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="批量同步 1 分钟分时数据到 MySQL")
    parser.add_argument("--code-csv", default=CODE_CSV_PATH, help="股票代码 CSV 路径，默认 ./code.csv")
    parser.add_argument("--date", help="只同步指定交易日，格式 YYYY-MM-DD；不填则同步接口返回的新数据")
    parser.add_argument("--bars", type=int, default=32000, help="向腾讯接口请求的 1 分钟K线条数，默认 32000")
    parser.add_argument("--limit", type=int, default=0, help="只处理前 N 只股票，便于试跑")
    parser.add_argument("--sleep-min", type=float, default=0.6, help="每只股票请求后的最小等待秒数")
    parser.add_argument("--sleep-max", type=float, default=1.2, help="每只股票请求后的最大等待秒数")
    parser.add_argument(
        "--refresh-existing",
        action="store_true",
        help="不按数据库最新时间过滤，重新写入接口返回范围内的所有记录",
    )
    parser.add_argument("--dry-run", action="store_true", help="只抓取和统计，不写入数据库")
    return parser.parse_args()


def build_engine() -> Any:
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL

    url = URL.create(
        "mysql+mysqlconnector",
        username=MYSQL_USER,
        password=MYSQL_PASSWORD,
        host=MYSQL_HOST,
        port=int(MYSQL_PORT),
        database=MYSQL_DB,
        query={"charset": "utf8mb4"},
    )
    return create_engine(url, pool_pre_ping=True)


def validate_date(date_text: str | None) -> None:
    if date_text is None:
        return
    try:
        datetime.strptime(date_text, "%Y-%m-%d")
    except ValueError as exc:
        raise SystemExit(f"日期格式错误: {date_text}，请使用 YYYY-MM-DD") from exc


def load_codes(csv_path: str) -> list[str]:
    codes: list[str] = []
    seen: set[str] = set()
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if not row:
                continue
            raw_code = row[0].strip()
            if not raw_code or raw_code.startswith("#") or raw_code.lower() == "code":
                continue
            code = normalize_code(raw_code)
            if code not in seen:
                seen.add(code)
                codes.append(code)
    return codes


def ensure_table(engine: Any) -> None:
    from sqlalchemy import text

    with engine.begin() as conn:
        conn.execute(text(CREATE_TABLE_SQL))


def get_latest_trade_datetime(engine: Any, code: str) -> str | None:
    from sqlalchemy import text

    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT MAX(`trade_datetime`) FROM `{TABLE_NAME}` WHERE `code` = :code"),
            {"code": code},
        ).scalar()
    return result.strftime("%Y-%m-%d %H:%M") if result else None


def to_db_rows(rows: list[dict]) -> list[dict]:
    db_rows = []
    for row in rows:
        db_rows.append(
            {
                "code": row["code"],
                "name": row["name"],
                "trade_date": row["date"],
                "trade_time": f"{row['time']}:00",
                "trade_datetime": row["datetime"],
                "open": row["open"],
                "close": row["close"],
                "high": row["high"],
                "low": row["low"],
                "volume_hand": row["volume_hand"],
                "turnover_rate_pct": row["turnover_rate_pct"],
                "source": SOURCE_NAME,
            }
        )
    return db_rows


def upsert_intraday_rows(engine: Any, rows: list[dict]) -> None:
    from sqlalchemy import text

    if not rows:
        return
    with engine.begin() as conn:
        conn.execute(text(UPSERT_SQL), rows)


def fetch_code_rows(code: str, bars: int, target_date: str | None) -> list[dict]:
    data = fetch_intraday_trends(code, bars=max(240, bars))
    rows = parse_trends(code, data.get("name", ""), data["rows"])
    if target_date:
        rows = filter_rows_by_date(rows, target_date)
    return rows


def write_failed_codes(failed_codes: list[str]) -> None:
    if not failed_codes:
        return
    failed_path = LOG_DIR / "failed_intraday_codes.txt"
    failed_path.write_text("\n".join(failed_codes), encoding="utf-8")
    logger.warning("失败股票 %s 只，已写入 %s", len(failed_codes), failed_path)


def main() -> None:
    args = parse_arguments()
    validate_date(args.date)
    if args.sleep_min < 0 or args.sleep_max < args.sleep_min:
        raise SystemExit("--sleep-max 必须大于等于 --sleep-min，且等待时间不能为负数")

    codes = load_codes(args.code_csv)
    if args.limit > 0:
        codes = codes[: args.limit]
    if not codes:
        logger.warning("未加载到任何股票代码，请检查 %s", args.code_csv)
        return

    engine = None if args.dry_run else build_engine()
    if engine is not None:
        ensure_table(engine)

    logger.info("准备同步 %s 只股票，date=%s，bars=%s，dry_run=%s", len(codes), args.date or "增量", args.bars, args.dry_run)
    failed_codes: list[str] = []
    total_rows = 0

    for index, code in enumerate(codes, start=1):
        try:
            rows = fetch_code_rows(code, args.bars, args.date)
            if not rows:
                logger.info("%s/%s %s 无分时数据", index, len(codes), code)
                continue

            if engine is not None and not args.date and not args.refresh_existing:
                latest_datetime = get_latest_trade_datetime(engine, code)
                if latest_datetime:
                    rows = [row for row in rows if row["datetime"] > latest_datetime]

            if not rows:
                logger.info("%s/%s %s 无新分时数据", index, len(codes), code)
                continue

            db_rows = to_db_rows(rows)
            if not args.dry_run and engine is not None:
                upsert_intraday_rows(engine, db_rows)

            total_rows += len(db_rows)
            logger.info(
                "%s/%s %s 写入 %s 条，范围 %s 至 %s",
                index,
                len(codes),
                code,
                len(db_rows),
                db_rows[0]["trade_datetime"],
                db_rows[-1]["trade_datetime"],
            )
        except Exception as exc:  # noqa: BLE001
            failed_codes.append(code)
            logger.exception("%s/%s %s 同步失败: %s", index, len(codes), code, exc)
        finally:
            time.sleep(random.uniform(args.sleep_min, args.sleep_max))

    write_failed_codes(failed_codes)
    logger.info("分时同步结束，成功写入/统计 %s 条，失败 %s 只", total_rows, len(failed_codes))


if __name__ == "__main__":
    main()
