from __future__ import annotations

import argparse
import os
import random
import re
import time
from dataclasses import dataclass
from typing import Iterable

import requests
from bs4 import BeautifulSoup
from requests import Session
from sqlalchemy import create_engine, text

THS_CONCEPT_URL = "https://basic.10jqka.com.cn/{code}/concept.html"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "InsightOne123456"),
    "database": os.getenv("MYSQL_DB", "stock_db_qfq"),
}


@dataclass
class ThsThemeInfo:
    code: str
    ths_industry_name: str
    ths_industry_code: str
    theme_tags: list[str]


def create_db_engine():
    db_uri = (
        f"mysql+mysqlconnector://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}"
        f"@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}"
    )
    return create_engine(db_uri, pool_pre_ping=True)


def ensure_theme_table(engine) -> None:
    ddl = text(
        """
        CREATE TABLE IF NOT EXISTS stock_theme_labels (
            code VARCHAR(10) NOT NULL PRIMARY KEY,
            ths_industry_name VARCHAR(50) NULL,
            ths_industry_code VARCHAR(20) NULL,
            ths_theme_tags TEXT NULL,
            ths_theme_count INT NOT NULL DEFAULT 0,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    with engine.begin() as conn:
        conn.execute(ddl)


def build_code_like(code_prefix: str) -> str:
    prefix = code_prefix.strip()
    return f"{prefix}%" if prefix else "%"


def load_target_codes(
    engine,
    *,
    codes: list[str] | None,
    code_prefix: str,
    only_missing: bool,
    limit: int | None,
) -> list[str]:
    if codes:
        return [normalize_code(code) for code in codes]

    where_missing = ""
    if only_missing:
        where_missing = """
          AND (
              l.code IS NULL
              OR l.ths_theme_tags IS NULL
              OR TRIM(l.ths_theme_tags) = ''
              OR l.ths_theme_count = 0
          )
        """
    limit_clause = "LIMIT :limit_value" if limit else ""
    query = text(
        f"""
        SELECT DISTINCT i.code
        FROM stock_info i
        LEFT JOIN stock_theme_labels l ON l.code = i.code
        WHERE i.code LIKE :code_like
        {where_missing}
        ORDER BY i.code
        {limit_clause}
        """
    )
    params = {"code_like": build_code_like(code_prefix)}
    if limit:
        params["limit_value"] = int(limit)
    with engine.connect() as conn:
        rows = conn.execute(query, params).fetchall()
    return [normalize_code(row[0]) for row in rows]


def normalize_code(code: str | int) -> str:
    digits = re.sub(r"\D", "", str(code))
    if len(digits) < 6:
        digits = digits.zfill(6)
    return digits[-6:]


def fetch_concept_html(session: Session, code: str, timeout: float) -> str:
    response = session.get(THS_CONCEPT_URL.format(code=code), timeout=timeout)
    response.raise_for_status()
    response.encoding = "gbk"
    return response.text


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def parse_theme_tags_from_table(soup: BeautifulSoup) -> list[str]:
    tags: list[str] = []
    seen: set[str] = set()

    table = soup.find("table", class_="gnContent")
    if table is None:
        return tags

    for row in table.find_all("tr"):
        cells = [clean_text(cell.get_text(" ", strip=True)) for cell in row.find_all(["td", "th"])]
        if len(cells) < 4 or not cells[0].isdigit():
            continue
        name = cells[1]
        if name and name not in seen:
            seen.add(name)
            tags.append(name)
    return tags


def parse_theme_tags_from_compare(soup: BeautifulSoup) -> list[str]:
    heading = soup.find("h2", string=lambda value: value and "概念对比" in value)
    if heading is None:
        return []

    container = heading.find_next("div", class_="gntc")
    if container is None:
        return []

    tags: list[str] = []
    seen: set[str] = set()
    for link in container.find_all("a"):
        name = clean_text(link.get_text(" ", strip=True))
        if not name or name in {"上一页", "下一页"} or name in seen:
            continue
        seen.add(name)
        tags.append(name)
    return tags


def merge_tags(primary: Iterable[str], fallback: Iterable[str]) -> list[str]:
    tags: list[str] = []
    seen: set[str] = set()
    for name in list(primary) + list(fallback):
        cleaned = clean_text(name)
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            tags.append(cleaned)
    return tags


def parse_ths_concept_page(code: str, html: str) -> ThsThemeInfo:
    soup = BeautifulSoup(html, "lxml")
    table_tags = parse_theme_tags_from_table(soup)
    compare_tags = parse_theme_tags_from_compare(soup)
    tags = merge_tags(table_tags, compare_tags)
    return ThsThemeInfo(
        code=normalize_code(code),
        ths_industry_name="",
        ths_industry_code="",
        theme_tags=tags,
    )


def upsert_theme_info(engine, info: ThsThemeInfo, *, overwrite_empty_only: bool) -> None:
    tags_text = ",".join(info.theme_tags)
    if overwrite_empty_only:
        query = text(
            """
            INSERT INTO stock_theme_labels
                (code, ths_industry_name, ths_industry_code, ths_theme_tags, ths_theme_count)
            VALUES
                (:code, :industry_name, :industry_code, :theme_tags, :theme_count)
            ON DUPLICATE KEY UPDATE
                ths_theme_count = IF(
                    ths_theme_tags IS NULL OR TRIM(ths_theme_tags) = '' OR ths_theme_count = 0,
                    VALUES(ths_theme_count),
                    ths_theme_count
                ),
                ths_theme_tags = IF(
                    ths_theme_tags IS NULL OR TRIM(ths_theme_tags) = '' OR ths_theme_count = 0,
                    VALUES(ths_theme_tags),
                    ths_theme_tags
                ),
                ths_industry_name = IF(
                    ths_industry_name IS NULL OR TRIM(ths_industry_name) = '',
                    VALUES(ths_industry_name),
                    ths_industry_name
                ),
                ths_industry_code = IF(
                    ths_industry_code IS NULL OR TRIM(ths_industry_code) = '',
                    VALUES(ths_industry_code),
                    ths_industry_code
                )
            """
        )
    else:
        query = text(
            """
            INSERT INTO stock_theme_labels
                (code, ths_industry_name, ths_industry_code, ths_theme_tags, ths_theme_count)
            VALUES
                (:code, :industry_name, :industry_code, :theme_tags, :theme_count)
            ON DUPLICATE KEY UPDATE
                ths_industry_name = VALUES(ths_industry_name),
                ths_industry_code = VALUES(ths_industry_code),
                ths_theme_tags = VALUES(ths_theme_tags),
                ths_theme_count = VALUES(ths_theme_count)
            """
        )
    params = {
        "code": info.code,
        "industry_name": info.ths_industry_name,
        "industry_code": info.ths_industry_code,
        "theme_tags": tags_text,
        "theme_count": len(info.theme_tags),
    }
    with engine.begin() as conn:
        conn.execute(query, params)


def build_session() -> Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": DEFAULT_USER_AGENT,
            "Referer": "https://basic.10jqka.com.cn/",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }
    )
    return session


def sync_themes(args: argparse.Namespace) -> None:
    engine = create_db_engine()
    ensure_theme_table(engine)
    codes = load_target_codes(
        engine,
        codes=args.codes,
        code_prefix=args.code_prefix,
        only_missing=args.only_missing,
        limit=args.limit,
    )
    if not codes:
        print("没有需要补充的股票。")
        return

    session = build_session()
    success = 0
    empty = 0
    failed = 0
    print(f"准备抓取 {len(codes)} 只股票的同花顺F10概念。")

    for index, code in enumerate(codes, start=1):
        try:
            html = fetch_concept_html(session, code, timeout=args.timeout)
            info = parse_ths_concept_page(code, html)
            if not info.theme_tags:
                empty += 1
                print(f"[{index}/{len(codes)}] {code} 未解析到概念")
            else:
                success += 1
                preview = "、".join(info.theme_tags[:8])
                suffix = "..." if len(info.theme_tags) > 8 else ""
                print(f"[{index}/{len(codes)}] {code} {len(info.theme_tags)}个: {preview}{suffix}")
                if not args.dry_run:
                    upsert_theme_info(engine, info, overwrite_empty_only=args.only_missing)
        except Exception as exc:
            failed += 1
            print(f"[{index}/{len(codes)}] {code} 失败: {type(exc).__name__}: {exc}")

        if index < len(codes):
            delay = args.sleep + random.uniform(0, args.jitter)
            time.sleep(max(0.0, delay))

    mode = "预览" if args.dry_run else "写入"
    print(f"{mode}完成: 成功 {success}, 空结果 {empty}, 失败 {failed}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="从同花顺F10补充本地股票概念标签")
    parser.add_argument("--codes", nargs="+", help="指定股票代码，例如 688371 688507")
    parser.add_argument("--code-prefix", default="688", help="默认只处理688开头股票")
    parser.add_argument("--limit", type=int, help="限制处理数量，便于试跑")
    parser.add_argument(
        "--only-missing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="只补充缺失/空概念，默认开启",
    )
    parser.add_argument("--sleep", type=float, default=10.0, help="每只股票之间的基础间隔秒数")
    parser.add_argument("--jitter", type=float, default=3.0, help="随机额外间隔秒数")
    parser.add_argument("--timeout", type=float, default=12.0, help="HTTP超时时间")
    parser.add_argument("--dry-run", action="store_true", help="只打印不写库")
    return parser.parse_args()


if __name__ == "__main__":
    sync_themes(parse_args())
