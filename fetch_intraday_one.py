#!/usr/bin/env python3
"""获取单只股票指定交易日的 1 分钟分时数据。"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen

TENCENT_M1_URL = "https://ifzq.gtimg.cn/appstock/app/kline/mkline"
REQUEST_TIMEOUT = 10
MAX_RETRIES = 3

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Referer": "https://finance.sina.com.cn/",
}

CSV_COLUMNS = [
    "code",
    "name",
    "date",
    "time",
    "datetime",
    "open",
    "close",
    "high",
    "low",
    "volume_hand",
    "turnover_rate_pct",
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


def to_float(value: str | None) -> float | None:
    if value in (None, "", "-", "--"):
        return None
    return float(value)


def http_get_text(url: str, params: dict[str, str | int]) -> str:
    query = "&".join(
        f"{quote(str(key), safe='')}"
        f"={quote(str(value), safe=',.-_~')}"
        for key, value in params.items()
    )
    full_url = f"{url}?{query}"
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        req = Request(full_url, headers=HEADERS)
        try:
            with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                return resp.read().decode("utf-8")
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt == MAX_RETRIES:
                break
            time.sleep(0.6 * attempt)

    try:
        result = subprocess.run(
            [
                "curl",
                "-s",
                "-H",
                f"User-Agent: {HEADERS['User-Agent']}",
                "-H",
                f"Referer: {HEADERS['Referer']}",
                full_url,
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=REQUEST_TIMEOUT,
        )
        if result.stdout.strip():
            return result.stdout
    except Exception as exc:  # noqa: BLE001
        last_error = exc

    raise RuntimeError(f"请求失败: {full_url}") from last_error


def parse_jsonp_text(raw_text: str) -> dict:
    text_value = raw_text.strip()
    if not text_value:
        raise ValueError("接口返回为空")
    if "=" in text_value:
        _, text_value = text_value.split("=", 1)
    return json.loads(text_value)


def fetch_intraday_trends(code: str, bars: int = 32000) -> dict:
    code = normalize_code(code)
    params = {
        "param": f"{get_market_prefix(code)}{code},m1,,{bars}",
        "_var": "m1_today",
        "r": f"{time.time():.9f}",
    }
    payload = parse_jsonp_text(http_get_text(TENCENT_M1_URL, params))
    data = payload.get("data") or {}
    quote_key = f"{get_market_prefix(code)}{code}"
    stock_block = data.get(quote_key) or {}
    minute_rows = stock_block.get("m1") or []
    if not minute_rows:
        raise ValueError(f"腾讯分时接口返回为空: {code}")
    return {
        "code": code,
        "name": (stock_block.get("qt") or {}).get(quote_key, ["", ""])[1],
        "rows": minute_rows,
    }


def parse_minute_timestamp(ts_text: str) -> datetime:
    return datetime.strptime(ts_text, "%Y%m%d%H%M")


def parse_trends(code: str, name: str, trends: list[list]) -> list[dict]:
    rows = []
    for line in trends:
        if len(line) < 8:
            raise ValueError(f"分时行字段数异常: {line}")

        dt_text, open_p, close_p, high_p, low_p, volume_hand, _unused, turnover_rate_pct = line[:8]
        dt_obj = parse_minute_timestamp(dt_text)
        rows.append(
            {
                "code": code,
                "name": name,
                "date": dt_obj.strftime("%Y-%m-%d"),
                "time": dt_obj.strftime("%H:%M"),
                "datetime": dt_obj.strftime("%Y-%m-%d %H:%M"),
                "open": to_float(open_p),
                "close": to_float(close_p),
                "high": to_float(high_p),
                "low": to_float(low_p),
                "volume_hand": to_float(volume_hand),
                "turnover_rate_pct": to_float(turnover_rate_pct),
            }
        )
    return rows


def filter_rows_by_date(rows: list[dict], target_date: str) -> list[dict]:
    return [row for row in rows if row["date"] == target_date]


def build_default_output(code: str, target_date: str, fmt: str) -> Path:
    suffix = "json" if fmt == "json" else "csv"
    return Path(f"{normalize_code(code)}_{target_date}_1m.{suffix}")


def write_csv(rows: list[dict], output_path: Path) -> None:
    with output_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def write_json(rows: list[dict], output_path: Path, meta: dict) -> None:
    payload = {
        "meta": meta,
        "rows": rows,
    }
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def print_summary(meta: dict, rows: list[dict], output_path: Path) -> None:
    total_volume = sum(row["volume_hand"] or 0 for row in rows)
    day_high = max(row["high"] for row in rows if row["high"] is not None)
    day_low = min(row["low"] for row in rows if row["low"] is not None)
    print(f"代码: {meta['code']}")
    print(f"名称: {meta['name']}")
    print(f"日期: {meta['date']}")
    print(f"分钟条数: {len(rows)}")
    print(f"首条: {rows[0]['datetime']}  末条: {rows[-1]['datetime']}")
    print(f"区间高低: {day_high:.2f} / {day_low:.2f}")
    print(f"成交量(手): {total_volume:.0f}")
    print(f"输出文件: {output_path}")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="获取单只股票指定交易日的 1 分钟分时数据")
    parser.add_argument("code", help="股票代码，支持 688566 / sh.688566 / sz.000001")
    parser.add_argument(
        "--date",
        required=True,
        help="目标交易日，格式 YYYY-MM-DD。",
    )
    parser.add_argument(
        "--format",
        choices=("csv", "json"),
        default="csv",
        help="输出格式，默认 csv",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="输出文件路径，默认写到当前目录",
    )
    parser.add_argument("--bars", type=int, default=32000, help="向腾讯接口请求的 1 分钟K线条数，默认 32000")
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError as exc:
        raise SystemExit(f"日期格式错误: {args.date}") from exc

    code = normalize_code(args.code)
    data = fetch_intraday_trends(code, bars=max(240, args.bars))
    rows = parse_trends(code, data.get("name", ""), data["rows"])
    day_rows = filter_rows_by_date(rows, args.date)

    if not day_rows:
        raise SystemExit(
            f"未找到 {code} 在 {args.date} 的分时数据。"
            "可尝试调大 --bars，或者改成盘中定时落库保存。"
        )

    output_path = Path(args.output) if args.output else build_default_output(code, args.date, args.format)
    meta = {
        "code": code,
        "name": data.get("name", ""),
        "date": args.date,
        "market": get_market_prefix(code).upper(),
        "source": "tencent mkline m1",
    }

    if args.format == "json":
        write_json(day_rows, output_path, meta)
    else:
        write_csv(day_rows, output_path)

    print_summary(meta, day_rows, output_path)


if __name__ == "__main__":
    main()
