"""TDnet 適時開示情報スクレイピングツール

日付を指定して、TDnet（適時開示情報閲覧サービス）から
開示情報一覧を取得し、CSVファイルとして保存する。
"""

import argparse
import csv
import io
import math
import re
import sys
import time
from datetime import datetime
from pathlib import Path

# Windows環境での文字化け対策: 標準出力/エラーをUTF-8に設定
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.release.tdnet.info/inbs/"
LIST_URL_TEMPLATE = BASE_URL + "I_list_{page:03d}_{date}.html"
REQUEST_INTERVAL = 1.0  # リクエスト間隔（秒）
MAX_RETRIES = 3
ITEMS_PER_PAGE = 100


def parse_date(date_str: str) -> str:
    """日付文字列をYYYYMMDD形式に正規化する。"""
    # ハイフン除去
    cleaned = date_str.replace("-", "").replace("/", "")
    try:
        datetime.strptime(cleaned, "%Y%m%d")
    except ValueError:
        print(f"エラー: 不正な日付形式です: {date_str}", file=sys.stderr)
        print("  YYYYMMDD または YYYY-MM-DD 形式で指定してください。", file=sys.stderr)
        sys.exit(1)
    return cleaned


def format_date(date_str: str) -> str:
    """YYYYMMDD形式の日付をYYYY/MM/DD形式に変換する。"""
    return f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:8]}"


def fetch_page(url: str) -> requests.Response:
    """URLからHTMLを取得する。リトライ付き。"""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding
            return resp
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait = 2 ** attempt
                print(f"  リトライ中 ({attempt + 1}/{MAX_RETRIES}): {e}", file=sys.stderr)
                time.sleep(wait)
            else:
                print(f"エラー: ページの取得に失敗しました: {url}", file=sys.stderr)
                print(f"  {e}", file=sys.stderr)
                sys.exit(1)


def parse_total_count(soup: BeautifulSoup) -> int:
    """ページから全件数を取得する。"""
    pager = soup.find("td", class_="pagerTd")
    if not pager:
        return 0
    text = pager.get_text(strip=True)
    match = re.search(r"全(\d+)件", text)
    if match:
        return int(match.group(1))
    return 0


def is_no_data(soup: BeautifulSoup) -> bool:
    """「開示された情報はありません」かどうかを判定する。"""
    text = soup.get_text()
    return "に開示された情報はありません" in text


def _cell_text(row, class_name: str) -> str:
    """行からセルを探してテキストを返す。見つからなければ空文字列。"""
    cell = row.find("td", class_=class_name)
    return cell.get_text(strip=True) if cell else ""


def _cell_link(row, class_name: str) -> str:
    """行からセルを探してリンクURLを返す。見つからなければ空文字列。"""
    cell = row.find("td", class_=class_name)
    if not cell:
        return ""
    a_tag = cell.find("a")
    if a_tag and a_tag.get("href"):
        return BASE_URL + a_tag["href"]
    return ""


def parse_rows(soup: BeautifulSoup, date_str: str) -> list[dict]:
    """HTMLテーブルからデータ行をパースする。"""
    table = soup.find("table", id="main-list-table")
    if not table:
        return []

    date_formatted = format_date(date_str)
    records = []

    for row in table.find_all("tr", recursive=False):
        time_text = _cell_text(row, "kjTime")
        if not time_text:
            continue

        records.append({
            "日時": f"{date_formatted} {time_text}",
            "コード": _cell_text(row, "kjCode"),
            "会社名": _cell_text(row, "kjName"),
            "表題": _cell_text(row, "kjTitle"),
            "PDF_URL": _cell_link(row, "kjTitle"),
            "XBRL_URL": _cell_link(row, "kjXbrl"),
            "上場取引所": _cell_text(row, "kjPlace"),
            "更新履歴": _cell_text(row, "kjHistroy"),  # typo in original HTML
        })

    return records


def save_csv(records: list[dict], output_path: Path) -> None:
    """レコードをCSVファイルに保存する。"""
    fieldnames = ["日時", "コード", "会社名", "表題", "PDF_URL", "XBRL_URL", "上場取引所", "更新履歴"]
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


def main():
    parser = argparse.ArgumentParser(
        description="TDnet 適時開示情報をCSVで取得する"
    )
    parser.add_argument(
        "--date",
        required=True,
        help="取得対象日（YYYYMMDD または YYYY-MM-DD）",
    )
    parser.add_argument(
        "--output",
        default="./output",
        help="CSV出力先ディレクトリ（デフォルト: ./output）",
    )
    args = parser.parse_args()

    date_str = parse_date(args.date)
    output_dir = Path(args.output)

    print(f"日付: {format_date(date_str)}")

    # 1ページ目を取得
    first_url = LIST_URL_TEMPLATE.format(page=1, date=date_str)
    print(f"1ページ目を取得中: {first_url}")
    resp = fetch_page(first_url)
    soup = BeautifulSoup(resp.text, "lxml")

    # データなしチェック
    if is_no_data(soup):
        print("この日に開示された情報はありません。")
        return

    # 全件数・ページ数を取得
    total_count = parse_total_count(soup)
    total_pages = math.ceil(total_count / ITEMS_PER_PAGE)
    print(f"全 {total_count} 件（{total_pages} ページ）")

    # 1ページ目のデータをパース
    all_records = parse_rows(soup, date_str)
    print(f"  1/{total_pages} ページ: {len(all_records)} 件取得")

    # 2ページ目以降を取得
    for page in range(2, total_pages + 1):
        time.sleep(REQUEST_INTERVAL)
        url = LIST_URL_TEMPLATE.format(page=page, date=date_str)
        print(f"  {page}/{total_pages} ページを取得中...")
        resp = fetch_page(url)
        soup = BeautifulSoup(resp.text, "lxml")
        records = parse_rows(soup, date_str)
        all_records.extend(records)
        print(f"  {page}/{total_pages} ページ: {len(records)} 件取得")

    # CSV出力
    output_path = output_dir / f"tdnet_{date_str}.csv"
    save_csv(all_records, output_path)
    print(f"\n完了: {len(all_records)} 件を {output_path} に保存しました。")


if __name__ == "__main__":
    main()
