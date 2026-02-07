"""不祥事・第三者委員会関連リリース検出ツール

日別CSVの表題をClaude APIで判定し、該当案件のPDFをダウンロードして
JSONファイルに累積保存する。
"""

import argparse
import csv
import io
import json
import os
import re
import sys
import time
from datetime import date
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

from dotenv import load_dotenv

import anthropic
import requests

# --- 定数 ---
MODEL = "claude-haiku-4-5-20251001"
OUTPUT_DIR = Path(__file__).parent / "output"
ALERTS_DIR = Path(__file__).parent / "alerts"
ALERTS_JSON = ALERTS_DIR / "scandal_alerts.json"
PDF_DIR = ALERTS_DIR / "pdf"
REQUEST_INTERVAL = 1.0  # PDFダウンロード間隔（秒）
MAX_RETRIES = 3

SYSTEM_PROMPT = """\
あなたは日本の上場企業の適時開示情報を分析する専門家です。
以下の表題一覧から、不祥事・コンプライアンス問題に**明確に**関連するものだけを特定してください。

## 該当する基準（これらに明確に該当するもののみ）
- 第三者委員会・特別調査委員会・調査委員会の設置や調査報告
- 不正行為・不適切な会計処理・粉飾決算の判明
- 横領・着服・資金流用
- 行政処分・業務改善命令・業務停止命令
- 課徴金・金融庁処分
- コンプライアンス違反・法令違反
- 不正に関する再発防止策・改善報告書・改善状況報告書の提出
- 役員の懲戒処分
- 過年度決算の訂正（不正会計に起因するもの）
- 不正に関する社内調査・外部調査の委嘱
- 不適切事案に関する経過報告

## 該当しないもの（必ず除外）
- 自己株式の処分・取得（株式報酬、持株会向け等）
- 第三者割当による株式発行・処分
- 剰余金の処分・固定資産の処分
- 政策保有株式の処分
- 上場維持基準の改善期間に関するもの
- 事業構造改善・体質改善（経営改善施策）
- 通常の決算短信・決算説明資料の訂正（単純な数値誤り・記載ミスの訂正）
- 決算発表の延期（不正が明示されていない場合）
- レビュー結論不表明（監査手続上の事項）
- 通常の訴訟（訴訟提起・和解のみで不祥事が明示されていないもの）
- 「一部報道の件」（不祥事の具体的内容が表題に含まれていない場合）

## 重要な注意事項
- 迷った場合は**除外**してください。明確に不祥事と分かるもののみ返してください。
- 判定理由には「不正ではない」「コンプライアンス問題ではない」と記載するようなものは絶対に含めないでください。

## 回答形式
JSON配列で回答してください。該当する表題の番号と判定理由を含めてください。
該当なしの場合は空配列 [] を返してください。
例: [{"番号": 3, "判定理由": "第三者委員会の設置"}]
"""


def get_api_key() -> str:
    """.envファイルからAPIキーを取得する。"""
    load_dotenv()
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        print(
            "エラー: ANTHROPIC_API_KEY が .env または環境変数に設定されていません。",
            file=sys.stderr,
        )
        sys.exit(1)
    return key


def load_alerts() -> list[dict]:
    """既存のアラートJSONを読み込む。ファイルがなければ空リスト。"""
    if not ALERTS_JSON.exists():
        return []
    with open(ALERTS_JSON, encoding="utf-8") as f:
        return json.load(f)


def save_alerts(alerts: list[dict]) -> None:
    """アラートJSONを保存する。"""
    ALERTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(ALERTS_JSON, "w", encoding="utf-8") as f:
        json.dump(alerts, f, ensure_ascii=False, indent=2)


def get_processed_dates(alerts: list[dict]) -> set[str]:
    """処理済み日付の一覧を取得する。日時フィールドからYYYYMMDD形式で抽出。"""
    dates: set[str] = set()
    for alert in alerts:
        dt = alert.get("日時", "")
        # "2026/01/13 08:30" → "20260113"
        match = re.match(r"(\d{4})/(\d{2})/(\d{2})", dt)
        if match:
            dates.add(match.group(1) + match.group(2) + match.group(3))
    return dates


def read_csv(csv_path: Path) -> list[dict]:
    """CSVファイルを読み込む。"""
    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return list(reader)


def detect_scandals(
    records: list[dict], client: anthropic.Anthropic
) -> list[dict]:
    """Claude APIで表題を判定し、該当レコードを返す。"""
    if not records:
        return []

    # 表題一覧を番号付きで作成
    lines = []
    for i, rec in enumerate(records, 1):
        lines.append(f"{i}. {rec['表題']}")
    user_message = "以下の表題一覧を判定してください。\n" + "\n".join(lines)

    response = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
    )

    # レスポンスからJSON部分を抽出
    response_text = response.content[0].text
    matches = _extract_json_array(response_text)

    results = []
    for match in matches:
        idx = match.get("番号")
        reason = match.get("判定理由", "")
        if idx is None or not (1 <= idx <= len(records)):
            continue
        rec = records[idx - 1]
        results.append({
            "日時": rec["日時"],
            "コード": rec["コード"],
            "会社名": rec["会社名"],
            "表題": rec["表題"],
            "PDF_URL": rec["PDF_URL"],
            "pdf_path": "",
            "判定理由": reason,
            "要約": "",
            "検出日": date.today().isoformat(),
        })
    return results


def _extract_json_array(text: str) -> list[dict]:
    """テキストからJSON配列を抽出する。"""
    # コードブロック内のJSONを優先
    code_block = re.search(r"```(?:json)?\s*(\[.*?])\s*```", text, re.DOTALL)
    if code_block:
        try:
            return json.loads(code_block.group(1))
        except json.JSONDecodeError:
            pass

    # テキスト全体から配列を探す
    bracket_match = re.search(r"\[.*]", text, re.DOTALL)
    if bracket_match:
        try:
            return json.loads(bracket_match.group(0))
        except json.JSONDecodeError:
            pass

    return []


def download_pdf(url: str, save_path: Path) -> bool:
    """PDFをダウンロードする。既に存在する場合はスキップ。"""
    if save_path.exists():
        return True

    save_path.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            with open(save_path, "wb") as f:
                f.write(resp.content)
            return True
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait = 2**attempt
                print(f"  PDFダウンロード リトライ中 ({attempt + 1}/{MAX_RETRIES}): {e}")
                time.sleep(wait)
            else:
                print(f"  警告: PDFのダウンロードに失敗しました: {url}")
                print(f"    {e}")
                return False
    return False


def _make_pdf_filename(date_str: str, code: str, seq: int) -> str:
    """PDF保存用のファイル名を生成する。"""
    return f"{date_str}_{code}_{seq:03d}.pdf"


def process_date(date_str: str, client: anthropic.Anthropic) -> list[dict]:
    """指定日付のCSVを読み込み、不祥事判定→PDFダウンロードを行う。"""
    csv_path = OUTPUT_DIR / f"tdnet_{date_str}.csv"
    if not csv_path.exists():
        print(f"  CSVが見つかりません: {csv_path}")
        return []

    records = read_csv(csv_path)
    if not records:
        print(f"  データなし: {date_str}")
        return []

    print(f"  {len(records)} 件の表題をAIで判定中...")
    detected = detect_scandals(records, client)

    if not detected:
        print(f"  該当なし")
        return []

    print(f"  {len(detected)} 件検出。PDFをダウンロード中...")

    # 同一日付・同一コードの連番管理
    code_counter: dict[str, int] = {}
    for alert in detected:
        code = alert["コード"]
        code_counter[code] = code_counter.get(code, 0) + 1
        seq = code_counter[code]

        pdf_filename = _make_pdf_filename(date_str, code, seq)
        pdf_path = PDF_DIR / pdf_filename

        if alert["PDF_URL"]:
            if download_pdf(alert["PDF_URL"], pdf_path):
                alert["pdf_path"] = str(pdf_path.relative_to(Path(__file__).parent))
            time.sleep(REQUEST_INTERVAL)

    return detected


def main():
    parser = argparse.ArgumentParser(
        description="TDnet 不祥事・第三者委員会関連リリースを検出する"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--date",
        help="判定対象日（YYYYMMDD）",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="output/ 内の全CSVを判定（処理済みはスキップ）",
    )
    args = parser.parse_args()

    api_key = get_api_key()
    client = anthropic.Anthropic(api_key=api_key)

    # 既存アラートを読み込み
    alerts = load_alerts()
    processed_dates = get_processed_dates(alerts)

    if args.date:
        date_str = args.date.replace("-", "").replace("/", "")
        if date_str in processed_dates:
            print(f"{date_str} は処理済みです。スキップします。")
            return
        print(f"日付: {date_str}")
        new_alerts = process_date(date_str, client)
        if new_alerts:
            alerts.extend(new_alerts)
            save_alerts(alerts)
            print(f"\n完了: {len(new_alerts)} 件を {ALERTS_JSON} に追加しました。")
        else:
            # 該当なしでも処理済みとして記録（ダミーエントリ）
            alerts.append({
                "日時": f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:8]}",
                "コード": "",
                "会社名": "",
                "表題": "",
                "PDF_URL": "",
                "pdf_path": "",
                "判定理由": "_処理済み（該当なし）",
                "要約": "",
                "検出日": date.today().isoformat(),
            })
            save_alerts(alerts)
            print(f"\n完了: 該当案件なし。")
    else:
        # --all: output/ 内の全CSVを処理
        csv_files = sorted(OUTPUT_DIR.glob("tdnet_*.csv"))
        if not csv_files:
            print("CSVファイルが見つかりません。")
            return

        total_new = 0
        for csv_file in csv_files:
            date_str = csv_file.stem.replace("tdnet_", "")
            if date_str in processed_dates:
                print(f"[スキップ] {date_str} - 処理済み")
                continue

            print(f"\n{'=' * 50}")
            print(f"[判定] {date_str}")
            print(f"{'=' * 50}")

            new_alerts = process_date(date_str, client)
            if new_alerts:
                alerts.extend(new_alerts)
                total_new += len(new_alerts)
            else:
                # 該当なしでも処理済みとして記録
                alerts.append({
                    "日時": f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:8]}",
                    "コード": "",
                    "会社名": "",
                    "表題": "",
                    "PDF_URL": "",
                    "pdf_path": "",
                    "判定理由": "_処理済み（該当なし）",
                    "要約": "",
                    "検出日": date.today().isoformat(),
                })

            # 都度保存（中断対策）
            save_alerts(alerts)

        print(f"\n{'=' * 50}")
        print(f"全日付の判定完了: 新規 {total_new} 件を検出しました。")
        print(f"結果: {ALERTS_JSON}")
        print(f"{'=' * 50}")


if __name__ == "__main__":
    main()
