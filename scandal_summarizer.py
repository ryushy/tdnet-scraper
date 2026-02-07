"""不祥事リリースPDF要約ツール

scandal_alerts.json 内の未要約レコードについて、
ダウンロード済みPDFからテキストを抽出し、Claude APIで要約を生成する。
"""

import argparse
import io
import json
import os
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

from dotenv import load_dotenv

import anthropic
import pymupdf

# --- 定数 ---
MODEL = "claude-haiku-4-5-20251001"
BASE_DIR = Path(__file__).parent
ALERTS_JSON = BASE_DIR / "alerts" / "scandal_alerts.json"

SUMMARY_PROMPT = """\
この適時開示資料の内容を以下の観点で要約してください:
1. 何が起きたか（不祥事・問題の概要）
2. 関与者（誰が関与したか）
3. 影響範囲（金額、期間、対象範囲）
4. 会社の対応（第三者委員会設置、再発防止策等）
5. 今後の見通し

300字以内で簡潔にまとめてください。
"""

# PDFテキスト抽出の最大文字数（APIコスト制御）
MAX_TEXT_LENGTH = 50000


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
    """アラートJSONを読み込む。"""
    if not ALERTS_JSON.exists():
        print("エラー: アラートファイルが見つかりません。", file=sys.stderr)
        print(f"  先に scandal_detector.py を実行してください。", file=sys.stderr)
        sys.exit(1)
    with open(ALERTS_JSON, encoding="utf-8") as f:
        return json.load(f)


def save_alerts(alerts: list[dict]) -> None:
    """アラートJSONを保存する。"""
    with open(ALERTS_JSON, "w", encoding="utf-8") as f:
        json.dump(alerts, f, ensure_ascii=False, indent=2)


def extract_text_from_pdf(pdf_path: Path) -> str:
    """PDFからテキストを抽出する。"""
    doc = pymupdf.open(pdf_path)
    text_parts = []
    for page in doc:
        text_parts.append(page.get_text())
    doc.close()

    full_text = "\n".join(text_parts)
    # 長すぎる場合は先頭部分のみ
    if len(full_text) > MAX_TEXT_LENGTH:
        full_text = full_text[:MAX_TEXT_LENGTH] + "\n\n（以下省略）"
    return full_text


def summarize_text(
    text: str, title: str, company: str, client: anthropic.Anthropic
) -> str:
    """Claude APIでPDFテキストを要約する。"""
    user_message = (
        f"会社名: {company}\n表題: {title}\n\n"
        f"--- 開示資料本文 ---\n{text}"
    )

    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=SUMMARY_PROMPT,
        messages=[{"role": "user", "content": user_message}],
    )
    return response.content[0].text


def main():
    parser = argparse.ArgumentParser(
        description="不祥事リリースPDFの要約を生成する"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="処理件数の上限（0で全件処理、デフォルト: 0）",
    )
    args = parser.parse_args()

    api_key = get_api_key()
    client = anthropic.Anthropic(api_key=api_key)

    alerts = load_alerts()

    # 未要約かつ実データ（ダミーでない）レコードを抽出
    unsummarized = [
        (i, alert)
        for i, alert in enumerate(alerts)
        if not alert.get("要約")
        and alert.get("pdf_path")
        and alert.get("コード")  # ダミーエントリ除外
    ]

    if not unsummarized:
        print("未要約のレコードはありません。")
        return

    limit = args.limit if args.limit > 0 else len(unsummarized)
    targets = unsummarized[:limit]

    print(f"未要約: {len(unsummarized)} 件 / 今回処理: {len(targets)} 件\n")

    done = 0
    for idx, (alert_idx, alert) in enumerate(targets, 1):
        pdf_path = BASE_DIR / alert["pdf_path"]

        print(f"[{idx}/{len(targets)}] {alert['会社名']} - {alert['表題'][:40]}...")

        if not pdf_path.exists():
            print(f"  警告: PDFが見つかりません: {pdf_path}")
            continue

        # テキスト抽出
        text = extract_text_from_pdf(pdf_path)
        if not text.strip():
            print(f"  警告: PDFからテキストを抽出できませんでした")
            alert["要約"] = "（テキスト抽出不可）"
            save_alerts(alerts)
            continue

        # 要約生成
        try:
            summary = summarize_text(
                text, alert["表題"], alert["会社名"], client
            )
            alerts[alert_idx]["要約"] = summary
            done += 1
            print(f"  要約完了（{len(summary)}文字）")
        except anthropic.APIError as e:
            print(f"  エラー: API呼び出しに失敗しました: {e}")
            continue

        # 都度保存（中断対策）
        save_alerts(alerts)

    print(f"\n完了: {done}/{len(targets)} 件の要約を生成しました。")
    print(f"結果: {ALERTS_JSON}")


if __name__ == "__main__":
    main()
