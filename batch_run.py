"""TDnetスクレイパーをバッチ実行するスクリプト

指定期間の全日付について tdnet_scraper.py を実行する。
既にCSVが存在する日付はスキップする。
"""

import io
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

START_DATE = date(2025, 12, 30)
END_DATE = date(2026, 2, 6)
OUTPUT_DIR = Path(__file__).parent / "output"
SCRIPT = Path(__file__).parent / "tdnet_scraper.py"


def main():
    current = START_DATE
    total = (END_DATE - START_DATE).days + 1
    done = 0
    skipped = 0
    failed = 0

    while current <= END_DATE:
        date_str = current.strftime("%Y%m%d")
        csv_path = OUTPUT_DIR / f"tdnet_{date_str}.csv"

        if csv_path.exists():
            print(f"[スキップ] {current} - 既にCSVが存在します: {csv_path.name}")
            skipped += 1
            current += timedelta(days=1)
            continue

        print(f"\n{'=' * 60}")
        print(f"[実行] {current} ({done + skipped + failed + 1}/{total})")
        print(f"{'=' * 60}")

        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--date", date_str],
            encoding="utf-8",
            errors="replace",
        )

        if result.returncode == 0:
            done += 1
        else:
            print(f"[警告] {current} の取得に失敗しました (終了コード: {result.returncode})")
            failed += 1

        current += timedelta(days=1)

    print(f"\n{'=' * 60}")
    print(f"バッチ処理完了:")
    print(f"  成功: {done} 日")
    print(f"  スキップ: {skipped} 日")
    print(f"  失敗: {failed} 日")
    print(f"  合計: {total} 日")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
