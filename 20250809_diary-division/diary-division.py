# split_notes.py
import os
import re
import sys
from pathlib import Path

# 入出力
INFILE = Path(sys.argv[1]) if len(sys.argv) >= 2 else Path("notes.txt")
OUTDIR = Path(sys.argv[2]) if len(sys.argv) >= 3 else Path("split_md")

# 日付行の検出（先頭の "- " を許容し、YYYY/MM/DD または YYYY-MM-DD を抽出し、タイトルも取得）
# 例: "- 2024/11/15 転職先の条件" → タイトル部分もキャプチャ
DATE_LINE = re.compile(r"^\s*-\s*(\d{4})[/-](\d{2})[/-](\d{2})(?:\s+(.+))?.*$")

def main():
    if not INFILE.exists():
        raise FileNotFoundError(f"Input not found: {INFILE}")

    OUTDIR.mkdir(parents=True, exist_ok=True)

    entries = []
    current = None  # {"date_key": "YYYY-MM-DD", "lines": [str, ...]}

    with INFILE.open("r", encoding="utf-8") as f:
        for raw in f:
            line = raw.rstrip("\n")
            m = DATE_LINE.match(line)
            if m:
                # 直前の塊を確定
                if current:
                    entries.append(current)
                y, mo, d = m.group(1), m.group(2), m.group(3)
                date_key = f"{y}-{mo}-{d}"
                title = m.group(4)  # Noneまたはタイトル文字列

                # 先頭の "- " を削除した日付行を本文に残す
                clean_date_line = re.sub(r"^\s*-\s*", "", line, count=1)

                current = {"date_key": date_key, "title": title, "lines": [clean_date_line]}
            else:
                if current:
                    current["lines"].append(line)
                # 日付が始まる前の行は無視

    if current:
        entries.append(current)

    # 書き出し（同日複数は _002, _003…）
    counts = {}
    for e in entries:
        base = e["date_key"]
        title = e.get("title")
        # ファイル名生成
        if title:
            base_fname = f"{base} {title}"
        else:
            base_fname = base
        counts[base_fname] = counts.get(base_fname, 0) + 1
        suffix = "" if counts[base_fname] == 1 else f"_{counts[base_fname]:03d}"
        fname = f"{base_fname}{suffix}.md"
        outpath = OUTDIR / fname

        # 末尾に改行を1つ付与（見た目を安定させるため）
        content = "\n".join(e["lines"]).rstrip() + "\n"
        with outpath.open("w", encoding="utf-8") as wf:
            wf.write(content)

    print(f"Done. Wrote {len(entries)} files to {OUTDIR}")

if __name__ == "__main__":
    main()
