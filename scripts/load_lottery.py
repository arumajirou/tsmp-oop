#!/usr/bin/env python3
import argparse, re, sys, json
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values

# 既定のCSVエンドポイント
DEFAULT_URLS = {
    "mini":     "https://loto-life.net/csv/mini",
    "loto6":    "https://loto-life.net/csv/loto6",
    "loto7":    "https://loto-life.net/csv/loto7",
    "bingo5":   "https://loto-life.net/csv/bingo5",
    "numbers3": "https://loto-life.net/csv/numbers3",
    "numbers4": "https://loto-life.net/csv/numbers4",
}

# 緩めの列名マッチャ
RE_MAIN   = re.compile(r"^(?:第)?(\d+)\s*数字$", re.IGNORECASE)     # 第1数字, 第2数字...
RE_MAIN_2 = re.compile(r"^数字\s*(\d+)$", re.IGNORECASE)            # 数字1...
RE_KETA   = re.compile(r"^(\d+)\s*桁目$", re.IGNORECASE)            # 1桁目...
RE_BONUS1 = re.compile(r"^ボーナス\s*数字\s*1?$", re.IGNORECASE)     # ボーナス数字 or ボーナス数字1
RE_BONUS2 = re.compile(r"^ボーナス\s*数字\s*2$", re.IGNORECASE)      # ボーナス数字2

def _digits_only(s: str) -> str:
    """非数字を除去し、残りの数字を返す（="0097" 等に対応）"""
    if s is None:
        return ""
    return re.sub(r"\D", "", str(s))

def normalize(df: pd.DataFrame, game: str):
    """
    共通正規化。numbers3/4 は '抽選数字' を桁分割して n1..nK を作る特別処理。
    それ以外は従来通り、列名をマッチングして n1..n7 / b1..b2 を組み立てる。
    """
    # numbers3 / numbers4 の特別処理（抽選数字 → 桁分割）
    if game in ("numbers3", "numbers4") and ("抽選数字" in df.columns) and ("開催日" in df.columns):
        digits = 3 if game == "numbers3" else 4
        df2 = pd.DataFrame(index=df.index).copy()

        # ds / round
        df2["ds"] = pd.to_datetime(df["開催日"], errors="coerce").dt.date
        if df2["ds"].isna().any():
            bad = df2[df2["ds"].isna()].index[:3].tolist()
            raise RuntimeError(f"開催日の変換に失敗（先頭例 index={bad}）")
        if "開催回" in df.columns:
            df2["round"] = pd.to_numeric(df["開催回"], errors="coerce").astype("Int64")
        else:
            df2["round"] = pd.Series([None]*len(df2), dtype="Int64")

        # 桁分割（先頭ゼロ保持）
        raw_vals = df["抽選数字"].astype(str)
        nums = raw_vals.map(_digits_only).map(lambda x: x.zfill(digits)[:digits])

        # n1..n7 を初期化
        for i in range(1, 8):
            df2[f"n{i}"] = pd.Series([None]*len(df2), dtype="Int64")

        for i in range(digits):
            df2[f"n{i+1}"] = nums.str[i].astype("Int64")

        # ボーナスは無いので None
        df2["b1"] = pd.Series([None]*len(df2), dtype="Int64")
        df2["b2"] = pd.Series([None]*len(df2), dtype="Int64")

        # raw_json は元行を残す
        df2["raw_json"] = df.to_dict(orient="records")
        keep = ["round","ds"] + [f"n{i}" for i in range(1,8)] + ["b1","b2","raw_json"]
        return df2[keep].copy()

    # === 通常系（mini/loto6/loto7/bingo5 等） ===
    colmap = {}
    bonus_cols = []
    round_col = None
    date_col = None

    for c in df.columns:
        c_clean = str(c).strip()

        if c_clean in ("開催回",):
            round_col = c_clean
            continue
        if c_clean in ("開催日",):
            date_col = c_clean
            continue

        # bonus
        if RE_BONUS2.match(c_clean):
            bonus_cols.append((2, c_clean)); continue
        if RE_BONUS1.match(c_clean):
            bonus_cols.append((1, c_clean)); continue

        # main numbers
        m = RE_MAIN.match(c_clean) or RE_MAIN_2.match(c_clean) or RE_KETA.match(c_clean)
        if m:
            idx = int(m.group(1))
            colmap[c_clean] = f"n{idx}"
            continue

        # フォールバック： '第１数字' の全角数字にも対応
        try:
            d = re.sub(r"\D", "", c_clean)
            if "数字" in c_clean and d:
                colmap[c_clean] = f"n{int(d)}"
                continue
        except Exception:
            pass

    # rename
    df2 = df.rename(columns=colmap).copy()

    # date → ds
    if not date_col:
        raise RuntimeError("開催日の列（開催日）が見つかりません")
    df2["ds"] = pd.to_datetime(df2[date_col], errors="coerce").dt.date
    if df2["ds"].isna().any():
        bad = df2[df2["ds"].isna()].index[:3].tolist()
        raise RuntimeError(f"開催日の変換に失敗（先頭例 index={bad}）")

    # round
    if round_col and round_col in df2:
        df2["round"] = pd.to_numeric(df2[round_col], errors="coerce").astype("Int64")
    else:
        df2["round"] = pd.Series([None]*len(df2), dtype="Int64")

    # bonus → b1,b2
    b1 = b2 = None
    for idx, col in sorted(bonus_cols, key=lambda x: x[0]):
        if idx == 1: b1 = col
        if idx == 2: b2 = col
    if b1 and b1 in df2: df2["b1"] = pd.to_numeric(df2[b1], errors="coerce").astype("Int64")
    else:                df2["b1"] = pd.Series([None]*len(df2), dtype="Int64")
    if b2 and b2 in df2: df2["b2"] = pd.to_numeric(df2[b2], errors="coerce").astype("Int64")
    else:                df2["b2"] = pd.Series([None]*len(df2), dtype="Int64")

    # n1..n7 を int に（存在しない列は作っておく）
    for i in range(1, 8):
        col = f"n{i}"
        if col not in df2:
            df2[col] = pd.Series([None]*len(df2), dtype="Int64")
        else:
            df2[col] = pd.to_numeric(df2[col], errors="coerce").astype("Int64")

    # 最終列を絞る（ラフに raw_json も残す）
    keep = ["round", "ds"] + [f"n{i}" for i in range(1,8)] + ["b1","b2"]
    df_out = df2[keep].copy()
    df_out["raw_json"] = df.to_dict(orient="records")  # 元の行を丸ごと保存

    return df_out

def upsert(engine, game: str, df_norm: pd.DataFrame, table="lottery_draws", batch_size=5000):
    # Python → PostgreSQL（psycopg2）で ON CONFLICT upsert
    rows = []
    cols = ["game","round","ds","n1","n2","n3","n4","n5","n6","n7","b1","b2","raw_json"]
    for _, r in df_norm.iterrows():
        rows.append((
            game,
            None if pd.isna(r["round"]) else int(r["round"]),
            r["ds"],
            *[None if pd.isna(r[f"n{i}"]) else int(r[f"n{i}"]) for i in range(1,8)],
            None if pd.isna(r["b1"]) else int(r["b1"]),
            None if pd.isna(r["b2"]) else int(r["b2"]),
            json.dumps(r["raw_json"], ensure_ascii=False)
        ))

    sql = f"""
    INSERT INTO {table} ({",".join(cols)})
    VALUES %s
    ON CONFLICT (game, COALESCE(round, -1), ds)
    DO UPDATE SET
      n1 = EXCLUDED.n1, n2 = EXCLUDED.n2, n3 = EXCLUDED.n3, n4 = EXCLUDED.n4,
      n5 = EXCLUDED.n5, n6 = EXCLUDED.n6, n7 = EXCLUDED.n7,
      b1 = EXCLUDED.b1, b2 = EXCLUDED.b2,
      raw_json = EXCLUDED.raw_json
    """

    with engine.begin() as conn:
        with conn.connection.cursor() as cur:
            for i in range(0, len(rows), batch_size):
                execute_values(cur, sql, rows[i:i+batch_size])

def main():
    ap = argparse.ArgumentParser(description="Load lottery CSV into PostgreSQL")
    ap.add_argument("--db", required=True, help="PostgreSQL URL (postgresql://user:pass@host:port/dbname)")
    ap.add_argument("--game", choices=list(DEFAULT_URLS.keys()), required=True)
    ap.add_argument("--url", help="Override CSV URL (Shift-JIS). Default depends on --game")
    args = ap.parse_args()

    url = args.url or DEFAULT_URLS[args.game]
    print(f"[load] game={args.game} url={url}")

    # 読み込み（Shift-JIS）
    df = pd.read_csv(url, encoding="sjis")
    df_norm = normalize(df, args.game)

    # 型の最終確認
    assert "ds" in df_norm and "n1" in df_norm

    # DB 接続
    engine = create_engine(args.db, pool_pre_ping=True)
    upsert(engine, args.game, df_norm)
    print(f"[done] inserted/updated: {len(df_norm)} rows into lottery_draws")

if __name__ == "__main__":
    main()
