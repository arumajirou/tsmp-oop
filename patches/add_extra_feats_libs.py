import re, pathlib

p = pathlib.Path('scripts/build_exog.py')
s = p.read_text(encoding='utf-8')

# (A) インポート部に try-import を注入
if 'TSFRESH_AVAILABLE' not in s:
    s = re.sub(
        r'(\nimport re\n)',
        r'''\1
# --- extra libs (optional) ---
try:
    from tsfresh.feature_extraction import feature_calculators as _tsf_fc
    TSFRESH_AVAILABLE = True
except Exception:
    TSFRESH_AVAILABLE = False

try:
    import tsfel as _tsfel
    TSFEL_AVAILABLE = True
except Exception:
    TSFEL_AVAILABLE = False
''', s, count=1
    )

# (B) ユーティリティ（スラグ化）
if 'def _slug' not in s:
    s = s.replace(
        'def split_unique_id(uid: str):',
        '''import unicodedata
def _slug(x:str)->str:
    x = unicodedata.normalize("NFKC", str(x))
    x = re.sub(r"[^A-Za-z0-9]+", "_", x).strip("_").lower()
    return x

def split_unique_id(uid: str):'''
    )

# (C) 追加特徴の構築関数を注入（pandasベース）
if 'def enrich_with_libs(' not in s:
    s = s.replace(
        'def build_features_pandas(df: pd.DataFrame, max_lag: int) -> pd.DataFrame:',
        r'''def enrich_with_libs(df: pd.DataFrame, feat: pd.DataFrame, window: int = 28,
                      enable_tsfresh: bool = True, enable_tsfel: bool = True) -> pd.DataFrame:
    """
    df: ソース（unique_id, ds, y）
    feat: 既存特徴（unique_id, ds を含む）
    返値: feat に hist_* 追加列を横結合
    """
    import numpy as np
    import pandas as pd

    df_sorted = df.sort_values(["unique_id","ds"]).reset_index(drop=True)
    out = pd.DataFrame({"unique_id": df_sorted["unique_id"].values,
                        "ds": df_sorted["ds"].dt.date.values})

    # --- tsfresh: rolling window でいくつかの電卓を適用（軽量セット） ---
    if enable_tsfresh and TSFRESH_AVAILABLE:
        def _roll_apply(series: pd.Series, fn):
            return series.rolling(window, min_periods=window).apply(lambda a: float(fn(a)), raw=True)

        group_idx = []
        blocks = []
        for uid, g in df_sorted.groupby("unique_id", sort=False):
            svals = g["y"].astype("float64")
            cols = {}
            try:
                cols[f"hist_tsfresh_kurt_{window}"] = _roll_apply(svals, _tsf_fc.kurtosis)
            except Exception:
                pass
            try:
                cols[f"hist_tsfresh_skew_{window}"] = _roll_apply(svals, _tsf_fc.skewness)
            except Exception:
                pass
            try:
                cols[f"hist_tsfresh_var_{window}"] = _roll_apply(svals, _tsf_fc.variance)
            except Exception:
                cols[f"hist_tsfresh_var_{window}"] = svals.rolling(window, min_periods=window).var()
            try:
                cols[f"hist_tsfresh_abs_energy_{window}"] = _roll_apply(svals, _tsf_fc.abs_energy)
            except Exception:
                pass
            try:
                cols[f"hist_tsfresh_abs_sum_changes_{window}"] = _roll_apply(svals, _tsf_fc.absolute_sum_of_changes)
            except Exception:
                pass

            blk = pd.DataFrame(cols, index=g.index)
            blocks.append(blk)
            group_idx.append(g.index)
        if blocks:
            tf = pd.concat(blocks, axis=0).sort_index()
            out = out.join(tf.reset_index(drop=True))

    # --- TSFEL: temporal ドメインから軽量特徴を移動窓で ---
    if enable_tsfel and TSFEL_AVAILABLE:
        try:
            cfg = _tsfel.get_features_by_domain('temporal')
            keep = {"Mean","Standard deviation","Skewness","Kurtosis","Zero Crossing Rate","Energy"}
            # 選抜
            for dom in list(cfg.keys()):
                for feat_name in list(cfg[dom].keys()):
                    if feat_name not in keep:
                        cfg[dom].pop(feat_name, None)
            # 各 unique_id ごとにオーバーラップ窓で各日出力（重い場合は overlap を落とす）
            import numpy as np
            recs = []
            for uid, g in df_sorted.groupby("unique_id", sort=False):
                svals = g["y"].astype("float64").reset_index(drop=True)
                # overlap=window-1 で「毎日1行」出す
                tf = _tsfel.time_series_features_extractor(cfg, svals, fs=1.0,
                                                           window_size=window, overlap=window-1,
                                                           verbose=0)
                if tf.empty:
                    continue
                tf.columns = [f"hist_tsfel_{_slug(c)}_{window}" for c in tf.columns]
                end_idx = np.arange(window-1, window-1+len(tf))
                tf["__idx__"] = end_idx
                # g.index は連続なので、end_idx を g.index[end] にマップ
                tf.index = g.index[tf["__idx__"].values]
                tf = tf.drop(columns=["__idx__"])
                # 前方は NaN のまま（後で 0 埋め）
                recs.append(tf.reindex(g.index))
            if recs:
                tfall = pd.concat(recs, axis=0).sort_index()
                out = out.join(tfall.reset_index(drop=True))
        except Exception:
            pass

    # 合体
    feat2 = pd.merge(feat, out.drop(columns=["unique_id","ds"], errors="ignore"),
                     left_on=["unique_id","ds"], right_on=["unique_id","ds"], how="left")
    return feat2

def build_features_pandas(df: pd.DataFrame, max_lag: int) -> pd.DataFrame:'''
    )

# (D) argparse にフラグを追加
s = re.sub(r'(parser = argparse\.ArgumentParser\(.*?\)\n)', r'''\1
    parser.add_argument("--fe-extra", choices=["none","tsfresh","tsfel","all"], default="all",
                        help="Add extra features from libs (tsfresh/tsfel)") 
''', s, flags=re.S)

# (E) main のビルド直後に enrich を差し込む（列抽出より前）
s = re.sub(
    r'(# ---- build features ----\s+if use_gpu:.*?else:.*?feat = build_features_pandas\(df, args\.max_lag\)\s+)',
    r'''\1
    # --- extra feature enrichment (CPU) ---
    if args.fe_extra != "none":
        _use_tsfresh = args.fe_extra in ("tsfresh","all")
        _use_tsfel   = args.fe_extra in ("tsfel","all")
        try:
            feat = enrich_with_libs(df, feat, window=28,
                                    enable_tsfresh=_use_tsfresh, enable_tsfel=_use_tsfel)
        except Exception as _e:
            print(f"[warn] enrich_with_libs failed: {_e}")
''',
    s, flags=re.S
)

p.write_text(s, encoding='utf-8')
print("patched", p)
