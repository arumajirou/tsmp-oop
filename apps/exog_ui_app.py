import os, io, json, numpy as np, pandas as pd
import datetime as dt
import streamlit as st
import plotly.graph_objs as go
from sqlalchemy import create_engine, text

st.set_page_config(layout="wide", page_title="Lottery Exog Viewer")

@st.cache_resource
def get_engine():
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL が未設定です。export DATABASE_URL='postgresql://...'")
    return create_engine(url, pool_pre_ping=True)

@st.cache_data(ttl=300)
def load_meta(engine):
    with engine.begin() as con:
        meta = pd.read_sql("SELECT * FROM lottery_exog_meta WHERE id=1", con)
        if meta.empty:
            return [], [], []
        row = meta.iloc[0]
        futr = list(row["futr_exog_list"])
        hist = list(row["hist_exog_list"])
        stat = list(row["stat_exog_list"])
        return futr, hist, stat

@st.cache_data(ttl=300)
def list_unique_ids(engine, table):
    q = text(f"SELECT DISTINCT unique_id FROM {table} ORDER BY 1")
    with engine.begin() as con:
        return pd.read_sql(q, con)["unique_id"].tolist()

@st.cache_data(ttl=120)
def date_range_for_uid(engine, table, uid):
    q = text(f"SELECT MIN(ds) AS min_ds, MAX(ds) AS max_ds FROM {table} WHERE unique_id=:u")
    with engine.begin() as con:
        r = pd.read_sql(q, con, params={"u": uid}).iloc[0]
        return (r["min_ds"], r["max_ds"])

@st.cache_data(ttl=120, show_spinner="問い合わせ中...")
def fetch_df(engine, table, uid, cols, start, end, limit):
    base = ["unique_id","ds"] + cols
    q = f"SELECT {', '.join(base)} FROM {table} WHERE unique_id=:u"
    params = {"u": uid}
    if start: q += " AND ds >= :s"; params["s"] = start
    if end:   q += " AND ds <= :e"; params["e"] = end
    q += " ORDER BY ds"
    if limit and limit > 0:
        q += f" LIMIT {int(limit)}"
    with engine.begin() as con:
        df = pd.read_sql(text(q), con, params=params)
    if not df.empty:
        df["ds"] = pd.to_datetime(df["ds"])
        num_cols = [c for c in df.columns if c not in ("unique_id","ds")]
        df[num_cols] = df[num_cols].replace([np.inf, -np.inf], 0).fillna(0)
    return df

def plot_lines(df, cols, title):
    traces = []
    for c in cols:
        if c in ("unique_id","ds"): continue
        traces.append(go.Scatter(x=df["ds"], y=df[c], mode="lines", name=c))
    fig = go.Figure(traces)
    fig.update_layout(title=title, xaxis_title="ds", hovermode="x unified")
    return fig

def main():
    st.title("🎛️ Lottery External Features Viewer")
    engine = get_engine()
    table = st.sidebar.text_input("テーブル名", value="lottery_exog")
    futr_cols, hist_cols, stat_cols = load_meta(engine)
    if not (futr_cols or hist_cols or stat_cols):
        st.warning("lottery_exog_meta が空です。先に特徴量を生成してください。")
    uids = list_unique_ids(engine, table)
    if not uids:
        st.stop()

    uid = st.sidebar.selectbox("unique_id", options=uids, index=0)
    min_ds, max_ds = date_range_for_uid(engine, table, uid)
    st.sidebar.markdown(f"期間: **{min_ds}** 〜 **{max_ds}**")
    start = st.sidebar.date_input("開始日", value=min_ds, min_value=min_ds, max_value=max_ds)
    end   = st.sidebar.date_input("終了日", value=max_ds, min_value=min_ds, max_value=max_ds)

    st.sidebar.markdown("### 列の選択")
    with st.sidebar.expander("futr_", expanded=True):
        pick_futr = st.multiselect("futr_", futr_cols, default=futr_cols[:5])
    with st.sidebar.expander("hist_", expanded=True):
        pick_hist = st.multiselect("hist_", hist_cols, default=["hist_lag_1","hist_roll7_mean"])
    with st.sidebar.expander("stat_", expanded=True):
        pick_stat = st.multiselect("stat_", stat_cols, default=["stat_y_mean","stat_len"])

    limit = st.sidebar.number_input("LIMIT（行上限）", min_value=0, max_value=200000, value=0, step=1000, help="0 で無制限（注意）")
    do_load = st.sidebar.button("データ取得", type="primary")

    if do_load:
        cols = pick_futr + pick_hist + pick_stat
        if not cols:
            st.error("少なくとも1列選択してください。")
            st.stop()
        df = fetch_df(engine, table, uid, cols, start, end, limit)
        if df.empty:
            st.info("該当データがありません。")
            st.stop()

        # 概要
        st.subheader("概要")
        c1, c2, c3 = st.columns(3)
        c1.metric("行数", f"{len(df):,}")
        c2.metric("期間", f"{df['ds'].min().date()} → {df['ds'].max().date()}")
        c3.metric("列数", f"{len(df.columns):,}")

        # 表示
        st.subheader("データプレビュー")
        st.dataframe(df, use_container_width=True, height=420)

        # 品質チェック
        st.subheader("品質チェック（NaN/±Inf→0 置換後）")
        num_cols = [c for c in df.columns if c not in ("unique_id","ds")]
        bad = {}
        for c in num_cols:
            s = df[c]
            bad[c] = int(np.isnan(s).sum() + np.isinf(s).sum())
        st.write({k:v for k,v in bad.items() if v})

        # 可視化
        st.subheader("可視化")
        fig = plot_lines(df, num_cols, f"{uid} : {', '.join(num_cols[:6])}{' ...' if len(num_cols)>6 else ''}")
        st.plotly_chart(fig, use_container_width=True)

        # ダウンロード
        st.subheader("ダウンロード")
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("CSV ダウンロード", data=csv, file_name=f"{uid}_{start}_{end}.csv", mime="text/csv")
        try:
            import pyarrow as pa, pyarrow.parquet as pq
            buf = io.BytesIO()
            table_pa = pa.Table.from_pandas(df)
            pq.write_table(table_pa, buf)
            st.download_button("Parquet ダウンロード", data=buf.getvalue(), file_name=f"{uid}_{start}_{end}.parquet", mime="application/octet-stream")
        except Exception as e:
            st.info(f"Parquet は省略: {e}")

    else:
        st.info("左のサイドバーで条件を設定し、**データ取得** を押してください。")

if __name__ == "__main__":
    main()
