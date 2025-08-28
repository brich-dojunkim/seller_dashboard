
from __future__ import annotations
import importlib
import pandas as pd
import streamlit as st
from core.io import get_orders
from core.registry import list_metrics
from core.types import MetricParams
import config

st.set_page_config(page_title="B-Flow Metrics Dashboard — Auto 82", layout="wide")

def _import_callable(fqn: str):
    mod_name, func_name = fqn.rsplit('.', 1)
    mod = importlib.import_module(mod_name)
    return getattr(mod, func_name)

df = get_orders()
areas = {1:'채널',2:'상품',3:'고객',4:'업체',5:'카테고리',6:'트렌드'}
area = st.sidebar.selectbox("영역", options=[1,2,3,4,5,6], format_func=lambda x: f"[{x}] {areas[x]}")
q = st.sidebar.text_input("지표 검색")

specs = list_metrics(area=area, q=q)
labels = [f"{s['id']} · {s['name']}" for s in specs]
idx = st.sidebar.selectbox("지표", options=list(range(len(specs))), format_func=lambda i: labels[i] if specs else "N/A")

date_from = st.sidebar.date_input("시작일", value=df['주문일시'].min().date() if not df.empty else None)
date_to = st.sidebar.date_input("종료일", value=df['주문일시'].max().date() if not df.empty else None)
channels = st.sidebar.multiselect("채널", sorted(df['채널명'].dropna().unique().tolist()))
sellers = st.sidebar.multiselect("업체", sorted(df['업체명'].dropna().unique().tolist()))
cats = st.sidebar.multiselect("카테고리", sorted(df['카테고리'].dropna().unique().tolist()))
top_n = st.sidebar.number_input("Top N", min_value=0, max_value=2000, value=50, step=10)
include_canceled = st.sidebar.checkbox("결제취소 포함", value=False)

params: MetricParams = {
    'date_from': pd.to_datetime(date_from),
    'date_to': pd.to_datetime(date_to) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1),
    'channels': channels,
    'sellers': sellers,
    'categories': cats,
    'top_n': int(top_n),
    'include_canceled': bool(include_canceled),
}

spec = specs[idx]
st.title(f"B-Flow Metrics — {spec['id']} · {spec['name']}")

func = _import_callable(spec['func_fqn'])
out = func(df, params)
st.dataframe(out, use_container_width=True, height=650)
st.download_button("CSV 다운로드", data=out.to_csv(index=False).encode("utf-8-sig"), file_name=f"{spec['id']}_{spec['name']}.csv", mime="text/csv")
