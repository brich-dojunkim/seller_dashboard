from __future__ import annotations
import pandas as pd
import streamlit as st
from . import preprocess
import config  # 절대 임포트로 교체

def load_orders_raw() -> pd.DataFrame:
    return pd.read_excel(config.DATA_XLSX_PATH, sheet_name=config.SHEET_NAME, engine="openpyxl")

@st.cache_data(ttl=config.CACHE_TTL_SEC, show_spinner=False)
def get_orders() -> pd.DataFrame:
    raw = load_orders_raw()
    std = raw.copy()
    for stdcol, rawcol in config.COLMAP.items():
        if rawcol in std.columns and stdcol != rawcol:
            std.rename(columns={rawcol: stdcol}, inplace=True)
    fin = preprocess.finalize(std)
    return fin
