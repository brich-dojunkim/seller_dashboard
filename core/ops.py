from __future__ import annotations
import pandas as pd
from typing import Optional
from .types import MetricParams

def filter_by_params(df: pd.DataFrame, p: MetricParams) -> pd.DataFrame:
    out = df.copy()
    if '주문일시' in out.columns:
        if p.get('date_from') is not None:
            out = out[out['주문일시'] >= p['date_from']]
        if p.get('date_to') is not None:
            out = out[out['주문일시'] <= p['date_to']]
    if not p.get('include_canceled', False) and '주문상태' in out.columns:
        out = out[out['주문상태'].astype(str) != '결제취소']
    for col, key in [('채널명','channels'),('업체명','sellers'),('카테고리','categories')]:
        vals = p.get(key, [])
        if vals and col in out.columns:
            out = out[out[col].isin(vals)]
    return out

def safe_group_sum(d: pd.DataFrame, by: list[str], val: str, out_col: str) -> pd.DataFrame:
    keys = [k for k in by if k in d.columns]
    if not keys:
        d = d.copy(); d['_전체'] = '전체'; keys=['_전체']
    if val not in d.columns:
        d = d.copy(); d[val]=0
    g = d.groupby(keys, dropna=False)[val].sum().reset_index(name=out_col)
    return g

def safe_group_size(d: pd.DataFrame, by: list[str], out_col: str) -> pd.DataFrame:
    keys = [k for k in by if k in d.columns]
    if not keys:
        d = d.copy(); d['_전체'] = '전체'; keys=['_전체']
    g = d.groupby(keys, dropna=False).size().reset_index(name=out_col)
    return g

def add_rank(df: pd.DataFrame, sort_col: str, asc: bool=False, rank_col: str='순위') -> pd.DataFrame:
    out = df.copy()
    if sort_col not in out.columns:
        out[sort_col] = 0
    out[rank_col] = out[sort_col].rank(method='dense', ascending=asc).astype(int)
    return out

def issue_rate(d: pd.DataFrame, keys: list[str], pattern: str) -> pd.DataFrame:
    total = safe_group_size(d, keys, '총건수')
    status = d.get('주문상태', pd.Series(dtype='string')).astype(str)
    num_df = d[ status.str.contains(pattern, na=False) ].copy()
    num = safe_group_size(num_df, keys, '이슈건수')
    on_keys = [k for k in keys if (k in total.columns and k in num.columns)]
    g = total.merge(num, on=on_keys, how='left')
    g['이슈건수'] = g['이슈건수'].fillna(0)
    g['비율(%)'] = (g['이슈건수'] / g['총건수'].replace({0: pd.NA}) * 100).fillna(0).round(2)
    return g
