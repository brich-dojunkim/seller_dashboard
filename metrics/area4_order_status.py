from __future__ import annotations
import pandas as pd
from core.types import MetricParams
# Auto-rewrite from metrics CSV

def mA4_001(df: pd.DataFrame, params: MetricParams):
    """업체별총매출액 — CSV 정의 반영 독립 구현"""
    from core import ops
    d = ops.filter_by_params(df, params)
    d = d.copy()
    # ensure time columns
    if '주문일' not in d.columns and '주문일시' in d.columns:
        d['주문일'] = pd.to_datetime(d['주문일시']).dt.floor('D')
    if '요일' not in d.columns and '주문일시' in d.columns:
        d['요일'] = pd.to_datetime(d['주문일시']).dt.dayofweek.map({0:'월',1:'화',2:'수',3:'목',4:'금',5:'토',6:'일'})
    if '시간대' not in d.columns and '주문일시' in d.columns:
        d['시간대'] = pd.to_datetime(d['주문일시']).dt.hour
    g = ops.safe_group_sum(d, ['업체명'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['업체명', '총매출액']]

def mA4_002(df: pd.DataFrame, params: MetricParams):
    """업체별총주문수 — CSV 정의 반영 독립 구현"""
    from core import ops
    d = ops.filter_by_params(df, params)
    d = d.copy()
    # ensure time columns
    if '주문일' not in d.columns and '주문일시' in d.columns:
        d['주문일'] = pd.to_datetime(d['주문일시']).dt.floor('D')
    if '요일' not in d.columns and '주문일시' in d.columns:
        d['요일'] = pd.to_datetime(d['주문일시']).dt.dayofweek.map({0:'월',1:'화',2:'수',3:'목',4:'금',5:'토',6:'일'})
    if '시간대' not in d.columns and '주문일시' in d.columns:
        d['시간대'] = pd.to_datetime(d['주문일시']).dt.hour
    g = ops.safe_group_size(d, ['업체명'], '주문건수')
    g = g.sort_values('주문건수', ascending=False).reset_index(drop=True)
    return g[['업체명', '주문건수']]

def mA4_003(df: pd.DataFrame, params: MetricParams):
    """업체별평균주문금액 — CSV 정의 반영 독립 구현"""
    from core import ops
    d = ops.filter_by_params(df, params)
    d = d.copy()
    # ensure time columns
    if '주문일' not in d.columns and '주문일시' in d.columns:
        d['주문일'] = pd.to_datetime(d['주문일시']).dt.floor('D')
    if '요일' not in d.columns and '주문일시' in d.columns:
        d['요일'] = pd.to_datetime(d['주문일시']).dt.dayofweek.map({0:'월',1:'화',2:'수',3:'목',4:'금',5:'토',6:'일'})
    if '시간대' not in d.columns and '주문일시' in d.columns:
        d['시간대'] = pd.to_datetime(d['주문일시']).dt.hour
    g = ops.safe_group_sum(d, ['업체명'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['업체명', '총매출액']]

def mA4_004(df: pd.DataFrame, params: MetricParams):
    """업체별총수익액 — CSV 정의 반영 독립 구현"""
    from core import ops
    d = ops.filter_by_params(df, params)
    d = d.copy()
    # ensure time columns
    if '주문일' not in d.columns and '주문일시' in d.columns:
        d['주문일'] = pd.to_datetime(d['주문일시']).dt.floor('D')
    if '요일' not in d.columns and '주문일시' in d.columns:
        d['요일'] = pd.to_datetime(d['주문일시']).dt.dayofweek.map({0:'월',1:'화',2:'수',3:'목',4:'금',5:'토',6:'일'})
    if '시간대' not in d.columns and '주문일시' in d.columns:
        d['시간대'] = pd.to_datetime(d['주문일시']).dt.hour
    g = ops.safe_group_sum(d, ['업체명'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['업체명', '총매출액']]

def mA4_005(df: pd.DataFrame, params: MetricParams):
    """업체별총수량 — CSV 정의 반영 독립 구현"""
    from core import ops
    d = ops.filter_by_params(df, params)
    d = d.copy()
    # ensure time columns
    if '주문일' not in d.columns and '주문일시' in d.columns:
        d['주문일'] = pd.to_datetime(d['주문일시']).dt.floor('D')
    if '요일' not in d.columns and '주문일시' in d.columns:
        d['요일'] = pd.to_datetime(d['주문일시']).dt.dayofweek.map({0:'월',1:'화',2:'수',3:'목',4:'금',5:'토',6:'일'})
    if '시간대' not in d.columns and '주문일시' in d.columns:
        d['시간대'] = pd.to_datetime(d['주문일시']).dt.hour
    g = ops.safe_group_sum(d, ['업체명'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['업체명', '총매출액']]

def mA4_006(df: pd.DataFrame, params: MetricParams):
    """업체별매출순위 — CSV 정의 반영 독립 구현"""
    from core import ops
    d = ops.filter_by_params(df, params)
    d = d.copy()
    # ensure time columns
    if '주문일' not in d.columns and '주문일시' in d.columns:
        d['주문일'] = pd.to_datetime(d['주문일시']).dt.floor('D')
    if '요일' not in d.columns and '주문일시' in d.columns:
        d['요일'] = pd.to_datetime(d['주문일시']).dt.dayofweek.map({0:'월',1:'화',2:'수',3:'목',4:'금',5:'토',6:'일'})
    if '시간대' not in d.columns and '주문일시' in d.columns:
        d['시간대'] = pd.to_datetime(d['주문일시']).dt.hour
    g = ops.safe_group_sum(d, ['업체명'], '상품별 총 주문금액', '총매출액')
    g = ops.add_rank(g, '총매출액', asc=False, rank_col='순위')
    g = g.sort_values('총매출액', ascending=False).head(30).reset_index(drop=True)
    return g[['업체명', '총매출액', '순위']]

def mA4_007(df: pd.DataFrame, params: MetricParams):
    """업체별매출성장률(전기대비) — CSV 정의 반영 독립 구현"""
    from core import ops
    d = ops.filter_by_params(df, params)
    d = d.copy()
    # ensure time columns
    if '주문일' not in d.columns and '주문일시' in d.columns:
        d['주문일'] = pd.to_datetime(d['주문일시']).dt.floor('D')
    if '요일' not in d.columns and '주문일시' in d.columns:
        d['요일'] = pd.to_datetime(d['주문일시']).dt.dayofweek.map({0:'월',1:'화',2:'수',3:'목',4:'금',5:'토',6:'일'})
    if '시간대' not in d.columns and '주문일시' in d.columns:
        d['시간대'] = pd.to_datetime(d['주문일시']).dt.hour
    date_from = params.get('date_from'); date_to = params.get('date_to')
    if date_from is None or date_to is None:
        prev = d.copy()
    else:
        period = date_to - date_from
        prev_from = date_from - period
        prev_to = date_from - pd.Timedelta(seconds=1)
        prev = df[(df['주문일시']>=prev_from) & (df['주문일시']<=prev_to)].copy()
    cur = d.groupby(['업체명'], dropna=False)['상품별 총 주문금액'].sum().reset_index(name='현재')
    pre = prev.groupby(['업체명'], dropna=False)['상품별 총 주문금액'].sum().reset_index(name='이전')
    g = pd.merge(cur, pre, on=[c for c in cur.columns if c in pre.columns and c not in ['현재','이전']], how='outer').fillna(0)
    g['변동량'] = (g['현재'] - g['이전']).round(2)
    g['변동률(%)'] = g.apply(lambda r: (100.0 if r['이전']==0 and r['현재']>0 else (0.0 if r['이전']==0 else (r['변동량']/r['이전']*100))), axis=1).round(2)
    g = g.sort_values('변동률(%)', ascending=False).reset_index(drop=True)
    return g[['업체명', '현재', '이전', '변동량', '변동률(%)']]

def mA4_008(df: pd.DataFrame, params: MetricParams):
    """업체매출성장률 — CSV 정의 반영 독립 구현"""
    from core import ops
    d = ops.filter_by_params(df, params)
    d = d.copy()
    # ensure time columns
    if '주문일' not in d.columns and '주문일시' in d.columns:
        d['주문일'] = pd.to_datetime(d['주문일시']).dt.floor('D')
    if '요일' not in d.columns and '주문일시' in d.columns:
        d['요일'] = pd.to_datetime(d['주문일시']).dt.dayofweek.map({0:'월',1:'화',2:'수',3:'목',4:'금',5:'토',6:'일'})
    if '시간대' not in d.columns and '주문일시' in d.columns:
        d['시간대'] = pd.to_datetime(d['주문일시']).dt.hour
    date_from = params.get('date_from'); date_to = params.get('date_to')
    if date_from is None or date_to is None:
        prev = d.copy()
    else:
        period = date_to - date_from
        prev_from = date_from - period
        prev_to = date_from - pd.Timedelta(seconds=1)
        prev = df[(df['주문일시']>=prev_from) & (df['주문일시']<=prev_to)].copy()
    cur = d.groupby(['업체명'], dropna=False)['상품별 총 주문금액'].sum().reset_index(name='현재')
    pre = prev.groupby(['업체명'], dropna=False)['상품별 총 주문금액'].sum().reset_index(name='이전')
    g = pd.merge(cur, pre, on=[c for c in cur.columns if c in pre.columns and c not in ['현재','이전']], how='outer').fillna(0)
    g['변동량'] = (g['현재'] - g['이전']).round(2)
    g['변동률(%)'] = g.apply(lambda r: (100.0 if r['이전']==0 and r['현재']>0 else (0.0 if r['이전']==0 else (r['변동량']/r['이전']*100))), axis=1).round(2)
    g = g.sort_values('변동률(%)', ascending=False).reset_index(drop=True)
    return g[['업체명', '현재', '이전', '변동량', '변동률(%)']]

def mA4_009(df: pd.DataFrame, params: MetricParams):
    """업체별매출순위변동 — CSV 정의 반영 독립 구현"""
    from core import ops
    d = ops.filter_by_params(df, params)
    d = d.copy()
    # ensure time columns
    if '주문일' not in d.columns and '주문일시' in d.columns:
        d['주문일'] = pd.to_datetime(d['주문일시']).dt.floor('D')
    if '요일' not in d.columns and '주문일시' in d.columns:
        d['요일'] = pd.to_datetime(d['주문일시']).dt.dayofweek.map({0:'월',1:'화',2:'수',3:'목',4:'금',5:'토',6:'일'})
    if '시간대' not in d.columns and '주문일시' in d.columns:
        d['시간대'] = pd.to_datetime(d['주문일시']).dt.hour
    g = ops.safe_group_sum(d, ['업체명'], '상품별 총 주문금액', '총매출액')
    g = ops.add_rank(g, '총매출액', asc=False, rank_col='순위')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['업체명', '총매출액', '순위']]

def mA4_010(df: pd.DataFrame, params: MetricParams):
    """업체별평균마진율 — CSV 정의 반영 독립 구현"""
    from core import ops
    d = ops.filter_by_params(df, params)
    d = d.copy()
    # ensure time columns
    if '주문일' not in d.columns and '주문일시' in d.columns:
        d['주문일'] = pd.to_datetime(d['주문일시']).dt.floor('D')
    if '요일' not in d.columns and '주문일시' in d.columns:
        d['요일'] = pd.to_datetime(d['주문일시']).dt.dayofweek.map({0:'월',1:'화',2:'수',3:'목',4:'금',5:'토',6:'일'})
    if '시간대' not in d.columns and '주문일시' in d.columns:
        d['시간대'] = pd.to_datetime(d['주문일시']).dt.hour
    g = ops.safe_group_sum(d, ['업체명'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['업체명', '총매출액']]

def mA4_011(df: pd.DataFrame, params: MetricParams):
    """업체별마진순위 — CSV 정의 반영 독립 구현"""
    from core import ops
    d = ops.filter_by_params(df, params)
    d = d.copy()
    # ensure time columns
    if '주문일' not in d.columns and '주문일시' in d.columns:
        d['주문일'] = pd.to_datetime(d['주문일시']).dt.floor('D')
    if '요일' not in d.columns and '주문일시' in d.columns:
        d['요일'] = pd.to_datetime(d['주문일시']).dt.dayofweek.map({0:'월',1:'화',2:'수',3:'목',4:'금',5:'토',6:'일'})
    if '시간대' not in d.columns and '주문일시' in d.columns:
        d['시간대'] = pd.to_datetime(d['주문일시']).dt.hour
    g = ops.safe_group_sum(d, ['업체명'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['업체명', '총매출액']]

def mA4_012(df: pd.DataFrame, params: MetricParams):
    """업체별취소율 — CSV 정의 반영 독립 구현"""
    from core import ops
    d = ops.filter_by_params(df, params)
    d = d.copy()
    # ensure time columns
    if '주문일' not in d.columns and '주문일시' in d.columns:
        d['주문일'] = pd.to_datetime(d['주문일시']).dt.floor('D')
    if '요일' not in d.columns and '주문일시' in d.columns:
        d['요일'] = pd.to_datetime(d['주문일시']).dt.dayofweek.map({0:'월',1:'화',2:'수',3:'목',4:'금',5:'토',6:'일'})
    if '시간대' not in d.columns and '주문일시' in d.columns:
        d['시간대'] = pd.to_datetime(d['주문일시']).dt.hour
    g = ops.issue_rate(d, ['업체명'], r"취소")
    return g[['업체명', '총건수', '이슈건수', '비율(%)']]

def mA4_013(df: pd.DataFrame, params: MetricParams):
    """업체별반품율 — CSV 정의 반영 독립 구현"""
    from core import ops
    d = ops.filter_by_params(df, params)
    d = d.copy()
    # ensure time columns
    if '주문일' not in d.columns and '주문일시' in d.columns:
        d['주문일'] = pd.to_datetime(d['주문일시']).dt.floor('D')
    if '요일' not in d.columns and '주문일시' in d.columns:
        d['요일'] = pd.to_datetime(d['주문일시']).dt.dayofweek.map({0:'월',1:'화',2:'수',3:'목',4:'금',5:'토',6:'일'})
    if '시간대' not in d.columns and '주문일시' in d.columns:
        d['시간대'] = pd.to_datetime(d['주문일시']).dt.hour
    g = ops.issue_rate(d, ['업체명'], r"반품")
    return g[['업체명', '총건수', '이슈건수', '비율(%)']]

def mA4_014(df: pd.DataFrame, params: MetricParams):
    """업체별교환율 — CSV 정의 반영 독립 구현"""
    from core import ops
    d = ops.filter_by_params(df, params)
    d = d.copy()
    # ensure time columns
    if '주문일' not in d.columns and '주문일시' in d.columns:
        d['주문일'] = pd.to_datetime(d['주문일시']).dt.floor('D')
    if '요일' not in d.columns and '주문일시' in d.columns:
        d['요일'] = pd.to_datetime(d['주문일시']).dt.dayofweek.map({0:'월',1:'화',2:'수',3:'목',4:'금',5:'토',6:'일'})
    if '시간대' not in d.columns and '주문일시' in d.columns:
        d['시간대'] = pd.to_datetime(d['주문일시']).dt.hour
    g = ops.safe_group_sum(d, ['업체명'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['업체명', '총매출액']]

def mA4_015(df: pd.DataFrame, params: MetricParams):
    """업체별클레임율 — CSV 정의 반영 독립 구현"""
    from core import ops
    d = ops.filter_by_params(df, params)
    d = d.copy()
    # ensure time columns
    if '주문일' not in d.columns and '주문일시' in d.columns:
        d['주문일'] = pd.to_datetime(d['주문일시']).dt.floor('D')
    if '요일' not in d.columns and '주문일시' in d.columns:
        d['요일'] = pd.to_datetime(d['주문일시']).dt.dayofweek.map({0:'월',1:'화',2:'수',3:'목',4:'금',5:'토',6:'일'})
    if '시간대' not in d.columns and '주문일시' in d.columns:
        d['시간대'] = pd.to_datetime(d['주문일시']).dt.hour
    g = ops.issue_rate(d, ['업체명'], r"취소|반품|교환|클레임")
    return g[['업체명', '총건수', '이슈건수', '비율(%)']]

