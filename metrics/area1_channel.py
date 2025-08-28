from __future__ import annotations
import pandas as pd
from core.types import MetricParams
# Auto-rewrite from metrics CSV

def mA1_001(df: pd.DataFrame, params: MetricParams):
    """채널별매출비중 — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['채널명'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['채널명', '총매출액']]

def mA1_002(df: pd.DataFrame, params: MetricParams):
    """채널별주문수비중 — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['채널명'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['채널명', '총매출액']]

def mA1_003(df: pd.DataFrame, params: MetricParams):
    """채널별업체매출순위 — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['채널명', '업체명'], '상품별 총 주문금액', '총매출액')
    g = ops.add_rank(g, '총매출액', asc=False, rank_col='순위')
    g = g.sort_values('총매출액', ascending=False).head(30).reset_index(drop=True)
    return g[['채널명', '업체명', '총매출액', '순위']]

def mA1_004(df: pd.DataFrame, params: MetricParams):
    """채널별업체점유율 — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['채널명', '업체명'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['채널명', '업체명', '총매출액']]

def mA1_005(df: pd.DataFrame, params: MetricParams):
    """채널매출성장률 — CSV 정의 반영 독립 구현"""
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
    cur = d.groupby(['채널명'], dropna=False)['상품별 총 주문금액'].sum().reset_index(name='현재')
    pre = prev.groupby(['채널명'], dropna=False)['상품별 총 주문금액'].sum().reset_index(name='이전')
    g = pd.merge(cur, pre, on=[c for c in cur.columns if c in pre.columns and c not in ['현재','이전']], how='outer').fillna(0)
    g['변동량'] = (g['현재'] - g['이전']).round(2)
    g['변동률(%)'] = g.apply(lambda r: (100.0 if r['이전']==0 and r['현재']>0 else (0.0 if r['이전']==0 else (r['변동량']/r['이전']*100))), axis=1).round(2)
    g = g.sort_values('변동률(%)', ascending=False).reset_index(drop=True)
    return g[['채널명', '현재', '이전', '변동량', '변동률(%)']]

def mA1_006(df: pd.DataFrame, params: MetricParams):
    """채널주문수성장률 — CSV 정의 반영 독립 구현"""
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
    cur = d.groupby(['채널명'], dropna=False)['상품별 총 주문금액'].sum().reset_index(name='현재')
    pre = prev.groupby(['채널명'], dropna=False)['상품별 총 주문금액'].sum().reset_index(name='이전')
    g = pd.merge(cur, pre, on=[c for c in cur.columns if c in pre.columns and c not in ['현재','이전']], how='outer').fillna(0)
    g['변동량'] = (g['현재'] - g['이전']).round(2)
    g['변동률(%)'] = g.apply(lambda r: (100.0 if r['이전']==0 and r['현재']>0 else (0.0 if r['이전']==0 else (r['변동량']/r['이전']*100))), axis=1).round(2)
    g = g.sort_values('변동률(%)', ascending=False).reset_index(drop=True)
    return g[['채널명', '현재', '이전', '변동량', '변동률(%)']]

def mA1_007(df: pd.DataFrame, params: MetricParams):
    """채널점유율추세 — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['채널명', '주문일'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['채널명', '주문일', '총매출액']]

def mA1_008(df: pd.DataFrame, params: MetricParams):
    """채널별매출순위변동 — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['채널명'], '상품별 총 주문금액', '총매출액')
    g = ops.add_rank(g, '총매출액', asc=False, rank_col='순위')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['채널명', '총매출액', '순위']]

def mA1_009(df: pd.DataFrame, params: MetricParams):
    """채널내업체매출순위변동 — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['채널명', '업체명'], '상품별 총 주문금액', '총매출액')
    g = ops.add_rank(g, '총매출액', asc=False, rank_col='순위')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['채널명', '업체명', '총매출액', '순위']]

def mA1_010(df: pd.DataFrame, params: MetricParams):
    """채널내상품매출순위 — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['채널명', '상품명'], '상품별 총 주문금액', '총매출액')
    g = ops.add_rank(g, '총매출액', asc=False, rank_col='순위')
    g = g.sort_values('총매출액', ascending=False).head(50).reset_index(drop=True)
    return g[['채널명', '상품명', '총매출액', '순위']]

def mA1_011(df: pd.DataFrame, params: MetricParams):
    """채널별취소율 — CSV 정의 반영 독립 구현"""
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
    g = ops.issue_rate(d, ['채널명'], r"취소")
    return g[['채널명', '총건수', '이슈건수', '비율(%)']]

def mA1_012(df: pd.DataFrame, params: MetricParams):
    """채널별반품율 — CSV 정의 반영 독립 구현"""
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
    g = ops.issue_rate(d, ['채널명'], r"반품")
    return g[['채널명', '총건수', '이슈건수', '비율(%)']]

def mA1_013(df: pd.DataFrame, params: MetricParams):
    """채널별클레임율 — CSV 정의 반영 독립 구현"""
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
    g = ops.issue_rate(d, ['채널명'], r"취소|반품|교환|클레임")
    return g[['채널명', '총건수', '이슈건수', '비율(%)']]

def mA1_014(df: pd.DataFrame, params: MetricParams):
    """채널딜기여도 — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['채널명'], '상품별 총 주문금액', '상품별 총 주문금액')
    tot = g['상품별 총 주문금액'].sum()
    g['기여도(%)'] = (g['상품별 총 주문금액']/(tot if tot else 1)*100).round(2)
    g['누적기여도(%)'] = g['기여도(%)'].cumsum().round(2)
    g = g.sort_values('상품별 총 주문금액', ascending=False).reset_index(drop=True)
    return g[['채널명', '상품별 총 주문금액', '기여도(%)', '누적기여도(%)']]

