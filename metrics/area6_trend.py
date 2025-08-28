from __future__ import annotations
import pandas as pd
from core.types import MetricParams
# Auto-rewrite from metrics CSV

def mA6_001(df: pd.DataFrame, params: MetricParams):
    """트렌드일자별매출추이 — CSV 정의 반영 독립 구현"""
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
    # 기간 파생
    d['_기간'] = d['주문일']
    g = d.groupby(['_기간'] + [], dropna=False)['상품별 총 주문금액'].sum().reset_index(name='값')
    g = g.sort_values('_기간').reset_index(drop=True)
    return g[['_기간', '값']]

def mA6_002(df: pd.DataFrame, params: MetricParams):
    """트렌드일자별주문수추이 — CSV 정의 반영 독립 구현"""
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
    # 기간 파생
    d['_기간'] = d['주문일']
    g = d.groupby(['_기간'] + [], dropna=False)['상품별 총 주문금액'].sum().reset_index(name='값')
    g = g.sort_values('_기간').reset_index(drop=True)
    return g[['_기간', '값']]

def mA6_003(df: pd.DataFrame, params: MetricParams):
    """트렌드주차별매출추이 — CSV 정의 반영 독립 구현"""
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
    # 기간 파생
    d['_기간'] = pd.to_datetime(d['주문일']).dt.to_period('W').astype(str)
    g = d.groupby(['_기간'] + [], dropna=False)['상품별 총 주문금액'].sum().reset_index(name='값')
    g = g.sort_values('_기간').reset_index(drop=True)
    return g[['_기간', '값']]

def mA6_004(df: pd.DataFrame, params: MetricParams):
    """트렌드매출성장률2주 — CSV 정의 반영 독립 구현"""
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
    # 기간 파생
    d['_기간'] = pd.to_datetime(d['주문일']).dt.to_period('W').astype(str)
    g = d.groupby(['_기간'] + [], dropna=False)['상품별 총 주문금액'].sum().reset_index(name='값')
    g = g.sort_values('_기간').reset_index(drop=True)
    return g[['_기간', '값']]

def mA6_005(df: pd.DataFrame, params: MetricParams):
    """트렌드시간대별매출패턴 — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['시간대'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['시간대', '총매출액']]

def mA6_006(df: pd.DataFrame, params: MetricParams):
    """트렌드시간대별주문수패턴 — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['시간대'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['시간대', '총매출액']]

def mA6_007(df: pd.DataFrame, params: MetricParams):
    """트렌드피크시간대Top3 — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['시간대'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['시간대', '총매출액']]

def mA6_008(df: pd.DataFrame, params: MetricParams):
    """트렌드요일별매출패턴 — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['요일'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['요일', '총매출액']]

def mA6_009(df: pd.DataFrame, params: MetricParams):
    """트렌드요일패턴스코어 — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['요일'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['요일', '총매출액']]

def mA6_010(df: pd.DataFrame, params: MetricParams):
    """트렌드채널별매출추이 — CSV 정의 반영 독립 구현"""
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
    # 기간 파생
    d['_기간'] = d['주문일']
    g = d.groupby(['_기간'] + ['채널명'], dropna=False)['상품별 총 주문금액'].sum().reset_index(name='값')
    g = g.sort_values('_기간').reset_index(drop=True)
    return g[['_기간', '채널명', '값']]

def mA6_011(df: pd.DataFrame, params: MetricParams):
    """트렌드중분류별매출추이Top5 — CSV 정의 반영 독립 구현"""
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
    # 기간 파생
    d['_기간'] = d['주문일']
    g = d.groupby(['_기간'] + ['중분류코드'], dropna=False)['상품별 총 주문금액'].sum().reset_index(name='값')
    g = g.sort_values('_기간').reset_index(drop=True)
    return g[['_기간', '중분류코드', '값']]

