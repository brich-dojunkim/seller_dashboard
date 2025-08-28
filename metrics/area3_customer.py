from __future__ import annotations
import pandas as pd
from core.types import MetricParams
# Auto-rewrite from metrics CSV

def mA3_001(df: pd.DataFrame, params: MetricParams):
    """총고객수 — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['_전체'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['총매출액']]

def mA3_002(df: pd.DataFrame, params: MetricParams):
    """재구매율 — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['_전체'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['총매출액']]

def mA3_003(df: pd.DataFrame, params: MetricParams):
    """상위10퍼고객매출비중 — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['_전체'], '상품별 총 주문금액', '상품별 총 주문금액')
    tot = g['상품별 총 주문금액'].sum()
    g['기여도(%)'] = (g['상품별 총 주문금액']/(tot if tot else 1)*100).round(2)
    g['누적기여도(%)'] = g['기여도(%)'].cumsum().round(2)
    g = g.sort_values('상품별 총 주문금액', ascending=False).reset_index(drop=True)
    g = g.head(10)
    return g[['상품별 총 주문금액', '기여도(%)', '누적기여도(%)']]

def mA3_004(df: pd.DataFrame, params: MetricParams):
    """다중구매고객비율 — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['_전체'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['총매출액']]

def mA3_005(df: pd.DataFrame, params: MetricParams):
    """고객당평균구매상품수 — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['상품명'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['상품명', '총매출액']]

def mA3_006(df: pd.DataFrame, params: MetricParams):
    """신규고객비율 — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['_전체'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['총매출액']]

def mA3_007(df: pd.DataFrame, params: MetricParams):
    """기존고객이탈률 — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['_전체'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['총매출액']]

def mA3_008(df: pd.DataFrame, params: MetricParams):
    """재구매고객매출비중 — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['_전체'], '상품별 총 주문금액', '상품별 총 주문금액')
    tot = g['상품별 총 주문금액'].sum()
    g['기여도(%)'] = (g['상품별 총 주문금액']/(tot if tot else 1)*100).round(2)
    g['누적기여도(%)'] = g['기여도(%)'].cumsum().round(2)
    g = g.sort_values('상품별 총 주문금액', ascending=False).reset_index(drop=True)
    return g[['상품별 총 주문금액', '기여도(%)', '누적기여도(%)']]

def mA3_009(df: pd.DataFrame, params: MetricParams):
    """고객재구매횟수분포 — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['_전체'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['총매출액']]

def mA3_010(df: pd.DataFrame, params: MetricParams):
    """고객평균LTV — CSV 정의 반영 독립 구현"""
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
    g = ops.safe_group_sum(d, ['_전체'], '상품별 총 주문금액', '총매출액')
    g = g.sort_values('총매출액', ascending=False).reset_index(drop=True)
    return g[['총매출액']]

