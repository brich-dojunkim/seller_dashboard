from __future__ import annotations
import pandas as pd
from core.types import MetricParams
from core import base_metrics

def mA4_001(df: pd.DataFrame, params: MetricParams):
    """업체별총매출액 - 업체별 총매출액"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_sum(d, ['업체명'], '상품별 총 주문금액', '총매출액')
    return g.sort_values('총매출액', ascending=False)[['업체명', '총매출액']]

def mA4_002(df: pd.DataFrame, params: MetricParams):
    """업체별총주문수 - 업체별 총주문건수"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_size(d, ['업체명'], '총주문수')
    return g.sort_values('총주문수', ascending=False)[['업체명', '총주문수']]

def mA4_003(df: pd.DataFrame, params: MetricParams):
    """업체별평균주문금액 - 업체별 평균주문금액"""
    d = base_metrics.apply_params_filter(df, params)
    
    # 업체별 매출액과 주문수 계산
    revenue = d.groupby('업체명')['상품별 총 주문금액'].sum().reset_index(name='총매출액')
    orders = d.groupby('업체명').size().reset_index(name='총주문수')
    
    result = pd.merge(revenue, orders, on='업체명')
    result['평균주문금액'] = (result['총매출액'] / result['총주문수']).round(0)
    
    return result[['업체명', '평균주문금액', '총매출액', '총주문수']].sort_values('평균주문금액', ascending=False)

def mA4_004(df: pd.DataFrame, params: MetricParams):
    """업체별총수익액 - 업체별 총수익액 (정산금액 기준)"""
    d = base_metrics.apply_params_filter(df, params)
    
    if '정산금액' not in d.columns:
        return pd.DataFrame({'업체명': ['데이터없음'], '총수익액': [0]})
    
    g = d.groupby('업체명')['정산금액'].sum().reset_index(name='총수익액')
    return g.sort_values('총수익액', ascending=False)[['업체명', '총수익액']]

def mA4_005(df: pd.DataFrame, params: MetricParams):
    """업체별총수량 - 업체별 총수량"""
    d = base_metrics.apply_params_filter(df, params)
    
    if '수량' not in d.columns:
        return pd.DataFrame({'업체명': ['데이터없음'], '총수량': [0]})
    
    g = d.groupby('업체명')['수량'].sum().reset_index(name='총수량')
    return g.sort_values('총수량', ascending=False)[['업체명', '총수량']]

def mA4_006(df: pd.DataFrame, params: MetricParams):
    """업체별매출순위 - 업체별 매출액 기준 순위"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_sum(d, ['업체명'], '상품별 총 주문금액', '총매출액')
    g = base_metrics.add_rank(g, '총매출액', asc=False, rank_col='순위')
    return g.head(params.get('top_n', 30))[['업체명', '총매출액', '순위']]

def mA4_007(df: pd.DataFrame, params: MetricParams):
    """업체별매출성장률(전기대비) - 전기 대비 업체 매출 변동률"""
    d = base_metrics.apply_params_filter(df, params)
    
    date_from = params.get('date_from')
    date_to = params.get('date_to')
    if date_from is None or date_to is None:
        prev = d.copy()
    else:
        period = date_to - date_from
        prev_from = date_from - period
        prev_to = date_from - pd.Timedelta(seconds=1)
        prev = df[(df['주문일시']>=prev_from) & (df['주문일시']<=prev_to)].copy()
        prev = base_metrics.apply_params_filter(prev, {**params, 'date_from': None, 'date_to': None})
    
    return base_metrics.change_analyzer(
        d, prev, '상품별 총 주문금액', group_by='업체명'
    )

def mA4_008(df: pd.DataFrame, params: MetricParams):
    """업체매출성장률 - 업체별 매출 성장률"""
    return mA4_007(df, params)

def mA4_009(df: pd.DataFrame, params: MetricParams):
    """업체별매출순위변동 - 업체별 매출순위 변동"""
    return mA4_006(df, params)

def mA4_010(df: pd.DataFrame, params: MetricParams):
    """업체별평균마진율 - 업체별 평균 마진율 (수익률)"""
    d = base_metrics.apply_params_filter(df, params)
    
    if '정산금액' not in d.columns:
        return pd.DataFrame({'업체명': ['데이터없음'], '마진율(%)': [0]})
    
    profit = d.groupby('업체명')['정산금액'].sum().reset_index(name='총수익액')
    revenue = d.groupby('업체명')['상품별 총 주문금액'].sum().reset_index(name='총매출액')
    
    result = pd.merge(profit, revenue, on='업체명')
    result['마진율(%)'] = (result['총수익액'] / result['총매출액'].replace(0, 1) * 100).round(2)
    
    return result[['업체명', '마진율(%)', '총수익액', '총매출액']].sort_values('마진율(%)', ascending=False)

def mA4_011(df: pd.DataFrame, params: MetricParams):
    """업체별마진순위 - 업체별 마진율 기준 순위"""
    margin_data = mA4_010(df, params)
    if '마진율(%)' in margin_data.columns:
        margin_data = base_metrics.add_rank(margin_data, '마진율(%)', asc=False, rank_col='순위')
        return margin_data.head(params.get('top_n', 50))[['업체명', '마진율(%)', '순위']]
    return margin_data

def mA4_012(df: pd.DataFrame, params: MetricParams):
    """업체별취소율 - 업체별 취소율"""
    d = base_metrics.apply_params_filter(df, params)
    return base_metrics.issue_rate(d, ['업체명'], r"취소")

def mA4_013(df: pd.DataFrame, params: MetricParams):
    """업체별반품율 - 업체별 반품률"""
    d = base_metrics.apply_params_filter(df, params)
    return base_metrics.issue_rate(d, ['업체명'], r"반품")

def mA4_014(df: pd.DataFrame, params: MetricParams):
    """업체별교환율 - 업체별 교환율"""
    d = base_metrics.apply_params_filter(df, params)
    return base_metrics.issue_rate(d, ['업체명'], r"교환")

def mA4_015(df: pd.DataFrame, params: MetricParams):
    """업체별클레임율 - 업체별 종합 클레임율"""
    d = base_metrics.apply_params_filter(df, params)
    return base_metrics.issue_rate(d, ['업체명'], r"취소|반품|교환|클레임")