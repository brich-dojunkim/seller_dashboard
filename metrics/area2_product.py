from __future__ import annotations
import pandas as pd
from core.types import MetricParams
from core import base_metrics

def mA2_001(df: pd.DataFrame, params: MetricParams):
    """상품별매출순위 - 상품별 매출액 순위"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_sum(d, ['상품명'], '상품별 총 주문금액', '총매출액')
    g = base_metrics.add_rank(g, '총매출액', asc=False, rank_col='순위')
    return g.head(params.get('top_n', 30))[['상품명', '총매출액', '순위']]

def mA2_002(df: pd.DataFrame, params: MetricParams):
    """상품별주문수순위 - 상품별 주문건수 순위"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_size(d, ['상품명'], '주문건수')
    g = base_metrics.add_rank(g, '주문건수', asc=False, rank_col='순위')
    return g.head(params.get('top_n', 30))[['상품명', '주문건수', '순위']]

def mA2_003(df: pd.DataFrame, params: MetricParams):
    """히트상품기여도 - 상품별 매출 기여도"""
    d = base_metrics.apply_params_filter(df, params)
    contrib = base_metrics.contribution_calculator(
        d, '상품별 총 주문금액', group_by='상품명'
    )
    return contrib.head(20)

def mA2_004(df: pd.DataFrame, params: MetricParams):
    """상품별취소율 - 상품별 취소율"""
    d = base_metrics.apply_params_filter(df, params)
    return base_metrics.issue_rate(d, ['상품명'], r"취소")

def mA2_005(df: pd.DataFrame, params: MetricParams):
    """상품별반품율 - 상품별 반품율"""
    d = base_metrics.apply_params_filter(df, params)
    return base_metrics.issue_rate(d, ['상품명'], r"반품")

def mA2_006(df: pd.DataFrame, params: MetricParams):
    """상품매출성장률 - 상품별 매출액 성장률"""
    d = base_metrics.apply_params_filter(df, params)
    
    # 이전 기간 데이터 계산
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
        d, prev, '상품별 총 주문금액', group_by='상품명'
    )

def mA2_007(df: pd.DataFrame, params: MetricParams):
    """상품매출순위변동 - 상품별 매출 순위 변동"""
    return mA2_001(df, params)  # 현재 순위 표시

def mA2_008(df: pd.DataFrame, params: MetricParams):
    """상품가격대별매출분포 - 상품 가격대별 매출 분포"""
    d = base_metrics.apply_params_filter(df, params)
    
    if '상품가격' not in d.columns:
        return pd.DataFrame({'구간': ['데이터없음'], '매출액': [0], '비율(%)': [0]})
    
    # 가격대 구간 설정
    d = d[d['상품가격'].notna() & (d['상품가격'] > 0)].copy()
    if len(d) == 0:
        return pd.DataFrame({'구간': ['데이터없음'], '매출액': [0], '비율(%)': [0]})
    
    # 5개 구간으로 분할
    d['가격대구간'] = pd.cut(d['상품가격'], bins=5, labels=['저가', '중저가', '중간', '중고가', '고가'])
    
    result = d.groupby('가격대구간')['상품별 총 주문금액'].agg(['sum', 'count']).reset_index()
    result.columns = ['구간', '매출액', '건수']
    result['비율(%)'] = (result['매출액'] / result['매출액'].sum() * 100).round(2)
    
    return result.sort_values('매출액', ascending=False)

def mA2_009(df: pd.DataFrame, params: MetricParams):
    """상품별클레임율 - 상품별 종합 클레임율"""
    d = base_metrics.apply_params_filter(df, params)
    return base_metrics.issue_rate(d, ['상품명'], r"취소|반품|교환|클레임")

def mA2_010(df: pd.DataFrame, params: MetricParams):
    """상품별재구매기여도 - 상품별 재구매 유도 기여도"""
    d = base_metrics.apply_params_filter(df, params)
    
    # 고유구매자별 구매 횟수 계산
    if '고유구매자' not in d.columns:
        d = base_metrics.create_unique_buyer(d)
    
    # 재구매 고객들의 상품별 구매 패턴 분석
    customer_product_counts = d.groupby(['고유구매자', '상품명']).size().reset_index(name='구매횟수')
    repeat_customers = customer_product_counts[customer_product_counts['구매횟수'] >= 2]['고유구매자'].unique()
    
    # 재구매 고객들의 상품별 매출 기여도
    repeat_customer_data = d[d['고유구매자'].isin(repeat_customers)]
    
    if len(repeat_customer_data) == 0:
        return pd.DataFrame({'상품명': ['데이터없음'], '재구매매출액': [0], '기여도(%)': [0]})
    
    contrib = base_metrics.contribution_calculator(
        repeat_customer_data, '상품별 총 주문금액', group_by='상품명'
    )
    
    return contrib.head(20)