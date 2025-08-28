from __future__ import annotations
import pandas as pd
from core.types import MetricParams
from core import base_metrics

def mA5_001(df: pd.DataFrame, params: MetricParams):
    """카테고리중분류별매출 - 중분류별 매출액 집계"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_sum(d, ['중분류코드'], '상품별 총 주문금액', '총매출액')
    return g.sort_values('총매출액', ascending=False)[['중분류코드', '총매출액']]

def mA5_002(df: pd.DataFrame, params: MetricParams):
    """카테고리중분류별업체매출순위 - 중분류 내 업체별 매출 순위"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_sum(d, ['중분류코드', '업체명'], '상품별 총 주문금액', '총매출액')
    g = base_metrics.add_rank(g, '총매출액', asc=False, rank_col='순위')
    return g.head(params.get('top_n', 30))[['중분류코드', '업체명', '총매출액', '순위']]

def mA5_003(df: pd.DataFrame, params: MetricParams):
    """카테고리중분류별상품매출순위 - 중분류 내 상품별 매출 순위"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_sum(d, ['중분류코드', '상품명'], '상품별 총 주문금액', '총매출액')
    g = base_metrics.add_rank(g, '총매출액', asc=False, rank_col='순위')
    return g.head(params.get('top_n', 50))[['중분류코드', '상품명', '총매출액', '순위']]

def mA5_004(df: pd.DataFrame, params: MetricParams):
    """카테고리중분류별업체시장점유율 - 중분류 내 업체별 점유율"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_sum(d, ['중분류코드', '업체명'], '상품별 총 주문금액', '총매출액')
    
    category_totals = g.groupby('중분류코드')['총매출액'].sum().reset_index(name='중분류총매출')
    g = g.merge(category_totals, on='중분류코드')
    g['점유율(%)'] = (g['총매출액'] / g['중분류총매출'] * 100).round(2)
    
    return g[['중분류코드', '업체명', '총매출액', '점유율(%)']].sort_values(['중분류코드', '점유율(%)'], ascending=[True, False])

def mA5_005(df: pd.DataFrame, params: MetricParams):
    """카테고리중분류별주문수 - 중분류별 주문건수 집계"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_size(d, ['중분류코드'], '주문건수')
    return g.sort_values('주문건수', ascending=False)[['중분류코드', '주문건수']]

def mA5_006(df: pd.DataFrame, params: MetricParams):
    """카테고리중분류별평균주문금액 - 중분류별 평균주문금액"""
    d = base_metrics.apply_params_filter(df, params)
    
    revenue = d.groupby('중분류코드')['상품별 총 주문금액'].sum().reset_index(name='총매출액')
    orders = d.groupby('중분류코드').size().reset_index(name='주문건수')
    
    result = pd.merge(revenue, orders, on='중분류코드')
    result['평균주문금액'] = (result['총매출액'] / result['주문건수']).round(0)
    
    return result[['중분류코드', '평균주문금액']].sort_values('평균주문금액', ascending=False)

def mA5_007(df: pd.DataFrame, params: MetricParams):
    """카테고리중분류별매출순위변동 - 중분류별 매출 순위 변동"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_sum(d, ['중분류코드'], '상품별 총 주문금액', '총매출액')
    g = base_metrics.add_rank(g, '총매출액', asc=False, rank_col='순위')
    return g.sort_values('순위')[['중분류코드', '총매출액', '순위']]

def mA5_008(df: pd.DataFrame, params: MetricParams):
    """카테고리중분류별점유율 - 중분류별 매출 점유율"""
    d = base_metrics.apply_params_filter(df, params)
    return base_metrics.market_share_calculator(
        d, '상품별 총 주문금액', group_by='중분류코드'
    )

def mA5_009(df: pd.DataFrame, params: MetricParams):
    """카테고리중분류별점유율추세 - 중분류별 일자별 점유율 추이"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_sum(d, ['중분류코드', '주문일'], '상품별 총 주문금액', '총매출액')
    
    # 일자별 전체 매출 계산
    daily_totals = g.groupby('주문일')['총매출액'].sum().reset_index(name='일별총매출')
    g = g.merge(daily_totals, on='주문일')
    g['점유율(%)'] = (g['총매출액'] / g['일별총매출'] * 100).round(2)
    
    return g[['중분류코드', '주문일', '총매출액', '점유율(%)']].sort_values(['주문일', '점유율(%)'], ascending=[True, False])

def mA5_010(df: pd.DataFrame, params: MetricParams):
    """카테고리중분류Top5 - 상위 5개 중분류"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_sum(d, ['중분류코드'], '상품별 총 주문금액', '총매출액')
    return g.sort_values('총매출액', ascending=False).head(5)[['중분류코드', '총매출액']]

def mA5_011(df: pd.DataFrame, params: MetricParams):
    """카테고리중분류Top5성장률 - 상위 5개 중분류의 성장률"""
    # 먼저 상위 5개 중분류 선정
    top5 = mA5_010(df, params)
    top5_codes = top5['중분류코드'].tolist()
    
    d = base_metrics.apply_params_filter(df, params)
    d = d[d['중분류코드'].isin(top5_codes)]
    
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
        prev = prev[prev['중분류코드'].isin(top5_codes)]
    
    return base_metrics.change_analyzer(
        d, prev, '상품별 총 주문금액', group_by='중분류코드'
    )

def mA5_012(df: pd.DataFrame, params: MetricParams):
    """카테고리소분류별매출 - 소분류별 매출액 집계"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_sum(d, ['카테고리'], '상품별 총 주문금액', '총매출액')
    return g.sort_values('총매출액', ascending=False)[['카테고리', '총매출액']]

def mA5_013(df: pd.DataFrame, params: MetricParams):
    """카테고리소분류별업체매출순위 - 소분류 내 업체별 매출 순위"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_sum(d, ['카테고리', '업체명'], '상품별 총 주문금액', '총매출액')
    g = base_metrics.add_rank(g, '총매출액', asc=False, rank_col='순위')
    return g.head(params.get('top_n', 30))[['카테고리', '업체명', '총매출액', '순위']]

def mA5_014(df: pd.DataFrame, params: MetricParams):
    """카테고리소분류별상품매출순위 - 소분류 내 상품별 매출 순위"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_sum(d, ['카테고리', '상품명'], '상품별 총 주문금액', '총매출액')
    g = base_metrics.add_rank(g, '총매출액', asc=False, rank_col='순위')
    return g.head(params.get('top_n', 50))[['카테고리', '상품명', '총매출액', '순위']]

def mA5_015(df: pd.DataFrame, params: MetricParams):
    """카테고리소분류별업체시장점유율 - 소분류 내 업체별 점유율"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_sum(d, ['카테고리', '업체명'], '상품별 총 주문금액', '총매출액')
    
    category_totals = g.groupby('카테고리')['총매출액'].sum().reset_index(name='카테고리총매출')
    g = g.merge(category_totals, on='카테고리')
    g['점유율(%)'] = (g['총매출액'] / g['카테고리총매출'] * 100).round(2)
    
    return g[['카테고리', '업체명', '총매출액', '점유율(%)']].sort_values(['카테고리', '점유율(%)'], ascending=[True, False])

def mA5_016(df: pd.DataFrame, params: MetricParams):
    """카테고리소분류별주문수 - 소분류별 주문건수 집계"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_size(d, ['카테고리'], '주문건수')
    return g.sort_values('주문건수', ascending=False)[['카테고리', '주문건수']]

def mA5_017(df: pd.DataFrame, params: MetricParams):
    """카테고리소분류별평균주문금액 - 소분류별 평균주문금액"""
    d = base_metrics.apply_params_filter(df, params)
    
    revenue = d.groupby('카테고리')['상품별 총 주문금액'].sum().reset_index(name='총매출액')
    orders = d.groupby('카테고리').size().reset_index(name='주문건수')
    
    result = pd.merge(revenue, orders, on='카테고리')
    result['평균주문금액'] = (result['총매출액'] / result['주문건수']).round(0)
    
    return result[['카테고리', '평균주문금액']].sort_values('평균주문금액', ascending=False)

def mA5_018(df: pd.DataFrame, params: MetricParams):
    """카테고리소분류별매출순위변동 - 소분류별 매출 순위 변동"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_sum(d, ['카테고리'], '상품별 총 주문금액', '총매출액')
    g = base_metrics.add_rank(g, '총매출액', asc=False, rank_col='순위')
    return g.sort_values('순위')[['카테고리', '총매출액', '순위']]

def mA5_019(df: pd.DataFrame, params: MetricParams):
    """카테고리소분류별점유율 - 소분류별 매출 점유율"""
    d = base_metrics.apply_params_filter(df, params)
    return base_metrics.market_share_calculator(
        d, '상품별 총 주문금액', group_by='카테고리'
    )

def mA5_020(df: pd.DataFrame, params: MetricParams):
    """카테고리소분류별점유율추세 - 소분류별 일자별 점유율 추이"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_sum(d, ['카테고리', '주문일'], '상품별 총 주문금액', '총매출액')
    
    daily_totals = g.groupby('주문일')['총매출액'].sum().reset_index(name='일별총매출')
    g = g.merge(daily_totals, on='주문일')
    g['점유율(%)'] = (g['총매출액'] / g['일별총매출'] * 100).round(2)
    
    return g[['카테고리', '주문일', '총매출액', '점유율(%)']].sort_values(['주문일', '점유율(%)'], ascending=[True, False])

def mA5_021(df: pd.DataFrame, params: MetricParams):
    """카테고리소분류Top5 - 상위 5개 소분류"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_sum(d, ['카테고리'], '상품별 총 주문금액', '총매출액')
    return g.sort_values('총매출액', ascending=False).head(5)[['카테고리', '총매출액']]

def mA5_022(df: pd.DataFrame, params: MetricParams):
    """카테고리소분류Top5성장률 - 상위 5개 소분류의 성장률"""
    # 먼저 상위 5개 소분류 선정
    top5 = mA5_021(df, params)
    top5_categories = top5['카테고리'].tolist()
    
    d = base_metrics.apply_params_filter(df, params)
    d = d[d['카테고리'].isin(top5_categories)]
    
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
        prev = prev[prev['카테고리'].isin(top5_categories)]
    
    return base_metrics.change_analyzer(
        d, prev, '상품별 총 주문금액', group_by='카테고리'
    )