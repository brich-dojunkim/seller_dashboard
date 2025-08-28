from __future__ import annotations
import pandas as pd
from core.types import MetricParams
from core import base_metrics

def mA1_001(df: pd.DataFrame, params: MetricParams):
    """채널별매출비중 - 채널별 매출액 점유율"""
    d = base_metrics.apply_params_filter(df, params)
    return base_metrics.market_share_calculator(
        d, '상품별 총 주문금액', group_by='채널명'
    )

def mA1_002(df: pd.DataFrame, params: MetricParams):
    """채널별주문수비중 - 채널별 주문수 점유율"""
    d = base_metrics.apply_params_filter(df, params)
    # 주문수 기반으로 점유율 계산
    channel_orders = d.groupby('채널명').size().reset_index(name='주문건수')
    total_orders = channel_orders['주문건수'].sum()
    channel_orders['점유율(%)'] = (channel_orders['주문건수'] / total_orders * 100).round(2)
    return channel_orders.sort_values('점유율(%)', ascending=False)

def mA1_003(df: pd.DataFrame, params: MetricParams):
    """채널별업체매출순위 - 채널 내 업체별 매출 순위"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_sum(d, ['채널명', '업체명'], '상품별 총 주문금액', '총매출액')
    g = base_metrics.add_rank(g, '총매출액', asc=False, rank_col='순위')
    g = g.sort_values(['채널명', '순위']).head(params.get('top_n', 30)).reset_index(drop=True)
    return g[['채널명', '업체명', '총매출액', '순위']]

def mA1_004(df: pd.DataFrame, params: MetricParams):
    """채널별업체점유율 - 채널 내 업체별 매출 점유율"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_sum(d, ['채널명', '업체명'], '상품별 총 주문금액', '총매출액')
    
    # 채널별로 점유율 계산
    channel_totals = g.groupby('채널명')['총매출액'].sum().reset_index(name='채널총매출')
    g = g.merge(channel_totals, on='채널명')
    g['점유율(%)'] = (g['총매출액'] / g['채널총매출'] * 100).round(2)
    
    return g[['채널명', '업체명', '총매출액', '점유율(%)']].sort_values(['채널명', '점유율(%)'], ascending=[True, False])

def mA1_005(df: pd.DataFrame, params: MetricParams):
    """채널매출성장률 - 채널별 매출액 성장률"""
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
        d, prev, '상품별 총 주문금액', group_by='채널명'
    )

def mA1_006(df: pd.DataFrame, params: MetricParams):
    """채널주문수성장률 - 채널별 주문수 성장률"""
    d = base_metrics.apply_params_filter(df, params)
    
    # 현재 기간 주문수
    current_orders = d.groupby('채널명').size().reset_index(name='현재주문수')
    
    # 이전 기간 주문수
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
    
    prev_orders = prev.groupby('채널명').size().reset_index(name='이전주문수')
    
    # 변동률 계산
    comparison = pd.merge(current_orders, prev_orders, on='채널명', how='outer').fillna(0)
    comparison['변동량'] = comparison['현재주문수'] - comparison['이전주문수']
    comparison['변동률(%)'] = comparison.apply(
        lambda r: (100.0 if r['이전주문수']==0 and r['현재주문수']>0 
                  else (0.0 if r['이전주문수']==0 
                       else (r['변동량']/r['이전주문수']*100))), axis=1
    ).round(2)
    
    return comparison.sort_values('변동률(%)', ascending=False)

def mA1_007(df: pd.DataFrame, params: MetricParams):
    """채널점유율추세 - 채널별 일자별 매출 추이"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_sum(d, ['채널명', '주문일'], '상품별 총 주문금액', '총매출액')
    
    # 일자별 전체 매출 계산하여 점유율 산출
    daily_totals = g.groupby('주문일')['총매출액'].sum().reset_index(name='일별총매출')
    g = g.merge(daily_totals, on='주문일')
    g['점유율(%)'] = (g['총매출액'] / g['일별총매출'] * 100).round(2)
    
    return g[['채널명', '주문일', '총매출액', '점유율(%)']].sort_values(['주문일', '점유율(%)'], ascending=[True, False])

def mA1_008(df: pd.DataFrame, params: MetricParams):
    """채널별매출순위변동 - 채널간 매출 순위 변동"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_sum(d, ['채널명'], '상품별 총 주문금액', '총매출액')
    g = base_metrics.add_rank(g, '총매출액', asc=False, rank_col='순위')
    return g[['채널명', '총매출액', '순위']].sort_values('순위')

def mA1_009(df: pd.DataFrame, params: MetricParams):
    """채널내업체매출순위변동 - 채널 내 업체별 매출 순위 변동"""
    return mA1_003(df, params)  # 기본적으로 같은 로직

def mA1_010(df: pd.DataFrame, params: MetricParams):
    """채널내상품매출순위 - 채널별 상품 매출 순위"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_sum(d, ['채널명', '상품명'], '상품별 총 주문금액', '총매출액')
    g = base_metrics.add_rank(g, '총매출액', asc=False, rank_col='순위')
    g = g.sort_values(['채널명', '순위']).head(params.get('top_n', 50)).reset_index(drop=True)
    return g[['채널명', '상품명', '총매출액', '순위']]

def mA1_011(df: pd.DataFrame, params: MetricParams):
    """채널별취소율 - 채널별 주문 취소율"""
    d = base_metrics.apply_params_filter(df, params)
    return base_metrics.issue_rate(d, ['채널명'], r"취소")

def mA1_012(df: pd.DataFrame, params: MetricParams):
    """채널별반품율 - 채널별 상품 반품률"""
    d = base_metrics.apply_params_filter(df, params)
    return base_metrics.issue_rate(d, ['채널명'], r"반품")

def mA1_013(df: pd.DataFrame, params: MetricParams):
    """채널별클레임율 - 채널별 종합 클레임율"""
    d = base_metrics.apply_params_filter(df, params)
    return base_metrics.issue_rate(d, ['채널명'], r"취소|반품|교환|클레임")

def mA1_014(df: pd.DataFrame, params: MetricParams):
    """채널딜기여도 - 채널별 매출 기여도"""
    d = base_metrics.apply_params_filter(df, params)
    return base_metrics.contribution_calculator(
        d, '상품별 총 주문금액', group_by='채널명'
    )