from __future__ import annotations
import pandas as pd
from core.types import MetricParams
from core import base_metrics

def mA6_001(df: pd.DataFrame, params: MetricParams):
    """트렌드일자별매출추이 - 일자별 매출액 시계열 추이"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_sum(d, ['주문일'], '상품별 총 주문금액', '매출액')
    return g.sort_values('주문일')[['주문일', '매출액']]

def mA6_002(df: pd.DataFrame, params: MetricParams):
    """트렌드일자별주문수추이 - 일자별 주문수 시계열 추이"""
    d = base_metrics.apply_params_filter(df, params)
    g = base_metrics.safe_group_size(d, ['주문일'], '주문수')
    return g.sort_values('주문일')[['주문일', '주문수']]

def mA6_003(df: pd.DataFrame, params: MetricParams):
    """트렌드주차별매출추이 - 주차별 매출액 시계열 추이"""
    d = base_metrics.filter_by_params(df, params)
    
    # 주차 파생
    d = d.copy()
    d['주차'] = pd.to_datetime(d['주문일']).dt.to_period('W').astype(str)
    
    g = d.groupby('주차')['상품별 총 주문금액'].sum().reset_index(name='매출액')
    return g.sort_values('주차')[['주차', '매출액']]

def mA6_004(df: pd.DataFrame, params: MetricParams):
    """트렌드매출성장률2주 - 2주 단위 매출 성장률"""
    # 주차별 매출 데이터 먼저 생성
    weekly_data = mA6_003(df, params)
    
    if len(weekly_data) < 2:
        return pd.DataFrame({'기간': ['데이터부족'], '매출액': [0], '성장률(%)': [0]})
    
    # 2주씩 그룹핑
    weekly_data = weekly_data.reset_index(drop=True)
    biweekly_results = []
    
    for i in range(0, len(weekly_data), 2):
        if i + 1 < len(weekly_data):
            # 2주 합계
            period_revenue = weekly_data.iloc[i:i+2]['매출액'].sum()
            period_name = f"{weekly_data.iloc[i]['주차']}~{weekly_data.iloc[i+1]['주차']}"
        else:
            # 마지막 홀수 주
            period_revenue = weekly_data.iloc[i]['매출액']
            period_name = weekly_data.iloc[i]['주차']
        
        biweekly_results.append({'기간': period_name, '매출액': period_revenue})
    
    biweekly_df = pd.DataFrame(biweekly_results)
    
    # 성장률 계산
    biweekly_df['이전기간매출'] = biweekly_df['매출액'].shift(1)
    biweekly_df['성장률(%)'] = biweekly_df.apply(
        lambda row: 0.0 if pd.isna(row['이전기간매출']) or row['이전기간매출'] == 0
        else ((row['매출액'] - row['이전기간매출']) / row['이전기간매출'] * 100),
        axis=1
    ).round(2)
    
    return biweekly_df[['기간', '매출액', '성장률(%)']].dropna()

def mA6_005(df: pd.DataFrame, params: MetricParams):
    """트렌드시간대별매출패턴 - 시간대별 매출 패턴"""
    d = base_metrics.filter_by_params(df, params)
    g = base_metrics.safe_group_sum(d, ['시간대'], '상품별 총 주문금액', '매출액')
    
    # 패턴 강도 계산 (변동계수)
    mean_revenue = g['매출액'].mean()
    std_revenue = g['매출액'].std()
    pattern_strength = round(std_revenue / mean_revenue * 100, 2) if mean_revenue > 0 else 0
    
    g = g.sort_values('시간대')
    g['패턴강도'] = pattern_strength
    
    return g[['시간대', '매출액', '패턴강도']]

def mA6_006(df: pd.DataFrame, params: MetricParams):
    """트렌드시간대별주문수패턴 - 시간대별 주문수 패턴"""
    d = base_metrics.filter_by_params(df, params)
    g = base_metrics.safe_group_size(d, ['시간대'], '주문수')
    
    # 패턴 강도 계산
    mean_orders = g['주문수'].mean()
    std_orders = g['주문수'].std()
    pattern_strength = round(std_orders / mean_orders * 100, 2) if mean_orders > 0 else 0
    
    g = g.sort_values('시간대')
    g['패턴강도'] = pattern_strength
    
    return g[['시간대', '주문수', '패턴강도']]

def mA6_007(df: pd.DataFrame, params: MetricParams):
    """트렌드피크시간대Top3 - 매출 피크 시간대 상위 3개"""
    hourly_pattern = mA6_005(df, params)
    return hourly_pattern.nlargest(3, '매출액')[['시간대', '매출액']].reset_index(drop=True)

def mA6_008(df: pd.DataFrame, params: MetricParams):
    """트렌드요일별매출패턴 - 요일별 매출 패턴"""
    d = base_metrics.filter_by_params(df, params)
    g = base_metrics.safe_group_sum(d, ['요일'], '상품별 총 주문금액', '매출액')
    
    # 요일 순서 정렬
    weekday_order = ['월', '화', '수', '목', '금', '토', '일']
    g['요일'] = pd.Categorical(g['요일'], categories=weekday_order, ordered=True)
    g = g.sort_values('요일')
    
    # 패턴 강도 계산
    mean_revenue = g['매출액'].mean()
    std_revenue = g['매출액'].std()
    pattern_strength = round(std_revenue / mean_revenue * 100, 2) if mean_revenue > 0 else 0
    
    g['패턴강도'] = pattern_strength
    return g[['요일', '매출액', '패턴강도']]

def mA6_009(df: pd.DataFrame, params: MetricParams):
    """트렌드요일패턴스코어 - 요일별 매출 패턴 강도 스코어"""
    weekday_pattern = mA6_008(df, params)
    
    if len(weekday_pattern) > 0:
        # 일관성 점수 (변동계수의 역수)
        mean_val = weekday_pattern['매출액'].mean()
        std_val = weekday_pattern['매출액'].std()
        consistency_score = round(1 / (std_val / mean_val + 0.01) * 10, 2) if mean_val > 0 else 0
        
        # 크기 점수 (평균값 정규화)
        max_val = weekday_pattern['매출액'].max()
        magnitude_score = round(mean_val / max_val * 100, 2) if max_val > 0 else 0
        
        # 종합 점수 (가중평균)
        total_score = round(consistency_score * 0.6 + magnitude_score * 0.4, 2)
        
        return pd.DataFrame({
            '일관성점수': [consistency_score],
            '크기점수': [magnitude_score], 
            '종합점수': [total_score]
        })
    
    return pd.DataFrame({'점수': ['데이터부족']})

def mA6_010(df: pd.DataFrame, params: MetricParams):
    """트렌드채널별매출추이 - 채널별 매출 시계열 추이"""
    d = base_metrics.filter_by_params(df, params)
    g = base_metrics.safe_group_sum(d, ['주문일', '채널명'], '상품별 총 주문금액', '매출액')
    return g.sort_values(['주문일', '매출액'], ascending=[True, False])[['주문일', '채널명', '매출액']]

def mA6_011(df: pd.DataFrame, params: MetricParams):
    """트렌드중분류별매출추이Top5 - 상위 5개 중분류의 매출 추이"""
    d = base_metrics.filter_by_params(df, params)
    
    # 상위 5개 중분류 선정
    top_categories = d.groupby('중분류코드')['상품별 총 주문금액'].sum().nlargest(5).index.tolist()
    d_top5 = d[d['중분류코드'].isin(top_categories)]
    
    g = base_metrics.safe_group_sum(d_top5, ['주문일', '중분류코드'], '상품별 총 주문금액', '매출액')
    return g.sort_values(['주문일', '매출액'], ascending=[True, False])[['주문일', '중분류코드', '매출액']]