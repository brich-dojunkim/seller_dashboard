from __future__ import annotations
import pandas as pd
from core.types import MetricParams
from core import base_metrics

def mA3_001(df: pd.DataFrame, params: MetricParams):
    """총고객수 - 고유구매자 수"""
    d = base_metrics.apply_params_filter(df, params)
    total_customers = base_metrics.total_customers(d)
    return pd.DataFrame({'총고객수': [total_customers]})

def mA3_002(df: pd.DataFrame, params: MetricParams):
    """재구매율 - 2회 이상 구매 고객 비율"""
    d = base_metrics.apply_params_filter(df, params)
    repurchase_rate = base_metrics.repurchase_rate(d)
    return pd.DataFrame({'재구매율(%)': [repurchase_rate]})

def mA3_003(df: pd.DataFrame, params: MetricParams):
    """상위10퍼고객매출비중 - 상위 10% 고객의 매출 기여도"""
    d = base_metrics.apply_params_filter(df, params)
    
    if '고유구매자' not in d.columns:
        d = base_metrics.create_unique_buyer(d)
    
    # 고객별 매출액 계산
    customer_revenue = d.groupby('고유구매자')['상품별 총 주문금액'].sum().reset_index()
    customer_revenue.columns = ['고유구매자', '매출액']
    customer_revenue = customer_revenue.sort_values('매출액', ascending=False)
    
    # 상위 10% 고객
    total_customers = len(customer_revenue)
    top_10_percent_count = max(1, int(total_customers * 0.1))
    top_customers = customer_revenue.head(top_10_percent_count)
    
    # 기여도 계산
    top_revenue = top_customers['매출액'].sum()
    total_revenue = customer_revenue['매출액'].sum()
    contribution = round(top_revenue / total_revenue * 100, 2) if total_revenue > 0 else 0
    
    return pd.DataFrame({
        '상위10%고객수': [top_10_percent_count],
        '전체고객수': [total_customers],
        '상위10%매출기여도(%)': [contribution],
        '상위10%매출액': [top_revenue],
        '전체매출액': [total_revenue]
    })

def mA3_004(df: pd.DataFrame, params: MetricParams):
    """다중구매고객비율 - 여러 상품 구매 고객 비율"""
    d = base_metrics.apply_params_filter(df, params)
    
    if '고유구매자' not in d.columns:
        d = base_metrics.create_unique_buyer(d)
    
    # 고객별 구매 상품 수
    customer_products = d.groupby('고유구매자')['상품명'].nunique().reset_index()
    customer_products.columns = ['고유구매자', '구매상품수']
    
    # 2개 이상 상품 구매 고객
    multi_product_customers = len(customer_products[customer_products['구매상품수'] >= 2])
    total_customers = len(customer_products)
    
    ratio = round(multi_product_customers / total_customers * 100, 2) if total_customers > 0 else 0
    
    return pd.DataFrame({
        '다중구매고객수': [multi_product_customers],
        '전체고객수': [total_customers],
        '다중구매고객비율(%)': [ratio]
    })

def mA3_005(df: pd.DataFrame, params: MetricParams):
    """고객당평균구매상품수 - 고객별 평균 구매 상품수"""
    d = base_metrics.apply_params_filter(df, params)
    
    if '고유구매자' not in d.columns:
        d = base_metrics.create_unique_buyer(d)
    
    customer_products = d.groupby('고유구매자')['상품명'].nunique().reset_index()
    customer_products.columns = ['고유구매자', '구매상품수']
    
    avg_products = customer_products['구매상품수'].mean()
    
    return pd.DataFrame({
        '전체고객수': [len(customer_products)],
        '고객당평균구매상품수': [round(avg_products, 2)]
    })

def mA3_006(df: pd.DataFrame, params: MetricParams):
    """신규고객비율 - 분석 기간 내 신규 고객 비율"""
    d = base_metrics.apply_params_filter(df, params)
    
    if '고유구매자' not in d.columns:
        d = base_metrics.create_unique_buyer(d)
    
    all_customers = df.groupby('고유구매자')['주문일시'].min().reset_index()
    all_customers.columns = ['고유구매자', '첫구매일']
    
    analysis_start = params.get('date_from')
    if analysis_start:
        new_customers = all_customers[all_customers['첫구매일'] >= analysis_start]
        new_customer_count = len(new_customers)
    else:
        new_customer_count = 0
    
    period_customers = d['고유구매자'].nunique()
    ratio = round(new_customer_count / period_customers * 100, 2) if period_customers > 0 else 0
    
    return pd.DataFrame({
        '신규고객수': [new_customer_count],
        '기간내전체고객수': [period_customers],
        '신규고객비율(%)': [ratio]
    })

def mA3_007(df: pd.DataFrame, params: MetricParams):
    """기존고객이탈률 - 일정 기간 구매 중단 고객 비율"""
    d = base_metrics.apply_params_filter(df, params)
    churn_days = 90
    
    if '고유구매자' not in d.columns:
        d = base_metrics.create_unique_buyer(d)
    
    all_recent_purchase = df.groupby('고유구매자')['주문일시'].max().reset_index()
    all_recent_purchase.columns = ['고유구매자', '최근구매일']
    
    analysis_date = df['주문일시'].max()
    all_recent_purchase['경과일수'] = (analysis_date - all_recent_purchase['최근구매일']).dt.days
    churned_customers = all_recent_purchase[all_recent_purchase['경과일수'] > churn_days]
    
    churn_rate = round(len(churned_customers) / len(all_recent_purchase) * 100, 2)
    
    return pd.DataFrame({
        '전체고객수': [len(all_recent_purchase)],
        '이탈고객수': [len(churned_customers)],
        '이탈률(%)': [churn_rate],
        '기준일수': [churn_days]
    })

def mA3_008(df: pd.DataFrame, params: MetricParams):
    """재구매고객매출비중 - 재구매 고객의 매출 기여도"""
    d = base_metrics.apply_params_filter(df, params)
    
    if '고유구매자' not in d.columns:
        d = base_metrics.create_unique_buyer(d)
    
    customer_orders = d.groupby('고유구매자').size().reset_index(name='구매횟수')
    repeat_customers = customer_orders[customer_orders['구매횟수'] >= 2]['고유구매자'].tolist()
    
    repeat_customer_revenue = d[d['고유구매자'].isin(repeat_customers)]['상품별 총 주문금액'].sum()
    total_revenue = d['상품별 총 주문금액'].sum()
    
    contribution = round(repeat_customer_revenue / total_revenue * 100, 2) if total_revenue > 0 else 0
    
    return pd.DataFrame({
        '재구매고객수': [len(repeat_customers)],
        '전체고객수': [len(customer_orders)],
        '재구매고객매출액': [repeat_customer_revenue],
        '전체매출액': [total_revenue],
        '재구매고객매출비중(%)': [contribution]
    })

def mA3_009(df: pd.DataFrame, params: MetricParams):
    """고객재구매횟수분포 - 고객별 재구매 횟수 분포"""
    d = base_metrics.apply_params_filter(df, params)
    
    if '고유구매자' not in d.columns:
        d = base_metrics.create_unique_buyer(d)
    
    customer_orders = d.groupby('고유구매자').size().reset_index(name='구매횟수')
    
    bins = [0, 1, 2, 3, 4, float('inf')]
    labels = ['1회', '2회', '3회', '4회', '5회이상']
    customer_orders['구매구간'] = pd.cut(customer_orders['구매횟수'], bins=bins, labels=labels, right=False)
    
    distribution = customer_orders.groupby('구매구간').size().reset_index(name='고객수')
    distribution['비율(%)'] = (distribution['고객수'] / distribution['고객수'].sum() * 100).round(2)
    
    return distribution

def mA3_010(df: pd.DataFrame, params: MetricParams):
    """고객평균LTV - 고객별 평균 생애가치"""
    d = base_metrics.apply_params_filter(df, params)
    
    if '고유구매자' not in d.columns:
        d = base_metrics.create_unique_buyer(d)
    
    customer_summary = d.groupby('고유구매자').agg({
        '상품별 총 주문금액': ['sum', 'mean', 'count'],
        '주문일시': ['min', 'max']
    }).reset_index()
    
    customer_summary.columns = [
        '고유구매자', '총구매금액', '평균구매금액', '구매횟수', '첫구매일', '최근구매일'
    ]
    
    customer_summary['구매기간(일)'] = (customer_summary['최근구매일'] - customer_summary['첫구매일']).dt.days
    
    customer_summary['구매주기(일)'] = customer_summary.apply(
        lambda row: row['구매기간(일)'] / (row['구매횟수'] - 1) if row['구매횟수'] > 1 else 0,
        axis=1
    ).round(1)
    
    expected_lifetime_days = 365 * 2
    customer_summary['예상LTV'] = customer_summary.apply(
        lambda row: row['평균구매금액'] * (expected_lifetime_days / row['구매주기(일)']) 
        if row['구매주기(일)'] > 0 else row['총구매금액'],
        axis=1
    ).round(0)
    
    customer_summary['현재LTV'] = customer_summary['총구매금액']
    
    avg_current_ltv = customer_summary['현재LTV'].mean().round(0)
    avg_predicted_ltv = customer_summary['예상LTV'].mean().round(0)
    
    return pd.DataFrame({
        '전체고객수': [len(customer_summary)],
        '평균현재LTV': [avg_current_ltv],
        '평균예상LTV': [avg_predicted_ltv],
        '중간값현재LTV': [customer_summary['현재LTV'].median().round(0)],
        '최대현재LTV': [customer_summary['현재LTV'].max()],
        '최대예상LTV': [customer_summary['예상LTV'].max()]
    })