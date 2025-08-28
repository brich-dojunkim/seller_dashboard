from __future__ import annotations
import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# 전역 상수
DEFAULT_TABLE_NAME = 'b-flow 주문 내역'
DEFAULT_EXCLUDE_CANCELLED = True
DEFAULT_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
NUMERIC_PRECISION = 2
CATEGORY_FILE_PATH = 'brich_category_2504071.csv'

# 전역 변수
CATEGORY_DF = None

# ===== [0] 기본 - 설정 =====
def library_setup():
    """라이브러리설정"""
    return {'pandas': pd.__version__, 'numpy': np.__version__, 'status': 'loaded'}

def basic_constants_setup():
    """기본상수설정"""
    return {
        'table_name': DEFAULT_TABLE_NAME,
        'exclude_cancelled': DEFAULT_EXCLUDE_CANCELLED,
        'date_format': DEFAULT_DATE_FORMAT,
        'precision': NUMERIC_PRECISION,
        'category_file': CATEGORY_FILE_PATH
    }

def load_category_data(file_path=CATEGORY_FILE_PATH):
    """카테고리데이터로드"""
    global CATEGORY_DF
    if CATEGORY_DF is None:
        try:
            CATEGORY_DF = pd.read_csv(file_path)
            CATEGORY_DF = CATEGORY_DF[['Depth', 'Code', 'Name']].copy()
            CATEGORY_DF['Code'] = CATEGORY_DF['Code'].astype(str)
            return {'status': 'loaded', 'rows': len(CATEGORY_DF)}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    return {'status': 'already_loaded', 'rows': len(CATEGORY_DF)}

def basic_filtering(df, company_name=None, exclude_cancelled=DEFAULT_EXCLUDE_CANCELLED):
    """기본필터링"""
    filtered_df = df.copy()
    if company_name is not None:
        if isinstance(company_name, str):
            filtered_df = filtered_df[filtered_df['업체명'] == company_name]
        elif isinstance(company_name, list):
            filtered_df = filtered_df[filtered_df['업체명'].isin(company_name)]
    if exclude_cancelled:
        filtered_df = filtered_df[filtered_df['주문상태'] != '결제취소']
    return filtered_df

def apply_additional_filter(df, filter_condition=None):
    """추가필터적용"""
    if filter_condition is None:
        return df
    if isinstance(filter_condition, str):
        return df.query(filter_condition)
    elif isinstance(filter_condition, dict):
        filtered_df = df.copy()
        for column, condition in filter_condition.items():
            if isinstance(condition, list):
                filtered_df = filtered_df[filtered_df[column].isin(condition)]
            elif isinstance(condition, tuple) and len(condition) == 2:
                filtered_df = filtered_df[
                    (filtered_df[column] >= condition[0]) & 
                    (filtered_df[column] <= condition[1])
                ]
            else:
                filtered_df = filtered_df[filtered_df[column] == condition]
        return filtered_df
    elif callable(filter_condition):
        return df[filter_condition(df)]
    return df

def build_filter_condition(*conditions, operator='and'):
    """필터조건빌더"""
    if not conditions:
        return None
    valid_conditions = [cond for cond in conditions if cond and str(cond).strip()]
    if not valid_conditions:
        return None
    if len(valid_conditions) == 1:
        return str(valid_conditions[0])
    wrapped_conditions = [f"({cond})" for cond in valid_conditions]
    return ' or '.join(wrapped_conditions) if operator.lower() == 'or' else ' and '.join(wrapped_conditions)

def get_settings_info():
    """설정정보조회"""
    return {
        'basic_settings': basic_constants_setup(),
        'category_data': load_category_data(),
        'library_info': library_setup()
    }

# ===== [0] 기본 - 데이터변환 =====
def create_unique_buyer(df):
    """고유구매자생성"""
    def extract_phone_last4(phone_str):
        if pd.isna(phone_str):
            return ''
        phone_digits = re.sub(r'[^0-9]', '', str(phone_str))
        return phone_digits[-4:] if len(phone_digits) >= 4 else phone_digits
    
    df = df.copy()
    phone_last4 = df['구매자연락처'].apply(extract_phone_last4)
    df['고유구매자'] = df['구매자명'] + '_' + phone_last4
    return df

def map_middle_category_name(df, category_file_path=CATEGORY_FILE_PATH):
    """중분류명매핑"""
    load_result = load_category_data(category_file_path)
    if load_result['status'] == 'error':
        raise Exception(f"카테고리 데이터 로드 실패: {load_result['message']}")
    
    df = df.copy()
    df['정규카테고리코드'] = (df['카테고리'].astype(str)
                        .str.replace(r'\.0$', '', regex=True)
                        .str.replace(r'\D', '', regex=True))
    
    df['중분류코드'] = df['정규카테고리코드'].apply(lambda x: str(x)[:5] if len(str(x)) >= 5 else str(x))
    
    if CATEGORY_DF is not None:
        middle_categories = CATEGORY_DF[CATEGORY_DF['Depth'] == 2].copy()
        middle_mapping = dict(zip(middle_categories['Code'], middle_categories['Name']))
        
        def map_to_middle_name(code):
            if pd.isna(code) or code == '':
                return '기타'
            code_str = str(code)
            if code_str in middle_mapping:
                return middle_mapping[code_str]
            for cat_code, cat_name in middle_mapping.items():
                if code_str.zfill(5) == cat_code.zfill(5):
                    return cat_name
            return f'미분류_{code_str}'
        
        df['중분류명'] = df['중분류코드'].apply(map_to_middle_name)
    else:
        df['중분류명'] = '기타'
    
    return df

def map_sub_category_name(df, category_file_path=CATEGORY_FILE_PATH):
    """소분류명매핑"""
    load_result = load_category_data(category_file_path)
    if load_result['status'] == 'error':
        raise Exception(f"카테고리 데이터 로드 실패: {load_result['message']}")
    
    df = df.copy()
    if '정규카테고리코드' not in df.columns:
        df['정규카테고리코드'] = (df['카테고리'].astype(str)
                            .str.replace(r'\.0$', '', regex=True)
                            .str.replace(r'\D', '', regex=True))
    
    def extract_sub_code(code_str):
        code_str = str(code_str)
        if len(code_str) >= 9:
            return code_str[:9]
        elif len(code_str) >= 5:
            return code_str[:5]
        return code_str
    
    df['소분류코드'] = df['정규카테고리코드'].apply(extract_sub_code)
    
    if CATEGORY_DF is not None:
        sub_categories = CATEGORY_DF[CATEGORY_DF['Depth'] == 3].copy()
        sub_mapping = dict(zip(sub_categories['Code'], sub_categories['Name']))
        
        def map_to_sub_name(code):
            if pd.isna(code) or code == '':
                return '기타'
            code_str = str(code)
            if code_str in sub_mapping:
                return sub_mapping[code_str]
            for cat_code, cat_name in sub_mapping.items():
                if code_str.zfill(9) == cat_code.zfill(9):
                    return cat_name
            
            middle_code = code_str[:5] if len(code_str) >= 5 else code_str
            middle_categories = CATEGORY_DF[CATEGORY_DF['Depth'] == 2]
            middle_mapping = dict(zip(middle_categories['Code'], middle_categories['Name']))
            
            if middle_code in middle_mapping:
                return f"{middle_mapping[middle_code]} (세부미분류)"
            return f'미분류_{code_str}'
        
        df['소분류명'] = df['소분류코드'].apply(map_to_sub_name)
    else:
        df['소분류명'] = '기타'
    
    return df

def category_classifier(df, category_type='중분류', filter_condition=None):
    """카테고리분류"""
    if filter_condition:
        df = apply_additional_filter(df, filter_condition)
    
    if CATEGORY_DF is None:
        load_category_data()
    
    df_with_category = df.copy()
    
    if category_type == '중분류':
        df_with_category = map_middle_category_name(df_with_category)
        return df_with_category.dropna(subset=['중분류명'])
    elif category_type == '소분류':
        df_with_category = map_sub_category_name(df_with_category)
        return df_with_category.dropna(subset=['소분류명'])
    
    return df_with_category

# ===== [0] 기본 - 집계 =====
def total_revenue(df, group_by=None, filter_condition=None):
    """총매출액"""
    filtered_df = apply_additional_filter(df, filter_condition)
    if group_by:
        return filtered_df.groupby(group_by)['상품별 총 주문금액'].sum()
    return filtered_df['상품별 총 주문금액'].sum()

def total_orders(df, group_by=None, filter_condition=None):
    """총주문건수"""
    filtered_df = apply_additional_filter(df, filter_condition)
    if group_by:
        return filtered_df.groupby(group_by).size()
    return len(filtered_df)

def successful_orders(df, group_by=None, filter_condition=None):
    """성공주문건수"""
    filtered_df = apply_additional_filter(df, filter_condition)
    success_df = filtered_df[filtered_df['주문상태'] != '결제취소']
    if group_by:
        return success_df.groupby(group_by).size()
    return len(success_df)

def cancelled_orders(df, group_by=None, filter_condition=None):
    """취소주문건수"""
    filtered_df = apply_additional_filter(df, filter_condition)
    cancel_df = filtered_df[filtered_df['주문상태'] == '결제취소']
    if group_by:
        return cancel_df.groupby(group_by).size()
    return len(cancel_df)

def return_orders(df, group_by=None, filter_condition=None):
    """반품주문건수"""
    filtered_df = apply_additional_filter(df, filter_condition)
    return_df = filtered_df[filtered_df['주문상태'] == '반품']
    if group_by:
        return return_df.groupby(group_by).size()
    return len(return_df)

def claim_orders(df, group_by=None, filter_condition=None):
    """클레임주문건수"""
    filtered_df = apply_additional_filter(df, filter_condition)
    claim_df = filtered_df[filtered_df['클레임'].notna()]
    if group_by:
        return claim_df.groupby(group_by).size()
    return len(claim_df)

def total_customers(df, group_by=None, filter_condition=None):
    """총고객수"""
    filtered_df = apply_additional_filter(df, filter_condition)
    if '고유구매자' not in filtered_df.columns:
        filtered_df = create_unique_buyer(filtered_df)
    if group_by:
        return filtered_df.groupby(group_by)['고유구매자'].nunique()
    return filtered_df['고유구매자'].nunique()

def total_products(df, group_by=None, filter_condition=None):
    """총상품수"""
    filtered_df = apply_additional_filter(df, filter_condition)
    if group_by:
        return filtered_df.groupby(group_by)['상품명'].nunique()
    return filtered_df['상품명'].nunique()

def total_profit(df, group_by=None, filter_condition=None):
    """총수익액"""
    filtered_df = apply_additional_filter(df, filter_condition)
    if group_by:
        return filtered_df.groupby(group_by)['정산금액'].sum()
    return filtered_df['정산금액'].sum()

def total_quantity(df, group_by=None, filter_condition=None):
    """총수량"""
    filtered_df = apply_additional_filter(df, filter_condition)
    if group_by:
        return filtered_df.groupby(group_by)['수량'].sum()
    return filtered_df['수량'].sum()

# ===== [0] 기본 - 평균 =====
def avg_order_value(df, group_by=None, filter_condition=None):
    """평균주문금액"""
    revenue = total_revenue(df, group_by=group_by, filter_condition=filter_condition)
    orders = total_orders(df, group_by=group_by, filter_condition=filter_condition)
    if group_by:
        return (revenue / orders.replace(0, np.nan)).fillna(0)
    return revenue / orders if orders > 0 else 0

def avg_product_price(df, group_by=None, filter_condition=None):
    """평균상품가격"""
    filtered_df = apply_additional_filter(df, filter_condition)
    if group_by:
        return filtered_df.groupby(group_by)['상품가격'].mean()
    return filtered_df['상품가격'].mean()

def avg_quantity(df, group_by=None, filter_condition=None):
    """평균수량"""
    filtered_df = apply_additional_filter(df, filter_condition)
    if group_by:
        return filtered_df.groupby(group_by)['수량'].mean()
    return filtered_df['수량'].mean()

def avg_shipping_time(df, group_by=None, filter_condition=None):
    """평균출고시간"""
    filtered_df = apply_additional_filter(df, filter_condition)
    valid_df = filtered_df[filtered_df['주문일시'].notna() & filtered_df['출고예정일'].notna()].copy()
    if len(valid_df) == 0:
        return 0 if not group_by else pd.Series(dtype=float)
    valid_df['주문일시'] = pd.to_datetime(valid_df['주문일시'])
    valid_df['출고예정일'] = pd.to_datetime(valid_df['출고예정일'])
    valid_df['출고시간'] = (valid_df['출고예정일'] - valid_df['주문일시']).dt.days
    if group_by:
        return valid_df.groupby(group_by)['출고시간'].mean()
    return valid_df['출고시간'].mean()

def unique_avg_value(df, numerator_col, denominator_col, group_by=None, filter_condition=None):
    """고유평균값"""
    filtered_df = apply_additional_filter(df, filter_condition)
    valid_df = filtered_df[filtered_df[denominator_col] != 0].copy()
    if len(valid_df) == 0:
        return 0 if not group_by else pd.Series(dtype=float)
    valid_df['비율'] = valid_df[numerator_col] / valid_df[denominator_col]
    if group_by:
        return valid_df.groupby(group_by)['비율'].mean()
    return valid_df['비율'].mean()

# ===== [0] 기본 - 비율 =====
def cancellation_rate(df, group_by=None, filter_condition=None):
    """취소율"""
    cancelled = cancelled_orders(df, group_by=group_by, filter_condition=filter_condition)
    total = total_orders(df, group_by=group_by, filter_condition=filter_condition)
    
    if group_by:
        return (cancelled / total * 100).fillna(0).round(2)
    return round(cancelled / total * 100, 2) if total > 0 else 0

def return_rate(df, group_by=None, filter_condition=None):
    """반품률"""
    returned = return_orders(df, group_by=group_by, filter_condition=filter_condition)
    total = total_orders(df, group_by=group_by, filter_condition=filter_condition)
    
    if group_by:
        return (returned / total * 100).fillna(0).round(2)
    return round(returned / total * 100, 2) if total > 0 else 0

def exchange_rate(df, group_by=None, filter_condition=None):
    """교환율"""
    filtered_df = apply_additional_filter(df, filter_condition)
    exchange_df = filtered_df[filtered_df['클레임'].str.contains('교환', na=False)]
    
    exchanged = total_orders(exchange_df, group_by=group_by)
    total = total_orders(filtered_df, group_by=group_by)
    
    if group_by:
        return (exchanged / total * 100).fillna(0).round(2)
    return round(exchanged / total * 100, 2) if total > 0 else 0

def delivery_success_rate(df, group_by=None, filter_condition=None):
    """배송성공률"""
    success = successful_orders(df, group_by=group_by, filter_condition=filter_condition)
    total = total_orders(df, group_by=group_by, filter_condition=filter_condition)
    
    if group_by:
        return (success / total * 100).fillna(0).round(2)
    return round(success / total * 100, 2) if total > 0 else 0

def claim_rate(df, group_by=None, filter_condition=None):
    """클레임률"""
    claims = claim_orders(df, group_by=group_by, filter_condition=filter_condition)
    total = total_orders(df, group_by=group_by, filter_condition=filter_condition)
    
    if group_by:
        return (claims / total * 100).fillna(0).round(2)
    return round(claims / total * 100, 2) if total > 0 else 0

def profit_rate(df, group_by=None, filter_condition=None):
    """수익률"""
    profit = total_profit(df, group_by=group_by, filter_condition=filter_condition)
    revenue = total_revenue(df, group_by=group_by, filter_condition=filter_condition)
    
    if group_by:
        return (profit / revenue * 100).fillna(0).round(2)
    return round(profit / revenue * 100, 2) if revenue > 0 else 0

def repurchase_rate(df, group_by=None, filter_condition=None):
    """재구매율"""
    filtered_df = apply_additional_filter(df, filter_condition)
    if '고유구매자' not in filtered_df.columns:
        filtered_df = create_unique_buyer(filtered_df)
    
    if group_by:
        grouped = filtered_df.groupby(group_by)
        results = {}
        for name, group in grouped:
            customer_orders = group.groupby('고유구매자').size()
            repeat_customers = len(customer_orders[customer_orders >= 2])
            total_customers = len(customer_orders)
            results[name] = round(repeat_customers / total_customers * 100, 2) if total_customers > 0 else 0
        return pd.Series(results)
    
    customer_orders = filtered_df.groupby('고유구매자').size()
    repeat_customers = len(customer_orders[customer_orders >= 2])
    total_customers = len(customer_orders)
    
    return round(repeat_customers / total_customers * 100, 2) if total_customers > 0 else 0

# ===== [0] 기본 - 고급 분석 =====
def contribution_calculator(df, metric_column, group_by=None, filter_condition=None):
    """기여도"""
    if filter_condition:
        df = apply_additional_filter(df, filter_condition)
    
    if group_by:
        group_sum = df.groupby(group_by)[metric_column].sum().reset_index()
        total_sum = df[metric_column].sum()
        group_sum['기여도(%)'] = (group_sum[metric_column] / total_sum * 100).round(2)
        group_sum['누적기여도(%)'] = group_sum['기여도(%)'].cumsum().round(2)
        return group_sum.sort_values(metric_column, ascending=False)
    else:
        return df[metric_column].sum()

def market_share_calculator(df, metric_column, group_by, filter_condition=None):
    """점유율"""
    if filter_condition:
        df = apply_additional_filter(df, filter_condition)
    
    group_sum = df.groupby(group_by)[metric_column].sum().reset_index()
    total_sum = group_sum[metric_column].sum()
    group_sum['점유율(%)'] = (group_sum[metric_column] / total_sum * 100).round(2)
    
    return group_sum.sort_values('점유율(%)', ascending=False)

def change_analyzer(current_df, previous_df, metric_column, group_by=None, filter_condition=None):
    """변동분석"""
    if filter_condition:
        current_df = apply_additional_filter(current_df, filter_condition)
        previous_df = apply_additional_filter(previous_df, filter_condition)
    
    if group_by:
        current = current_df.groupby(group_by)[metric_column].sum().reset_index()
        previous = previous_df.groupby(group_by)[metric_column].sum().reset_index()
        
        comparison = pd.merge(current, previous, on=group_by, suffixes=['_현재', '_이전'], how='outer').fillna(0)
        
        comparison['변동률(%)'] = comparison.apply(
            lambda row: 100.0 if row[f'{metric_column}_이전'] == 0 and row[f'{metric_column}_현재'] > 0
            else 0.0 if row[f'{metric_column}_이전'] == 0
            else ((row[f'{metric_column}_현재'] - row[f'{metric_column}_이전']) / row[f'{metric_column}_이전'] * 100),
            axis=1
        ).round(2)
        
        comparison['변동량'] = (comparison[f'{metric_column}_현재'] - comparison[f'{metric_column}_이전']).round(2)
        
        return comparison.sort_values('변동률(%)', ascending=False)
    else:
        current_total = current_df[metric_column].sum()
        previous_total = previous_df[metric_column].sum()
        
        if previous_total == 0:
            return 100.0 if current_total > 0 else 0.0
        
        return round((current_total - previous_total) / previous_total * 100, 2)

def ranking_calculator(df, rank_column, group_by=None, ascending=False, filter_condition=None):
    """순위"""
    if filter_condition:
        df = apply_additional_filter(df, filter_condition)
    
    if group_by:
        result = df.groupby(group_by)[rank_column].sum().reset_index()
        result = result.sort_values(rank_column, ascending=ascending)
        result['순위'] = result[rank_column].rank(ascending=ascending, method='dense').astype(int)
        return result
    else:
        df_copy = df.copy()
        df_copy['순위'] = df_copy[rank_column].rank(ascending=ascending, method='dense').astype(int)
        return df_copy.sort_values(rank_column, ascending=ascending)

def top_n_extractor(df, n=30):
    """상위N개추출"""
    return df.head(n)

# ===== 파라미터 기반 필터링 =====
def apply_params_filter(df: pd.DataFrame, params: dict) -> pd.DataFrame:
    """파라미터 기반 데이터 필터링"""
    out = df.copy()
    
    # 날짜 필터링
    if '주문일시' in out.columns:
        if params.get('date_from') is not None:
            out = out[out['주문일시'] >= params['date_from']]
        if params.get('date_to') is not None:
            out = out[out['주문일시'] <= params['date_to']]
    
    # 취소 주문 제외
    if not params.get('include_canceled', False) and '주문상태' in out.columns:
        out = out[out['주문상태'].astype(str) != '결제취소']
    
    # 채널, 업체, 카테고리 필터링
    for col, key in [('채널명','channels'),('업체명','sellers'),('카테고리','categories')]:
        vals = params.get(key, [])
        if vals and col in out.columns:
            out = out[out[col].isin(vals)]
    
    return out

def safe_group_sum(d: pd.DataFrame, by: list[str], val: str, out_col: str) -> pd.DataFrame:
    """안전한 그룹별 합계 (하위 호환성을 위해 유지)"""
    keys = [k for k in by if k in d.columns]
    if not keys:
        d = d.copy()
        d['_전체'] = '전체'
        keys = ['_전체']
    if val not in d.columns:
        d = d.copy()
        d[val] = 0
    
    g = d.groupby(keys, dropna=False)[val].sum().reset_index(name=out_col)
    return g

def safe_group_size(d: pd.DataFrame, by: list[str], out_col: str) -> pd.DataFrame:
    """안전한 그룹별 개수 (하위 호환성을 위해 유지)"""
    keys = [k for k in by if k in d.columns]
    if not keys:
        d = d.copy()
        d['_전체'] = '전체'
        keys = ['_전체']
    
    g = d.groupby(keys, dropna=False).size().reset_index(name=out_col)
    return g

def add_rank(df: pd.DataFrame, sort_col: str, asc: bool=False, rank_col: str='순위') -> pd.DataFrame:
    """순위 추가 (하위 호환성을 위해 유지)"""
    out = df.copy()
    if sort_col not in out.columns:
        out[sort_col] = 0
    out[rank_col] = out[sort_col].rank(method='dense', ascending=asc).astype(int)
    return out

def issue_rate(d: pd.DataFrame, keys: list[str], pattern: str) -> pd.DataFrame:
    """이슈율 계산 (취소/반품/클레임 등) - ops.py에서 이동"""
    total = safe_group_size(d, keys, '총건수')
    status = d.get('주문상태', pd.Series(dtype='string')).astype(str)
    claim = d.get('클레임', pd.Series(dtype='string')).astype(str)
    
    # 주문상태와 클레임 컬럼 모두에서 패턴 검색
    issue_mask = (status.str.contains(pattern, na=False) | 
                  claim.str.contains(pattern, na=False))
    issue_df = d[issue_mask].copy()
    
    issue_count = safe_group_size(issue_df, keys, '이슈건수')
    on_keys = [k for k in keys if (k in total.columns and k in issue_count.columns)]
    g = total.merge(issue_count, on=on_keys, how='left')
    g['이슈건수'] = g['이슈건수'].fillna(0)
    g['비율(%)'] = (g['이슈건수'] / g['총건수'].replace({0: pd.NA}) * 100).fillna(0).round(2)
    return g