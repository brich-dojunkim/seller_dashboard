from __future__ import annotations
import pandas as pd
import streamlit as st
import re
from . import preprocess
import config

def load_orders_raw() -> pd.DataFrame:
    """원본 Excel 파일 로드"""
    return pd.read_excel(config.DATA_XLSX_PATH, sheet_name=config.SHEET_NAME, engine="openpyxl")

def apply_column_mapping(df: pd.DataFrame) -> pd.DataFrame:
    """컬럼명 표준화 매핑"""
    std = df.copy()
    for stdcol, rawcol in config.COLMAP.items():
        if rawcol in std.columns and stdcol != rawcol:
            std.rename(columns={rawcol: stdcol}, inplace=True)
    return std

def create_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """파생 컬럼 생성"""
    df = df.copy()
    
    # 1. 고유구매자 생성
    def extract_phone_last4(phone_str):
        if pd.isna(phone_str):
            return 'XXXX'
        phone_digits = re.sub(r'[^0-9]', '', str(phone_str))
        return phone_digits[-4:] if len(phone_digits) >= 4 else (phone_digits if phone_digits else 'XXXX')
    
    if '구매자명' not in df.columns: 
        df['구매자명'] = '미상'
    if '구매자연락처' not in df.columns: 
        df['구매자연락처'] = '0000'
    
    df['고유구매자'] = df['구매자명'].fillna('미상').astype(str) + '_' + df['구매자연락처'].apply(extract_phone_last4)
    
    # 2. 카테고리 파생 컬럼
    def midcode(x):
        x = '' if pd.isna(x) else str(x)
        digits = re.sub(r'[^0-9]', '', x)
        return digits[:5] if digits else '00000'
    
    df['중분류코드'] = df.get('카테고리', '').astype(str).map(midcode)
    
    # 3. 시간 파생 컬럼 (전역 제공)
    if '주문일시' in df.columns:
        dt = pd.to_datetime(df['주문일시'], errors='coerce')
        df['주문일'] = dt.dt.floor('D')
        df['요일'] = dt.dt.dayofweek.map({0:'월',1:'화',2:'수',3:'목',4:'금',5:'토',6:'일'})
        df['시간대'] = dt.dt.hour
    else:
        df['주문일'] = pd.NaT
        df['요일'] = pd.NA
        df['시간대'] = pd.NA
    
    return df

def apply_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """데이터 타입 정리"""
    df = df.copy()
    
    # 날짜/시간 컬럼
    datetime_cols = ['주문일시', '출고예정일', '발송처리일', '배송완료일', '구매확정일']
    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # 숫자 컬럼
    numeric_cols = ['상품별 총 주문금액', '정산금액', '수량', '상품가격', '옵션가격', '상품주문번호']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 문자열 컬럼
    string_cols = ['채널명', '업체명', '카테고리', '구매자명', '구매자연락처', '주문상태', '상품명', '클레임']
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype('string')
    
    return df

@st.cache_data(ttl=config.CACHE_TTL_SEC, show_spinner=False)
def get_orders() -> pd.DataFrame:
    """전체 데이터 로딩 및 전처리 파이프라인"""
    # 1. 원본 데이터 로드
    raw = load_orders_raw()
    
    # 2. 컬럼명 표준화
    std = apply_column_mapping(raw)
    
    # 3. 데이터 타입 정리
    typed = apply_data_types(std)
    
    # 4. 파생 컬럼 생성
    derived = create_derived_columns(typed)
    
    # 5. 최종 전처리
    final = preprocess.finalize(derived)
    
    return final