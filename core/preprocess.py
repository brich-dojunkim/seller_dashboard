from __future__ import annotations
import re
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype as is_dt

DAYMAP = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}

def coerce_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """데이터 타입 강제 변환"""
    out = df.copy()
    
    # 날짜/시간 컬럼 처리
    datetime_cols = ['주문일시', '출고예정일', '발송처리일', '배송완료일', '구매확정일']
    for col in datetime_cols:
        if col in out.columns and not is_dt(out[col]):
            out[col] = pd.to_datetime(out[col], errors='coerce')
    
    # 숫자 컬럼 처리
    if '상품별 총 주문금액' in out.columns:
        out['상품별 총 주문금액'] = pd.to_numeric(out['상품별 총 주문금액'], errors='coerce')
    if '정산금액' in out.columns:
        out['정산금액'] = pd.to_numeric(out['정산금액'], errors='coerce')
    if '수량' in out.columns:
        out['수량'] = pd.to_numeric(out['수량'], errors='coerce')
    if '상품가격' in out.columns:
        out['상품가격'] = pd.to_numeric(out['상품가격'], errors='coerce')
    
    # 문자열 컬럼 처리
    string_cols = ['채널명','업체명','카테고리','구매자명','구매자연락처','주문상태','상품명','상품주문번호','클레임']
    for c in string_cols:
        if c in out.columns:
            out[c] = out[c].astype('string')
    
    return out

def _last4(s: str) -> str:
    """연락처 뒷자리 4자리 추출"""
    if pd.isna(s): 
        return 'XXXX'
    s = str(s)
    digits = re.sub(r'[^0-9]', '', s)
    return digits[-4:] if len(digits)>=4 else (digits if digits else 'XXXX')

def finalize(df: pd.DataFrame) -> pd.DataFrame:
    """최종 전처리 파이프라인"""
    out = df.copy()
    out = coerce_dtypes(out)

    # 고유구매자 파생 (이미 생성되었지만 재확인)
    if '고유구매자' not in out.columns:
        if '구매자명' not in out.columns: 
            out['구매자명'] = '미상'
        if '구매자연락처' not in out.columns: 
            out['구매자연락처'] = '0000'
        out['고유구매자'] = out['구매자명'].fillna('미상').astype(str) + '_' + out['구매자연락처'].apply(_last4)

    # 카테고리 파생 (중분류코드) - 이미 생성되었지만 재확인
    if '중분류코드' not in out.columns:
        def midcode(x):
            x = '' if pd.isna(x) else str(x)
            digits = re.sub(r'[^0-9]', '', x)
            return digits[:5] if digits else '00000'
        out['중분류코드'] = out.get('카테고리','').astype(str).map(midcode)

    # 시간 파생 (전역 제공) - 이미 생성되었지만 재확인
    if '주문일시' in out.columns:
        if '주문일' not in out.columns or '요일' not in out.columns or '시간대' not in out.columns:
            dt = pd.to_datetime(out['주문일시'], errors='coerce')
            out['주문일'] = dt.dt.floor('D')
            out['요일'] = dt.dt.dayofweek.map(DAYMAP)
            out['시간대'] = dt.dt.hour
    else:
        if '주문일' not in out.columns:
            out['주문일'] = pd.NaT
        if '요일' not in out.columns:
            out['요일'] = pd.NA
        if '시간대' not in out.columns:
            out['시간대'] = pd.NA
    
    return out