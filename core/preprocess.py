# core/preprocess.py
from __future__ import annotations
import re
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype as is_dt

DAYMAP = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}

def coerce_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if '주문일시' in out.columns and not is_dt(out['주문일시']):
        out['주문일시'] = pd.to_datetime(out['주문일시'], errors='coerce')
    if '상품별 총 주문금액' in out.columns:
        out['상품별 총 주문금액'] = pd.to_numeric(out['상품별 총 주문금액'], errors='coerce')
    for c in ['채널명','업체명','카테고리','구매자명','구매자연락처','주문상태','상품명','상품주문번호']:
        if c in out.columns:
            out[c] = out[c].astype('string')
    return out

def _last4(s: str) -> str:
    if pd.isna(s): return 'XXXX'
    s = str(s)
    digits = re.sub(r'\\D','', s)
    return digits[-4:] if len(digits)>=4 else (digits if digits else 'XXXX')

def finalize(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = coerce_dtypes(out)

    # 구매자 파생
    if '구매자명' not in out.columns: out['구매자명'] = '미상'
    if '구매자연락처' not in out.columns: out['구매자연락처'] = '0000'
    out['고유구매자'] = out['구매자명'].fillna('미상').astype(str) + '_' + out['구매자연락처'].apply(_last4)

    # 카테고리 파생 (중분류코드)
    def midcode(x):
        x = '' if pd.isna(x) else str(x)
        digits = re.sub(r'\\D','', x)
        return digits[:5] if digits else '00000'
    out['중분류코드'] = out.get('카테고리','').astype(str).map(midcode)

    # 시간 파생 (전역 제공)
    if '주문일시' in out.columns:
        dt = pd.to_datetime(out['주문일시'], errors='coerce')
        out['주문일'] = dt.dt.floor('D')
        out['요일'] = dt.dt.dayofweek.map(DAYMAP)   # ← 로케일 의존 없음
        out['시간대'] = dt.dt.hour
    else:
        out['주문일'] = pd.NaT
        out['요일'] = pd.NA
        out['시간대'] = pd.NA
    return out
