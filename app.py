from __future__ import annotations
import importlib
import pandas as pd
import streamlit as st
from core.io import get_orders
from core.registry import list_metrics
from core.types import MetricParams
import config

st.set_page_config(page_title="B-Flow Metrics Dashboard — Auto 82", layout="wide")

def _import_callable(fqn: str):
    """함수 동적 임포트"""
    mod_name, func_name = fqn.rsplit('.', 1)
    mod = importlib.import_module(mod_name)
    return getattr(mod, func_name)

# 데이터 로드
try:
    df = get_orders()
    data_loaded = True
    st.sidebar.success(f"✅ 데이터 로드 완료: {len(df):,}건")
except Exception as e:
    st.sidebar.error(f"❌ 데이터 로드 실패: {str(e)}")
    data_loaded = False
    df = pd.DataFrame()

# 영역 선택
areas = {1:'채널',2:'상품',3:'고객',4:'업체',5:'카테고리',6:'트렌드'}
area = st.sidebar.selectbox("영역", options=[1,2,3,4,5,6], format_func=lambda x: f"[{x}] {areas[x]}")

# 지표 검색
q = st.sidebar.text_input("지표 검색")

# 지표 목록 및 선택
specs = list_metrics(area=area, q=q)
if specs:
    labels = [f"{s['id']} · {s['name']}" for s in specs]
    idx = st.sidebar.selectbox("지표", options=list(range(len(specs))), format_func=lambda i: labels[i])
    spec = specs[idx]
else:
    st.sidebar.warning("해당 조건의 지표가 없습니다.")
    spec = None

# 필터 설정
if data_loaded and not df.empty:
    # 날짜 필터
    date_from = st.sidebar.date_input("시작일", value=df['주문일시'].min().date())
    date_to = st.sidebar.date_input("종료일", value=df['주문일시'].max().date())
    
    # 채널, 업체, 카테고리 필터
    available_channels = sorted(df['채널명'].dropna().unique().tolist())
    available_sellers = sorted(df['업체명'].dropna().unique().tolist()) 
    available_categories = sorted(df['카테고리'].dropna().unique().tolist())
    
    channels = st.sidebar.multiselect("채널", available_channels)
    sellers = st.sidebar.multiselect("업체", available_sellers)
    cats = st.sidebar.multiselect("카테고리", available_categories)
    
    # 기타 옵션
    top_n = st.sidebar.number_input("Top N", min_value=5, max_value=200, value=50, step=5)
    include_canceled = st.sidebar.checkbox("결제취소 포함", value=False)
else:
    # 기본값 설정
    date_from = date_to = None
    channels = sellers = cats = []
    top_n = 50
    include_canceled = False

# 파라미터 구성
params: MetricParams = {
    'date_from': pd.to_datetime(date_from) if date_from else None,
    'date_to': pd.to_datetime(date_to) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1) if date_to else None,
    'channels': channels,
    'sellers': sellers,
    'categories': cats,
    'top_n': int(top_n),
    'include_canceled': bool(include_canceled),
}

# 메인 컨텐츠
if not data_loaded:
    st.error("❌ 데이터를 먼저 로드해주세요.")
    st.stop()

if not spec:
    st.info("👈 사이드바에서 지표를 선택해주세요.")
    st.stop()

# 지표 실행
st.title(f"B-Flow Metrics — {spec['id']} · {spec['name']}")

# 필터 정보 표시
if any([date_from, date_to, channels, sellers, cats]):
    filter_info = []
    if date_from and date_to:
        filter_info.append(f"📅 기간: {date_from} ~ {date_to}")
    if channels:
        filter_info.append(f"📺 채널: {', '.join(channels[:3])}{'...' if len(channels) > 3 else ''}")
    if sellers:
        filter_info.append(f"🏢 업체: {', '.join(sellers[:3])}{'...' if len(sellers) > 3 else ''}")
    if cats:
        filter_info.append(f"📂 카테고리: {', '.join(cats[:3])}{'...' if len(cats) > 3 else ''}")
    
    st.info(" | ".join(filter_info))

try:
    # 함수 실행
    func = _import_callable(spec['func_fqn'])
    
    with st.spinner("📊 지표 계산 중..."):
        result = func(df, params)
    
    if isinstance(result, pd.DataFrame) and not result.empty:
        # 데이터 표시
        st.dataframe(result, use_container_width=True, height=650)
        
        # 요약 정보
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("총 행수", f"{len(result):,}")
        with col2:
            if '총매출액' in result.columns:
                total_revenue = result['총매출액'].sum()
                st.metric("총 매출액", f"{total_revenue:,.0f}원")
            elif '매출액' in result.columns:
                total_revenue = result['매출액'].sum()
                st.metric("총 매출액", f"{total_revenue:,.0f}원")
        with col3:
            if '순위' in result.columns:
                st.metric("순위 범위", f"1 ~ {result['순위'].max()}")
        
        # CSV 다운로드
        csv_data = result.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            "📥 CSV 다운로드", 
            data=csv_data.encode('utf-8-sig'),
            file_name=f"{spec['id']}_{spec['name']}.csv", 
            mime="text/csv"
        )
    
    elif isinstance(result, pd.DataFrame) and result.empty:
        st.warning("⚠️ 해당 조건으로 조회된 데이터가 없습니다.")
        st.info("필터 조건을 조정해보세요.")
    
    else:
        st.error("❌ 예상치 못한 결과 형식입니다.")
        st.write("결과:", result)

except Exception as e:
    st.error(f"❌ 지표 계산 중 오류가 발생했습니다:")
    st.code(str(e))
    
    # 디버깅 정보
    with st.expander("🔍 디버깅 정보"):
        st.write("**선택된 지표:**", spec)
        st.write("**파라미터:**", params)
        st.write("**데이터 정보:**")
        if not df.empty:
            st.write(f"- 전체 행수: {len(df):,}")
            st.write(f"- 컬럼 목록: {list(df.columns)}")
            st.write(f"- 날짜 범위: {df['주문일시'].min()} ~ {df['주문일시'].max()}")
        else:
            st.write("- 데이터가 비어있음")

# 사이드바 하단에 데이터 정보 표시
if data_loaded and not df.empty:
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📊 데이터 정보")
    st.sidebar.write(f"**총 주문건수:** {len(df):,}건")
    st.sidebar.write(f"**총 매출액:** {df['상품별 총 주문금액'].sum():,.0f}원")
    st.sidebar.write(f"**기간:** {df['주문일시'].min().date()} ~ {df['주문일시'].max().date()}")
    st.sidebar.write(f"**채널 수:** {df['채널명'].nunique()}개")
    st.sidebar.write(f"**업체 수:** {df['업체명'].nunique()}개")
    st.sidebar.write(f"**상품 수:** {df['상품명'].nunique()}개")