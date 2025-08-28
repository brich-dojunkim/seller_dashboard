from __future__ import annotations

# 실제 Excel 파일 경로 및 설정
DATA_XLSX_PATH = "/Users/brich/Downloads/bflow_metrics_dashboard_impl/order_list_20250818120157_497.xlsx"
SHEET_NAME = "b-flow 주문 내역"

# 실제 Excel 컬럼명 → 표준화된 컬럼명 매핑
COLMAP = {
    "주문일시": "결제일",
    "채널명": "판매채널", 
    "업체명": "입점사명",
    "카테고리": "상품 카테고리",
    "구매자명": "구매자명",
    "구매자연락처": "구매자연락처", 
    "주문상태": "주문상태",
    "상품명": "상품명",
    "상품주문번호": "상품주문번호",
    "상품별 총 주문금액": "상품별 총 주문금액",
    "정산금액": "정산금액",
    "수량": "수량",
    "상품가격": "상품가격",
    "클레임": "클레임",
    "출고예정일": "출고예정일"
}

# 기본 파라미터 설정
DEFAULT_PARAMS = {
    "date_from": None,
    "date_to": None,
    "channels": [],
    "sellers": [],
    "categories": [],
    "top_n": 50,
    "min_orders": 0,
    "include_canceled": False,
    "extra_filter_expr": "",
}

# 캐시 설정
CACHE_TTL_SEC = 600

# 카테고리 파일 경로 (필요시)
CATEGORY_FILE_PATH = 'brich_category_2504071.csv'