
from __future__ import annotations
DATA_XLSX_PATH = "/Users/brich/Downloads/bflow_metrics_dashboard_impl/order_list_20250818120157_497.xlsx"
SHEET_NAME = "b-flow 주문 내역"
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
}
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
CACHE_TTL_SEC = 600
