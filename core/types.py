from __future__ import annotations
from typing import TypedDict, NotRequired, List, Dict, Any

# 대시보드 전역 필터/옵션 파라미터
class MetricParams(TypedDict, total=False):
    date_from: object          # pandas.Timestamp 등
    date_to: object
    channels: List[str]
    sellers: List[str]
    categories: List[str]
    top_n: int
    include_canceled: bool

# 레지스트리에서 사용하는 메트릭 메타 스키마
class MetricSpec(TypedDict, total=False):
    # 필수
    id: str                    # 예: "A2_014"
    name: str                  # 예: "상품별취소율"
    area: int                  # 1~6
    func_fqn: str              # fully qualified name, 예: "metrics.area2_product.mA2_014"

    # 선택(있으면 사용; 없어도 동작)
    description: NotRequired[str]
    tags: NotRequired[List[str]]
    params_schema: NotRequired[Dict[str, Any]]
    owner: NotRequired[str]
    version: NotRequired[str]
