from typing import List, Optional

from pydantic import BaseModel, Field, conlist


class HealthResponse(BaseModel):
    status: str = Field(..., examples=["ok"]) 


class AnalyzeRequest(BaseModel):
    timestamps: conlist(int, min_length=5) = Field(
        ..., description="Epoch seconds aligned with prices and volumes"
    )
    prices: conlist(float, min_length=5)
    volumes: Optional[conlist(float, min_length=5)] = None
    entities: Optional[List[str]] = Field(
        default=None,
        description="Optional list of entity identifiers participating in series",
    )


class AlertDetail(BaseModel):
    name: str
    value: float
    threshold: float
    severity: str


class AnalyzeResponse(BaseModel):
    is_alert: bool
    score: float = Field(ge=0.0, le=1.0)
    reason: str
    details: List[AlertDetail] = []


