from fastapi import APIRouter, HTTPException

from .schemas import AnalyzeRequest, AnalyzeResponse, HealthResponse
from .service import HerdBehaviorService


router = APIRouter()
service = HerdBehaviorService()


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok")


@router.post("/analyze", response_model=AnalyzeResponse)
def analyze(req: AnalyzeRequest) -> AnalyzeResponse:
    try:
        result = service.analyze_series(
            timestamps=req.timestamps,
            prices=req.prices,
            volumes=req.volumes,
            entities=req.entities,
        )
        return AnalyzeResponse(**result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/alerts/sample", response_model=AnalyzeResponse)
def sample_alert() -> AnalyzeResponse:
    example = service.example_signal()
    return AnalyzeResponse(**example)


