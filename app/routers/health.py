"""Health check router."""

from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health")
def health():
    """Health check for load balancers and monitoring."""
    return {"status": "healthy"}
