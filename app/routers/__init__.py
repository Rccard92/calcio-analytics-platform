from app.routers.db_status import router as db_status_router
from app.routers.health import router as health_router

__all__ = ["health_router", "db_status_router"]
