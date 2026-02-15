from app.routers.api_test import router as api_test_router
from app.routers.db_status import router as db_status_router
from app.routers.debug import router as debug_router
from app.routers.dashboard import router as dashboard_router
from app.routers.health import router as health_router
from app.routers.ingestion import router as ingestion_router
from app.routers.leagues import router as leagues_router

__all__ = ["health_router", "db_status_router", "ingestion_router", "api_test_router", "leagues_router", "debug_router", "dashboard_router"]
