"""Calcio Analytics Platform — pre-match football analytics API."""

import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from app.core.database import init_db
from app.routers import api_test_router, db_status_router, debug_router, health_router, ingestion_router, leagues_router

app = FastAPI(
    title="Calcio Analytics Platform",
    description="Production football analytics API. Serie A data (fixtures, team match stats).",
    version="0.1.0",
)

app.include_router(health_router)
app.include_router(db_status_router)
app.include_router(ingestion_router)
app.include_router(leagues_router)
app.include_router(api_test_router)
app.include_router(debug_router)

static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir), html=True), name="static")

    @app.get("/", include_in_schema=False)
    def _redirect_dashboard():
        return RedirectResponse(url="/static/")


@app.on_event("startup")
def on_startup():
    """Inizializza le tabelle al'avvio. Temporaneo per sviluppo."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s [%(name)s] %(message)s")
    init_db()
