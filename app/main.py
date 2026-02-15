"""Calcio Analytics Platform — pre-match football analytics API."""

from fastapi import FastAPI

from app.core.database import init_db
from app.routers import db_status_router, health_router

app = FastAPI(
    title="Calcio Analytics Platform",
    description="Production football analytics API. Serie A data (fixtures, team match stats).",
    version="0.1.0",
)

app.include_router(health_router)
app.include_router(db_status_router)


@app.on_event("startup")
def on_startup():
    """Inizializza le tabelle al'avvio. Temporaneo per sviluppo."""
    init_db()
