"""Calcio Analytics Platform — pre-match football analytics API."""

from fastapi import FastAPI

from app.routers import health_router

app = FastAPI(
    title="Calcio Analytics Platform",
    description="Production football analytics API. Serie A data (fixtures, team match stats).",
    version="0.1.0",
)

app.include_router(health_router)
