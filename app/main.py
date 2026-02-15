"""Calcio Analytics Platform — pre-match football analytics API."""

import logging
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.core.database import init_db
from app.routers import api_test_router, db_status_router, debug_router, dashboard_router, health_router, ingestion_router, leagues_router, teams_router

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
app.include_router(dashboard_router)
app.include_router(teams_router)

templates_dir = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(templates_dir))


@app.get("/", include_in_schema=False)
def index(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/ingestion", include_in_schema=False)
def page_ingestion(request: Request):
    return templates.TemplateResponse("ingestion.html", {"request": request})


@app.get("/overview", include_in_schema=False)
def page_overview(request: Request):
    return templates.TemplateResponse("overview.html", {"request": request})


@app.get("/api-status", include_in_schema=False)
def page_api_status(request: Request):
    return templates.TemplateResponse("api_status.html", {"request": request})


@app.get("/debug", include_in_schema=False)
def page_debug(request: Request):
    return templates.TemplateResponse("debug.html", {"request": request})


@app.get("/teams", include_in_schema=False)
def page_teams(request: Request):
    return templates.TemplateResponse("teams.html", {"request": request})


@app.get("/teams/{team_id}", include_in_schema=False)
def page_team_detail(request: Request, team_id: int, season: int = 2024):
    """Dettaglio squadra: overview, casa/trasferta, form ultime 5, rosa."""
    return templates.TemplateResponse(
        "team_detail.html",
        {"request": request, "team_id": team_id, "season": season},
    )


static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir), html=True), name="static")


@app.on_event("startup")
def on_startup():
    """Inizializza le tabelle al'avvio. Temporaneo per sviluppo."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s [%(name)s] %(message)s")
    init_db()
