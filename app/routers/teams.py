"""
API Teams: overview squadre per stagione, dettaglio squadra, rosa giocatori,
ingestion giocatori per squadra/stagione.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException

from app.core.database import get_db
from app.schemas.teams import (
    PlayerIngestionResponse,
    PlayerSeasonRow,
    TeamDetailResponse,
    TeamSeasonOverviewResponse,
)
from app.services.player_ingestion_service import ingest_team_players
from app.services.player_service import get_team_players
from app.services.team_service import get_team_season_detail
from app.services.teams_service import get_teams_season_overview
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/teams", tags=["teams"])


@router.get("/season/{season}/overview", response_model=TeamSeasonOverviewResponse)
def season_overview(season: int, db: Session = Depends(get_db)):
    """
    Restituisce una riga per ogni squadra della stagione con statistiche aggregate.
    Solo match conclusi (FT). Dati da fixtures (risultati) aggregati per team.
    """
    teams = get_teams_season_overview(season=season, db=db)
    return TeamSeasonOverviewResponse(season=season, teams=teams)


@router.get("/{team_id}/season/{season}/detail", response_model=TeamDetailResponse)
def team_detail(team_id: int, season: int, db: Session = Depends(get_db)):
    """
    Dettaglio squadra per stagione: overview, split casa/trasferta, form ultime 5.
    Solo match conclusi (FT). 404 se squadra non trovata.
    """
    detail = get_team_season_detail(team_id=team_id, season=season, db=db)
    if detail is None:
        raise HTTPException(status_code=404, detail="Squadra non trovata o nessun match in stagione")
    return detail


@router.get("/{team_id}/season/{season}/players", response_model=list[PlayerSeasonRow])
def team_players(team_id: int, season: int, db: Session = Depends(get_db)):
    """
    Rosa giocatori con statistiche stagionali per squadra e stagione.
    Query unica join players + player_season_stats.
    Se non ci sono dati restituisce array vuoto (mai errore).
    """
    return get_team_players(team_id=team_id, season=season, db=db)


@router.post("/{team_id}/season/{season}/ingest-players", response_model=PlayerIngestionResponse)
async def ingest_players(team_id: int, season: int, db: Session = Depends(get_db)):
    """
    Ingestion rosa giocatori da API-Sports per squadra e stagione.
    Upsert in players + player_season_stats. Idempotente: se rilanciato non crea duplicati.
    """
    try:
        count = await ingest_team_players(team_id=team_id, season=season, db=db)
    except Exception as e:
        logger.exception("Errore ingestion giocatori team_id=%s season=%s: %s", team_id, season, e)
        raise HTTPException(status_code=502, detail=f"Errore durante ingestion giocatori: {e}")

    return PlayerIngestionResponse(team_id=team_id, season=season, players_ingested=count)
