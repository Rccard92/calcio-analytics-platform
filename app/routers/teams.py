"""
API Teams: overview squadre per stagione, dettaglio squadra, rosa giocatori.
Solo lettura; nessuna modifica a ingestion o DB.
"""

from fastapi import APIRouter, Depends, HTTPException

from app.core.database import get_db
from app.schemas.teams import (
    PlayerSeasonRow,
    TeamDetailResponse,
    TeamSeasonOverviewResponse,
)
from app.services.player_service import get_team_players
from app.services.team_service import get_team_season_detail
from app.services.teams_service import get_teams_season_overview
from sqlalchemy.orm import Session

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
    """
    return get_team_players(team_id=team_id, season=season, db=db)
