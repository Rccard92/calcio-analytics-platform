"""
API Teams: overview squadre per stagione (statistiche aggregate).
Solo lettura; nessuna modifica a ingestion o DB.
"""

from fastapi import APIRouter, Depends

from app.core.database import get_db
from app.schemas.teams import TeamSeasonOverviewResponse
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
