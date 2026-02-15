"""
Endpoint di aggregazione per la dashboard di controllo dati per stagione.
Solo lettura e aggregazione; nessuna modifica a modelli o ingestion.
"""

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.models import Fixture, TeamMatchStats

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/season-overview")
def season_overview(season: int, db: Session = Depends(get_db)):
    """
    Restituisce panoramica dati per stagione: fixture, squadre, stats attese/effettive,
    copertura percentuale e fixture con statistiche incomplete.
    Query efficienti con filtri su season.
    """
    fixtures_count = db.query(Fixture).filter(Fixture.season == season).count()
    if fixtures_count == 0:
        return {
            "season": season,
            "teams_count": 0,
            "fixtures_count": 0,
            "expected_stats": 0,
            "actual_stats": 0,
            "coverage_percentage": 0.0,
            "fixtures_with_missing_stats": 0,
        }

    # Squadre distinte che hanno giocato almeno un match (casa o trasferta) in stagione
    home = select(Fixture.home_team_id.label("team_id")).where(Fixture.season == season)
    away = select(Fixture.away_team_id.label("team_id")).where(Fixture.season == season)
    teams_subq = home.union_all(away).subquery()
    teams_count = db.execute(select(func.count(func.distinct(teams_subq.c.team_id)))).scalar() or 0

    # Statistiche effettive (team_match_stats collegate a fixture della stagione)
    actual_stats = (
        db.query(TeamMatchStats)
        .join(Fixture, TeamMatchStats.fixture_id == Fixture.id)
        .filter(Fixture.season == season)
        .count()
    )

    # Fixture con meno di 2 stats (incomplete)
    incomplete = (
        db.query(Fixture.id)
        .outerjoin(TeamMatchStats, Fixture.id == TeamMatchStats.fixture_id)
        .filter(Fixture.season == season)
        .group_by(Fixture.id)
        .having(func.count(TeamMatchStats.id) < 2)
        .count()
    )

    expected_stats = fixtures_count * 2
    coverage_percentage = round((actual_stats / expected_stats * 100), 2) if expected_stats else 0.0

    return {
        "season": season,
        "teams_count": teams_count,
        "fixtures_count": fixtures_count,
        "expected_stats": expected_stats,
        "actual_stats": actual_stats,
        "coverage_percentage": coverage_percentage,
        "fixtures_with_missing_stats": incomplete,
    }
