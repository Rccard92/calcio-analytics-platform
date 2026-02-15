"""
Endpoint di debug per verificare i dati salvati in DB dopo l'ingestion.
Solo lettura; nessuna modifica strutturale al database.
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from app.core.database import get_db
from app.models import Fixture, Team, TeamMatchStats

router = APIRouter(prefix="/debug", tags=["Debug"])


@router.get("/raw-stats/{fixture_id}")
def raw_stats(fixture_id: int, db: Session = Depends(get_db)):
    """
    Restituisce fixture e team_match_stats (raw stats) per il fixture_id dato.
    Utile per debug dopo ingestion/repair.
    """
    fixture = (
        db.query(Fixture)
        .options(
            joinedload(Fixture.home_team),
            joinedload(Fixture.away_team),
            joinedload(Fixture.team_match_stats).joinedload(TeamMatchStats.team),
        )
        .filter(Fixture.id == fixture_id)
        .first()
    )
    if not fixture:
        raise HTTPException(status_code=404, detail=f"Fixture {fixture_id} non trovata")

    def stat_to_dict(s):
        return {
            "team_id": s.team_id,
            "team_name": s.team.name if s.team else None,
            "shots_total": s.shots_total,
            "shots_on_target": s.shots_on_target,
            "possession": s.possession,
            "fouls": s.fouls,
            "corners": s.corners,
            "yellow_cards": s.yellow_cards,
            "red_cards": s.red_cards,
        }

    date_out = fixture.date.isoformat() if fixture.date else None
    home = fixture.home_team
    away = fixture.away_team
    return {
        "id": fixture.id,
        "season": fixture.season,
        "date": date_out,
        "round": fixture.round,
        "status": fixture.status,
        "home_team": {"id": home.id, "name": home.name} if home else None,
        "away_team": {"id": away.id, "name": away.name} if away else None,
        "goals": {"home": fixture.home_goals, "away": fixture.away_goals},
        "team_match_stats": [stat_to_dict(s) for s in fixture.team_match_stats],
    }


@router.get("/db-overview")
def db_overview(db: Session = Depends(get_db)):
    """
    Restituisce i conteggi di teams, fixtures e team_match_stats.
    Utile per verificare cosa è stato salvato dopo un'ingestion.
    """
    teams_count = db.query(Team).count()
    fixtures_count = db.query(Fixture).count()
    team_match_stats_count = db.query(TeamMatchStats).count()
    return {
        "teams_count": teams_count,
        "fixtures_count": fixtures_count,
        "team_match_stats_count": team_match_stats_count,
    }


@router.get("/sample-fixture")
def sample_fixture(db: Session = Depends(get_db)):
    """
    Restituisce una fixture casuale con home_team, away_team, gol, data
    e fino a 2 statistiche squadra collegate (team_match_stats).
    """
    fixture = (
        db.query(Fixture)
        .options(
            joinedload(Fixture.home_team),
            joinedload(Fixture.away_team),
            joinedload(Fixture.team_match_stats).joinedload(TeamMatchStats.team),
        )
        .order_by(func.random())
        .limit(1)
        .first()
    )
    if not fixture:
        raise HTTPException(status_code=404, detail="Nessuna fixture nel database")

    date_out = fixture.date.isoformat() if fixture.date else None
    home = fixture.home_team
    away = fixture.away_team
    stats = fixture.team_match_stats[:2]

    def stat_to_dict(s):
        return {
            "team_id": s.team_id,
            "team_name": s.team.name if s.team else None,
            "shots_total": s.shots_total,
            "shots_on_target": s.shots_on_target,
            "possession": s.possession,
            "fouls": s.fouls,
            "corners": s.corners,
            "yellow_cards": s.yellow_cards,
            "red_cards": s.red_cards,
        }

    return {
        "id": fixture.id,
        "season": fixture.season,
        "date": date_out,
        "round": fixture.round,
        "status": fixture.status,
        "home_team": {"id": home.id, "name": home.name} if home else None,
        "away_team": {"id": away.id, "name": away.name} if away else None,
        "goals": {"home": fixture.home_goals, "away": fixture.away_goals},
        "team_match_stats": [stat_to_dict(s) for s in stats],
    }
