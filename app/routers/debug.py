"""
Endpoint di debug per verificare i dati salvati in DB dopo l'ingestion.
Solo lettura; nessuna modifica strutturale al database.
Include endpoint per ispezionare la response raw di API-Sports.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, text
from sqlalchemy.orm import Session, joinedload

from app.core.database import get_db
from app.models import Fixture, Team, TeamMatchStats
from app.services.api_sports_client import ApiSportsClient

logger = logging.getLogger(__name__)

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


@router.get("/player/{api_player_id}/season/{season}")
async def player_debug(api_player_id: int, season: int):
    """
    Ritorna la response completa di API-Football per un singolo giocatore e stagione.
    Chiama /players?id={api_player_id}&season={season}.
    Non salva nulla nel DB — solo lettura da API esterna.
    """
    client = ApiSportsClient()
    try:
        result = await client.get_player_by_id(player_id=api_player_id, season=season)
    except Exception as e:
        logger.exception(
            "Errore API-Sports debug player api_player_id=%s season=%s: %s",
            api_player_id, season, e,
        )
        return {
            "error": f"Errore chiamata API-Sports: {type(e).__name__}: {e}",
            "player_id": api_player_id,
            "season": season,
        }

    if not result:
        return {
            "error": "No data found",
            "player_id": api_player_id,
            "season": season,
        }

    player_info = result.get("player", {})
    statistics = result.get("statistics", [])

    return {
        "player_id": api_player_id,
        "season": season,
        "player": player_info,
        "statistics": statistics,
        "statistics_count": len(statistics),
    }


@router.get("/team/{team_id}/season/{season}/api-raw")
async def api_raw_players(team_id: int, season: int):
    """
    Ritorna la response RAW di API-Sports per /players?team={team_id}&season={season}.
    Solo il primo giocatore (per non esporre troppi dati) + metadati.
    Utile per verificare la struttura reale dei dati prima dell'ingestion.
    """
    client = ApiSportsClient()
    try:
        players = await client.get_team_players(team_id=team_id, season=season)
    except Exception as e:
        logger.exception("Errore API-Sports debug raw team_id=%s season=%s: %s", team_id, season, e)
        return {"error": str(e), "team_id": team_id, "season": season}

    first_player = players[0] if players else None
    first_stat_keys = []
    if first_player:
        stats_list = first_player.get("statistics") or []
        if stats_list and isinstance(stats_list[0], dict):
            first_stat_keys = list(stats_list[0].keys())

    return {
        "team_id": team_id,
        "season": season,
        "total_players": len(players),
        "first_player_raw": first_player,
        "first_stat_sections": first_stat_keys,
    }


@router.get("/team/{team_id}/season/{season}/db-columns")
def db_columns_check(team_id: int, season: int, db: Session = Depends(get_db)):
    """
    Verifica lo stato reale delle colonne della tabella player_season_stats nel DB.
    Utile per diagnosticare disallineamenti schema ORM ↔ DB.
    """
    try:
        cols = db.execute(text(
            "SELECT column_name, data_type, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_name = 'player_season_stats' "
            "ORDER BY ordinal_position"
        )).fetchall()

        player_count = db.execute(text(
            "SELECT COUNT(*) FROM player_season_stats "
            "WHERE team_id = :tid AND season = :s"
        ), {"tid": team_id, "s": season}).scalar()

        return {
            "team_id": team_id,
            "season": season,
            "rows_in_db": player_count,
            "columns": [
                {"name": c[0], "type": c[1], "nullable": c[2]}
                for c in cols
            ],
            "column_count": len(cols),
        }
    except Exception as e:
        logger.exception("Errore db-columns-check: %s", e)
        return {"error": str(e)}
