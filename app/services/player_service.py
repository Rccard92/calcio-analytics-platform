"""
Servizio rosa giocatori per squadra e stagione.
Query join players + player_season_stats, arricchita con metriche derivate e scoring.
Compatibile con schema vecchio (shots) e nuovo (shots_total, shots_on, tackles, ecc.).
"""

import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.schemas.teams import PlayerSeasonRow
from app.services.player_metrics import calculate_derived_metrics, calculate_player_score

logger = logging.getLogger(__name__)

# Query completa: estrae TUTTI i campi necessari per metriche e scoring
TEAM_PLAYERS_SQL = text("""
SELECT
  p.id            AS player_id,
  COALESCE(p.api_player_id, 0) AS api_player_id,
  p.name,
  COALESCE(p.position, '') AS position,
  s.appearances,
  s.minutes,
  s.goals,
  s.assists,
  s.shots_total,
  s.shots_on,
  s.passes_accuracy,
  s.rating,
  s.yellow_cards,
  s.red_cards,
  s.tackles_total,
  s.interceptions,
  s.duels_total,
  s.duels_won,
  s.dribbles_attempts,
  s.dribbles_success,
  s.key_passes,
  s.fouls_committed
FROM players p
INNER JOIN player_season_stats s ON s.player_id = p.id
WHERE s.team_id = :team_id AND s.season = :season
""")

# Fallback per DB con schema pre-migrazione
TEAM_PLAYERS_SQL_LEGACY = text("""
SELECT
  p.id            AS player_id,
  COALESCE(p.api_player_id, 0) AS api_player_id,
  p.name,
  COALESCE(p.position, '') AS position,
  s.appearances,
  s.minutes,
  s.goals,
  s.assists,
  COALESCE(s.shots, 0)  AS shots_total,
  0                      AS shots_on,
  s.passes_accuracy,
  s.rating,
  0 AS yellow_cards,
  0 AS red_cards,
  0 AS tackles_total,
  0 AS interceptions,
  0 AS duels_total,
  0 AS duels_won,
  0 AS dribbles_attempts,
  0 AS dribbles_success,
  0 AS key_passes,
  0 AS fouls_committed
FROM players p
INNER JOIN player_season_stats s ON s.player_id = p.id
WHERE s.team_id = :team_id AND s.season = :season
""")


def _safe_int(val: Any) -> int:
    """Converte in int; 0 se None o non convertibile."""
    if val is None:
        return 0
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


def _nullable_float(val: Any) -> float | None:
    """Converte in float mantenendo None (non converte None → 0.0)."""
    if val is None:
        return None
    try:
        return round(float(val), 2)
    except (ValueError, TypeError):
        return None


def _enrich_row(row: dict[str, Any]) -> PlayerSeasonRow:
    """
    Arricchisce una riga DB con metriche derivate (per-90, percentuali)
    e punteggi compositi calcolati nel service layer.
    """
    # Valori grezzi per il motore di scoring (preserva None per correttezza)
    raw_for_metrics: dict[str, Any] = {
        "minutes": row.get("minutes"),
        "goals": row.get("goals"),
        "assists": row.get("assists"),
        "shots_total": row.get("shots_total"),
        "shots_on": row.get("shots_on"),
        "key_passes": row.get("key_passes"),
        "tackles_total": row.get("tackles_total"),
        "interceptions": row.get("interceptions"),
        "duels_total": row.get("duels_total"),
        "duels_won": row.get("duels_won"),
        "dribbles_attempts": row.get("dribbles_attempts"),
        "dribbles_success": row.get("dribbles_success"),
        "yellow_cards": row.get("yellow_cards"),
        "red_cards": row.get("red_cards"),
        "fouls_committed": row.get("fouls_committed"),
        "rating": _nullable_float(row.get("rating")),
        "position": row.get("position") or "",
    }

    derived = calculate_derived_metrics(raw_for_metrics)
    scores = calculate_player_score(raw_for_metrics)

    return PlayerSeasonRow(
        player_id=row["player_id"],
        api_player_id=row.get("api_player_id") or 0,
        name=row.get("name") or "",
        position=raw_for_metrics["position"],
        appearances=_safe_int(row.get("appearances")),
        minutes=_safe_int(row.get("minutes")),
        goals=_safe_int(row.get("goals")),
        assists=_safe_int(row.get("assists")),
        shots_total=_safe_int(row.get("shots_total")),
        shots_on=_safe_int(row.get("shots_on")),
        pass_accuracy=_nullable_float(row.get("passes_accuracy")),
        rating=_nullable_float(row.get("rating")),
        yellow_cards=_safe_int(row.get("yellow_cards")),
        red_cards=_safe_int(row.get("red_cards")),
        tackles_total=_safe_int(row.get("tackles_total")),
        interceptions=_safe_int(row.get("interceptions")),
        key_passes=_safe_int(row.get("key_passes")),
        goals_per_90=derived.get("goals_per_90"),
        assists_per_90=derived.get("assists_per_90"),
        shots_per_90=derived.get("shots_per_90"),
        shots_on_per_90=derived.get("shots_on_per_90"),
        shot_accuracy_pct=derived.get("shot_accuracy_pct"),
        duels_won_pct=derived.get("duels_won_pct"),
        dribbles_success_pct=derived.get("dribbles_success_pct"),
        overall_score=scores.get("overall_score"),
        offensive_score=scores.get("offensive_score"),
        defensive_score=scores.get("defensive_score"),
        discipline_score=scores.get("discipline_score"),
    )


def _rows_to_list(rows: list) -> list[PlayerSeasonRow]:
    """Converte righe SQL in lista arricchita, ordinata per overall_score DESC."""
    result = [_enrich_row(dict(r)) for r in rows]
    result.sort(
        key=lambda p: (p.overall_score is not None, p.overall_score or 0),
        reverse=True,
    )
    return result


def get_team_players(
    team_id: int,
    season: int,
    db: Session,
) -> list[PlayerSeasonRow]:
    """
    Rosa giocatori con statistiche stagionali, metriche derivate e scoring.
    Tenta query con schema nuovo; se fallisce usa la legacy.
    Ordinata per overall_score DESC (giocatori senza score alla fine).
    """
    params = {"team_id": team_id, "season": season}

    try:
        rows = db.execute(TEAM_PLAYERS_SQL, params).mappings().all()
        return _rows_to_list(rows)
    except Exception as e:
        logger.warning(
            "Query players (schema nuovo) fallita per team_id=%s season=%s: %s. Provo legacy.",
            team_id, season, e,
        )
        db.rollback()

    try:
        rows = db.execute(TEAM_PLAYERS_SQL_LEGACY, params).mappings().all()
        return _rows_to_list(rows)
    except Exception as e:
        logger.exception(
            "Anche query legacy fallita per team_id=%s season=%s: %s",
            team_id, season, e,
        )
        db.rollback()
        return []
