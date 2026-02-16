"""
Servizio rosa giocatori per squadra e stagione.
Query join players + player_season_stats, arricchita con metriche derivate
e scoring FIFA-style normalizzato per ruolo (percentile empirico + Tier A/B/C).
"""

import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.schemas.teams import PlayerSeasonRow
from app.services.player_metrics import (
    RoleDistributions,
    build_role_distributions,
    calculate_all_metrics,
    calculate_player_score,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Query SQL
# ---------------------------------------------------------------------------

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
  s.fouls_committed,
  s.captain,
  s.blocks,
  s.saves,
  s.goals_conceded
FROM players p
INNER JOIN player_season_stats s ON s.player_id = p.id
WHERE s.team_id = :team_id AND s.season = :season
""")

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
  0 AS fouls_committed,
  false AS captain,
  0 AS blocks,
  0 AS saves,
  0 AS goals_conceded
FROM players p
INNER JOIN player_season_stats s ON s.player_id = p.id
WHERE s.team_id = :team_id AND s.season = :season
""")

ROLE_DISTRIBUTION_SQL = text("""
SELECT
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
  s.fouls_committed,
  s.captain,
  s.blocks,
  s.saves,
  s.goals_conceded
FROM player_season_stats s
INNER JOIN players p ON p.id = s.player_id
WHERE s.season = :season
  AND s.minutes IS NOT NULL
  AND s.minutes >= 300
""")


# ---------------------------------------------------------------------------
# Helper di conversione
# ---------------------------------------------------------------------------

def _safe_int(val: Any) -> int:
    if val is None:
        return 0
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


def _nullable_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return round(float(val), 2)
    except (ValueError, TypeError):
        return None


def _row_to_stats_dict(row: dict[str, Any]) -> dict[str, Any]:
    """Converte una riga DB in dict di stats per il motore di scoring."""
    return {
        "position": row.get("position") or "",
        "minutes": row.get("minutes"),
        "appearances": row.get("appearances"),
        "goals": row.get("goals"),
        "assists": row.get("assists"),
        "shots_total": row.get("shots_total"),
        "shots_on": row.get("shots_on"),
        "key_passes": row.get("key_passes"),
        "tackles_total": row.get("tackles_total"),
        "interceptions": row.get("interceptions"),
        "blocks": row.get("blocks"),
        "duels_total": row.get("duels_total"),
        "duels_won": row.get("duels_won"),
        "dribbles_attempts": row.get("dribbles_attempts"),
        "dribbles_success": row.get("dribbles_success"),
        "yellow_cards": row.get("yellow_cards"),
        "red_cards": row.get("red_cards"),
        "fouls_committed": row.get("fouls_committed"),
        "saves": row.get("saves"),
        "goals_conceded": row.get("goals_conceded"),
        "captain": bool(row.get("captain")),
        "pass_accuracy": _nullable_float(row.get("passes_accuracy")),
        "rating": _nullable_float(row.get("rating")),
    }


# ---------------------------------------------------------------------------
# Role distributions
# ---------------------------------------------------------------------------

def _compute_role_distributions(season: int, db: Session) -> RoleDistributions:
    """
    Calcola distribuzioni empiriche per ruolo su TUTTI i giocatori della
    stagione con >= 300 minuti. Usata come baseline per i percentili.
    """
    try:
        rows = db.execute(ROLE_DISTRIBUTION_SQL, {"season": season}).mappings().all()
    except Exception as e:
        logger.warning(
            "Query distribuzione ruoli fallita per season=%s: %s. Score saranno None.",
            season, e,
        )
        try:
            db.rollback()
        except Exception:
            pass
        return {}

    all_stats = [_row_to_stats_dict(dict(r)) for r in rows]
    dists = build_role_distributions(all_stats)

    logger.info(
        "Distribuzione per ruolo season=%s: %d giocatori qualificati totali, %d ruoli",
        season, len(all_stats), len(dists),
    )

    return dists


# ---------------------------------------------------------------------------
# Arricchimento riga
# ---------------------------------------------------------------------------

def _enrich_row(
    row: dict[str, Any],
    role_dists: RoleDistributions,
    include_breakdown: bool = False,
) -> PlayerSeasonRow:
    """
    Arricchisce una riga DB con metriche derivate (per-90, percentuali)
    e punteggi FIFA-style normalizzati per ruolo.
    """
    raw = _row_to_stats_dict(row)
    derived = calculate_all_metrics(raw)
    scores = calculate_player_score(raw, role_dists)

    return PlayerSeasonRow(
        player_id=row["player_id"],
        api_player_id=row.get("api_player_id") or 0,
        name=row.get("name") or "",
        position=raw["position"],
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
        attack_score=scores.get("attack_score"),
        creation_score=scores.get("creation_score"),
        defense_score=scores.get("defense_score"),
        impact_score=scores.get("impact_score"),
        discipline_malus=scores.get("discipline_malus"),
        breakdown=scores.get("breakdown") if include_breakdown else None,
    )


def _rows_to_list(
    rows: list,
    role_dists: RoleDistributions,
    include_breakdown: bool = False,
) -> list[PlayerSeasonRow]:
    """Converte righe SQL in lista arricchita, ordinata per overall_score DESC."""
    result = [_enrich_row(dict(r), role_dists, include_breakdown) for r in rows]
    result.sort(
        key=lambda p: (p.overall_score is not None, p.overall_score or 0),
        reverse=True,
    )
    return result


# ---------------------------------------------------------------------------
# API pubblica
# ---------------------------------------------------------------------------

def get_team_players(
    team_id: int,
    season: int,
    db: Session,
    include_breakdown: bool = False,
) -> list[PlayerSeasonRow]:
    """
    Rosa giocatori con statistiche stagionali, metriche derivate e scoring
    FIFA-style normalizzato per ruolo.

    Flusso:
      1. Calcola distribuzioni empiriche per ruolo (tutti i giocatori della stagione)
      2. Carica i giocatori della squadra richiesta
      3. Per ogni giocatore: percentile per ruolo -> shrinkage -> Tier A/B/C -> malus
      4. Ordina per overall_score DESC

    Tenta lo schema nuovo; fallback su legacy se colonne mancanti.
    """
    role_dists = _compute_role_distributions(season, db)
    params = {"team_id": team_id, "season": season}

    try:
        rows = db.execute(TEAM_PLAYERS_SQL, params).mappings().all()
        return _rows_to_list(rows, role_dists, include_breakdown)
    except Exception as e:
        logger.warning(
            "Query players (schema nuovo) fallita per team_id=%s season=%s: %s. Provo legacy.",
            team_id, season, e,
        )
        db.rollback()

    try:
        rows = db.execute(TEAM_PLAYERS_SQL_LEGACY, params).mappings().all()
        return _rows_to_list(rows, role_dists, include_breakdown)
    except Exception as e:
        logger.exception(
            "Anche query legacy fallita per team_id=%s season=%s: %s",
            team_id, season, e,
        )
        db.rollback()
        return []
