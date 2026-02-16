"""
Servizio di ingestion rosa giocatori per squadra e stagione.
Chiama API-Sports /players, upsert in players e player_season_stats.
Estrae TUTTE le statistiche disponibili da API-Football v3.
Separato dall'ingestion fixture per non interferire con il flusso esistente.
"""

import logging
from typing import Any

from sqlalchemy.orm import Session

from app.models import Player, PlayerSeasonStats
from app.services.api_sports_client import ApiSportsClient

logger = logging.getLogger(__name__)

STATS_DB_FIELDS = [
    "appearances",
    "lineups",
    "minutes",
    "rating",
    "captain",
    "shots_total",
    "shots_on",
    "goals",
    "assists",
    "goals_conceded",
    "saves",
    "passes_total",
    "key_passes",
    "passes_accuracy",
    "tackles_total",
    "blocks",
    "interceptions",
    "duels_total",
    "duels_won",
    "dribbles_attempts",
    "dribbles_success",
    "dribbled_past",
    "fouls_drawn",
    "fouls_committed",
    "yellow_cards",
    "red_cards",
    "penalty_won",
    "penalty_committed",
    "penalty_scored",
    "penalty_missed",
    "penalty_saved",
]


def _safe_int(value: Any) -> int | None:
    """Converte un valore in int; ritorna None se impossibile o None."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _safe_float(value: Any) -> float | None:
    """Converte un valore in float; ritorna None se impossibile o None."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _safe_bool(value: Any) -> bool | None:
    """Converte un valore in bool; ritorna None se impossibile."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    try:
        return bool(value)
    except (ValueError, TypeError):
        return None


def _extract_player_data(item: dict[str, Any]) -> dict[str, Any] | None:
    """
    Estrae dati anagrafici e TUTTE le statistiche da un elemento della response
    API-Sports /players. Ogni elemento ha struttura:
        { player: {...}, statistics: [{games, shots, goals, passes, ...}] }
    Ritorna None se mancano dati essenziali (api_player_id, name).
    Ogni accesso nested usa .get() con fallback — mai KeyError/TypeError.
    """
    player_info = item.get("player") or {}
    api_player_id = player_info.get("id")
    if not api_player_id:
        return None

    name = player_info.get("name") or ""
    if not name:
        firstname = player_info.get("firstname") or ""
        lastname = player_info.get("lastname") or ""
        name = f"{firstname} {lastname}".strip()
    if not name:
        return None

    statistics_list = item.get("statistics") or []
    if not statistics_list:
        return {
            "api_player_id": int(api_player_id),
            "name": name,
            "age": player_info.get("age"),
            "nationality": player_info.get("nationality"),
            "position": player_info.get("position"),
            **{field: None for field in STATS_DB_FIELDS},
        }

    stats = statistics_list[0] if isinstance(statistics_list[0], dict) else {}

    games = stats.get("games") or {}
    shots = stats.get("shots") or {}
    goals_data = stats.get("goals") or {}
    passes = stats.get("passes") or {}
    tackles = stats.get("tackles") or {}
    duels = stats.get("duels") or {}
    dribbles = stats.get("dribbles") or {}
    fouls = stats.get("fouls") or {}
    cards = stats.get("cards") or {}
    penalty = stats.get("penalty") or {}

    return {
        "api_player_id": int(api_player_id),
        "name": name,
        "age": player_info.get("age"),
        "nationality": player_info.get("nationality"),
        "position": games.get("position") or player_info.get("position"),

        "appearances": _safe_int(games.get("appearences")),
        "lineups": _safe_int(games.get("lineups")),
        "minutes": _safe_int(games.get("minutes")),
        "rating": _safe_float(games.get("rating")),
        "captain": _safe_bool(games.get("captain")),

        "shots_total": _safe_int(shots.get("total")),
        "shots_on": _safe_int(shots.get("on")),

        "goals": _safe_int(goals_data.get("total")),
        "assists": _safe_int(goals_data.get("assists")),
        "goals_conceded": _safe_int(goals_data.get("conceded")),
        "saves": _safe_int(goals_data.get("saves")),

        "passes_total": _safe_int(passes.get("total")),
        "key_passes": _safe_int(passes.get("key")),
        "passes_accuracy": _safe_float(passes.get("accuracy")),

        "tackles_total": _safe_int(tackles.get("total")),
        "blocks": _safe_int(tackles.get("blocks")),
        "interceptions": _safe_int(tackles.get("interceptions")),

        "duels_total": _safe_int(duels.get("total")),
        "duels_won": _safe_int(duels.get("won")),

        "dribbles_attempts": _safe_int(dribbles.get("attempts")),
        "dribbles_success": _safe_int(dribbles.get("success")),
        "dribbled_past": _safe_int(dribbles.get("past")),

        "fouls_drawn": _safe_int(fouls.get("drawn")),
        "fouls_committed": _safe_int(fouls.get("committed")),

        "yellow_cards": _safe_int(cards.get("yellow")),
        "red_cards": _safe_int(cards.get("red")),

        "penalty_won": _safe_int(penalty.get("won")),
        "penalty_committed": _safe_int(penalty.get("commited")),
        "penalty_scored": _safe_int(penalty.get("scored")),
        "penalty_missed": _safe_int(penalty.get("missed")),
        "penalty_saved": _safe_int(penalty.get("saved")),
    }


def _build_stats_dict(data: dict[str, Any]) -> dict[str, Any]:
    """Estrae dal dict completo solo i campi statistici (per upsert DB)."""
    return {field: data.get(field) for field in STATS_DB_FIELDS}


async def ingest_team_players(team_id: int, season: int, db: Session) -> int:
    """
    Ingestion rosa giocatori per una squadra e stagione.

    Flusso:
    1. Chiama API-Sports GET /players?team={team_id}&season={season} (paginato)
    2. Per ogni giocatore: upsert in tabella players (anagrafica)
    3. Upsert in player_season_stats con TUTTE le statistiche
    4. Commit in un'unica transaction
    5. Se un singolo giocatore fallisce, logga e continua con il prossimo

    Ritorna il numero di giocatori processati con successo.
    """
    client = ApiSportsClient()

    logger.info(
        "=== INIZIO ingestion giocatori team_id=%s season=%s ===",
        team_id, season,
    )

    # --- Chiamata API ---
    try:
        raw_players = await client.get_team_players(team_id=team_id, season=season)
    except Exception:
        logger.exception(
            "FATAL: errore chiamata API-Sports per giocatori team_id=%s season=%s",
            team_id, season,
        )
        raise

    if not raw_players:
        logger.warning(
            "API-Sports ha restituito 0 giocatori per team_id=%s season=%s",
            team_id, season,
        )
        return 0

    logger.info(
        "API-Sports ha restituito %s giocatori per team_id=%s season=%s",
        len(raw_players), team_id, season,
    )

    # Log del primo giocatore per debug struttura API
    if raw_players:
        first = raw_players[0]
        first_player_info = (first.get("player") or {})
        first_stats_list = first.get("statistics") or []
        first_stat_keys = list(first_stats_list[0].keys()) if first_stats_list and isinstance(first_stats_list[0], dict) else []
        logger.info(
            "DEBUG primo giocatore: id=%s name=%s, statistics[0] keys=%s",
            first_player_info.get("id"),
            first_player_info.get("name"),
            first_stat_keys,
        )

    processed = 0
    skipped = 0
    errors = 0

    for idx, item in enumerate(raw_players):
        try:
            data = _extract_player_data(item)
            if not data:
                skipped += 1
                continue

            # --- Upsert Player (anagrafica) ---
            player = db.query(Player).filter(
                Player.api_player_id == data["api_player_id"]
            ).first()

            if player:
                player.name = data["name"]
                player.age = data["age"]
                player.nationality = data["nationality"]
                player.position = data["position"]
            else:
                player = Player(
                    api_player_id=data["api_player_id"],
                    name=data["name"],
                    age=data["age"],
                    nationality=data["nationality"],
                    position=data["position"],
                )
                db.add(player)
                db.flush()

            # --- Upsert PlayerSeasonStats ---
            stats_dict = _build_stats_dict(data)

            existing = (
                db.query(PlayerSeasonStats)
                .filter(
                    PlayerSeasonStats.player_id == player.id,
                    PlayerSeasonStats.team_id == team_id,
                    PlayerSeasonStats.season == season,
                )
                .first()
            )
            if existing:
                for field, value in stats_dict.items():
                    setattr(existing, field, value)
            else:
                db.add(
                    PlayerSeasonStats(
                        player_id=player.id,
                        team_id=team_id,
                        season=season,
                        **stats_dict,
                    )
                )

            processed += 1

        except Exception:
            errors += 1
            player_name = "?"
            try:
                player_name = (item.get("player") or {}).get("name", "?")
            except Exception:
                pass
            logger.exception(
                "Errore su giocatore #%s (%s) — team_id=%s season=%s. Skip e continuo.",
                idx, player_name, team_id, season,
            )
            try:
                db.rollback()
            except Exception:
                pass

    # --- Commit finale ---
    try:
        db.commit()
    except Exception:
        db.rollback()
        logger.exception(
            "FATAL: errore commit finale team_id=%s season=%s "
            "(processati=%s, errori=%s)",
            team_id, season, processed, errors,
        )
        raise

    logger.info(
        "=== FINE ingestion giocatori team_id=%s season=%s — "
        "processati=%s, skippati=%s, errori=%s (su %s totali dalla API) ===",
        team_id, season, processed, skipped, errors, len(raw_players),
    )

    return processed
