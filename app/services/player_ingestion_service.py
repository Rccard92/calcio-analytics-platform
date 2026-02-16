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

# Mapping: chiave nel dict estratto → nome colonna DB in PlayerSeasonStats.
# Usato per upsert dinamico: evita duplicazione codice tra insert e update.
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
    Usa .get() sicuro su ogni nested dict — mai KeyError.
    """
    player_info = item.get("player") or {}
    api_player_id = player_info.get("id")
    if not api_player_id:
        return None

    name = player_info.get("name") or f"{player_info.get('firstname', '')} {player_info.get('lastname', '')}".strip()
    if not name:
        return None

    statistics_list = item.get("statistics") or []
    stats = statistics_list[0] if statistics_list else {}

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
        # --- Anagrafica ---
        "api_player_id": int(api_player_id),
        "name": name,
        "age": player_info.get("age"),
        "nationality": player_info.get("nationality"),
        "position": games.get("position") or player_info.get("position"),

        # --- GAMES ---
        "appearances": _safe_int(games.get("appearences")),  # typo nell'API
        "lineups": _safe_int(games.get("lineups")),
        "minutes": _safe_int(games.get("minutes")),
        "rating": _safe_float(games.get("rating")),
        "captain": _safe_bool(games.get("captain")),

        # --- SHOTS ---
        "shots_total": _safe_int(shots.get("total")),
        "shots_on": _safe_int(shots.get("on")),

        # --- GOALS ---
        "goals": _safe_int(goals_data.get("total")),
        "assists": _safe_int(goals_data.get("assists")),
        "goals_conceded": _safe_int(goals_data.get("conceded")),
        "saves": _safe_int(goals_data.get("saves")),

        # --- PASSES ---
        "passes_total": _safe_int(passes.get("total")),
        "key_passes": _safe_int(passes.get("key")),
        "passes_accuracy": _safe_float(passes.get("accuracy")),

        # --- TACKLES ---
        "tackles_total": _safe_int(tackles.get("total")),
        "blocks": _safe_int(tackles.get("blocks")),
        "interceptions": _safe_int(tackles.get("interceptions")),

        # --- DUELS ---
        "duels_total": _safe_int(duels.get("total")),
        "duels_won": _safe_int(duels.get("won")),

        # --- DRIBBLES ---
        "dribbles_attempts": _safe_int(dribbles.get("attempts")),
        "dribbles_success": _safe_int(dribbles.get("success")),
        "dribbled_past": _safe_int(dribbles.get("past")),

        # --- FOULS ---
        "fouls_drawn": _safe_int(fouls.get("drawn")),
        "fouls_committed": _safe_int(fouls.get("committed")),

        # --- CARDS ---
        "yellow_cards": _safe_int(cards.get("yellow")),
        "red_cards": _safe_int(cards.get("red")),

        # --- PENALTY ---
        "penalty_won": _safe_int(penalty.get("won")),
        "penalty_committed": _safe_int(penalty.get("commited")),  # typo nell'API
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
       Unique su (player_id, team_id, season)
    4. Commit in un'unica transaction

    Ritorna il numero di giocatori processati.
    """
    client = ApiSportsClient()

    logger.info("Avvio ingestion giocatori team_id=%s season=%s", team_id, season)

    try:
        raw_players = await client.get_team_players(team_id=team_id, season=season)
    except Exception as e:
        logger.exception("Errore chiamata API-Sports per giocatori team_id=%s season=%s: %s", team_id, season, e)
        raise

    if not raw_players:
        logger.warning("API-Sports ha restituito 0 giocatori per team_id=%s season=%s", team_id, season)
        return 0

    processed = 0
    fields_updated_total = 0

    try:
        for item in raw_players:
            data = _extract_player_data(item)
            if not data:
                continue

            # --- Upsert Player (anagrafica) ---
            player = db.query(Player).filter(Player.api_player_id == data["api_player_id"]).first()
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
            fields_count = sum(1 for v in stats_dict.values() if v is not None)
            fields_updated_total += fields_count

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

        db.commit()
        logger.info(
            "Ingestion giocatori completata: team_id=%s season=%s — "
            "%s giocatori processati su %s dalla API, %s campi statistici valorizzati",
            team_id, season, processed, len(raw_players), fields_updated_total,
        )

    except Exception as e:
        db.rollback()
        logger.exception(
            "Errore durante upsert giocatori team_id=%s season=%s: %s", team_id, season, e
        )
        raise

    return processed
