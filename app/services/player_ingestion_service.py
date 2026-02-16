"""
Servizio di ingestion rosa giocatori per squadra e stagione.
Chiama API-Sports /players, upsert in players e player_season_stats.
Separato dall'ingestion fixture per non interferire con il flusso esistente.
"""

import logging
from typing import Any

from sqlalchemy.orm import Session

from app.models import Player, PlayerSeasonStats
from app.services.api_sports_client import ApiSportsClient

logger = logging.getLogger(__name__)


def _extract_player_data(item: dict[str, Any]) -> dict[str, Any] | None:
    """
    Estrae dati anagrafici e statistiche da un elemento della response API-Sports /players.
    Ogni elemento ha struttura: { player: {...}, statistics: [{...}] }
    Ritorna None se mancano dati essenziali.
    """
    player_info = item.get("player", {})
    api_player_id = player_info.get("id")
    if not api_player_id:
        return None

    name = player_info.get("name") or f"{player_info.get('firstname', '')} {player_info.get('lastname', '')}".strip()
    if not name:
        return None

    statistics_list = item.get("statistics", [])
    stats = statistics_list[0] if statistics_list else {}

    games = stats.get("games", {}) or {}
    goals_data = stats.get("goals", {}) or {}
    shots_data = stats.get("shots", {}) or {}
    passes_data = stats.get("passes", {}) or {}

    return {
        "api_player_id": int(api_player_id),
        "name": name,
        "age": player_info.get("age"),
        "nationality": player_info.get("nationality"),
        "position": games.get("position") or player_info.get("position"),
        "appearances": games.get("appearences"),  # NB: typo nell'API ("appearences")
        "minutes": games.get("minutes"),
        "rating": _safe_float(games.get("rating")),
        "goals": goals_data.get("total"),
        "assists": goals_data.get("assists"),
        "shots_total": shots_data.get("total"),
        "shots_on_target": shots_data.get("on"),
        "passes_accuracy": _safe_float(passes_data.get("accuracy")),
    }


def _safe_float(value: Any) -> float | None:
    """Converte un valore in float; ritorna None se impossibile."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


async def ingest_team_players(team_id: int, season: int, db: Session) -> int:
    """
    Ingestion rosa giocatori per una squadra e stagione.

    Flusso:
    1. Chiama API-Sports GET /players?team={team_id}&season={season}
    2. Per ogni giocatore: upsert in tabella players
    3. Upsert in player_season_stats (unique su player_id + team_id + season)
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

    try:
        for item in raw_players:
            data = _extract_player_data(item)
            if not data:
                continue

            # --- Upsert Player ---
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
                db.flush()  # per ottenere player.id

            # --- Upsert PlayerSeasonStats ---
            stats = (
                db.query(PlayerSeasonStats)
                .filter(
                    PlayerSeasonStats.player_id == player.id,
                    PlayerSeasonStats.team_id == team_id,
                    PlayerSeasonStats.season == season,
                )
                .first()
            )
            if stats:
                stats.appearances = data["appearances"]
                stats.minutes = data["minutes"]
                stats.goals = data["goals"]
                stats.assists = data["assists"]
                stats.shots = data["shots_total"]
                stats.shots_on_target = data["shots_on_target"]
                stats.passes_accuracy = data["passes_accuracy"]
                stats.rating = data["rating"]
            else:
                db.add(
                    PlayerSeasonStats(
                        player_id=player.id,
                        team_id=team_id,
                        season=season,
                        appearances=data["appearances"],
                        minutes=data["minutes"],
                        goals=data["goals"],
                        assists=data["assists"],
                        shots=data["shots_total"],
                        shots_on_target=data["shots_on_target"],
                        passes_accuracy=data["passes_accuracy"],
                        rating=data["rating"],
                    )
                )

            processed += 1

        db.commit()
        logger.info(
            "Ingestion giocatori completata: team_id=%s season=%s — %s giocatori processati su %s dalla API",
            team_id, season, processed, len(raw_players),
        )

    except Exception as e:
        db.rollback()
        logger.exception(
            "Errore durante upsert giocatori team_id=%s season=%s: %s", team_id, season, e
        )
        raise

    return processed
