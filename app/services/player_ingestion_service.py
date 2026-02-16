"""
Servizio di ingestion rosa giocatori per squadra e stagione.
Chiama API-Sports /players, upsert in players e player_season_stats.
Estrae TUTTE le statistiche disponibili da API-Football v3.
Gestisce correttamente giocatori con statistiche multi-competizione
selezionando la entry giusta (Serie A / league principale).
"""

import logging
from typing import Any

from sqlalchemy.orm import Session

from app.models import Player, PlayerSeasonStats
from app.services.api_sports_client import ApiSportsClient

logger = logging.getLogger(__name__)

SERIE_A_LEAGUE_ID = 135

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
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _safe_bool(value: Any) -> bool | None:
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


def _pick_best_stat(statistics_list: list[dict], team_id: int, season: int) -> dict:
    """
    Seleziona la statistica migliore dall'array statistics[] di API-Football.

    Un giocatore può avere più entries: una per Serie A, una per Champions, ecc.
    Priorità:
      1. Stessa squadra + Serie A (league 135)
      2. Stessa squadra + stagione corretta + maggior numero di presenze
      3. Prima entry disponibile come fallback
    """
    if not statistics_list:
        return {}

    candidates = []
    for stat in statistics_list:
        if not isinstance(stat, dict):
            continue

        stat_team = (stat.get("team") or {})
        stat_league = (stat.get("league") or {})
        stat_team_id = stat_team.get("id")
        stat_league_id = stat_league.get("id")
        stat_season = stat_league.get("season")
        stat_appearances = ((stat.get("games") or {}).get("appearences")) or 0

        try:
            stat_appearances = int(stat_appearances)
        except (ValueError, TypeError):
            stat_appearances = 0

        candidates.append({
            "stat": stat,
            "team_match": stat_team_id == team_id,
            "league_id": stat_league_id,
            "season_match": stat_season == season,
            "is_serie_a": stat_league_id == SERIE_A_LEAGUE_ID,
            "appearances": stat_appearances,
        })

    if not candidates:
        return statistics_list[0] if statistics_list else {}

    # Ordina per: team match > Serie A > stagione > presenze
    candidates.sort(key=lambda c: (
        c["team_match"],
        c["is_serie_a"],
        c["season_match"],
        c["appearances"],
    ), reverse=True)

    best = candidates[0]
    return best["stat"]


def _extract_stats_from_block(stats: dict) -> dict[str, Any]:
    """Estrae tutti i campi statistici da un singolo blocco statistics entry."""
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
        "position": games.get("position"),
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


def _extract_player_data(
    item: dict[str, Any],
    team_id: int,
    season: int,
) -> dict[str, Any] | None:
    """
    Estrae dati anagrafici e statistiche da un elemento della response API-Sports.
    Seleziona la statistica giusta tra le multi-competizioni usando _pick_best_stat().
    Ritorna None se mancano dati essenziali.
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

    best_stat = _pick_best_stat(statistics_list, team_id, season)
    parsed = _extract_stats_from_block(best_stat)

    position = parsed.pop("position", None) or player_info.get("position")

    return {
        "api_player_id": int(api_player_id),
        "name": name,
        "age": player_info.get("age"),
        "nationality": player_info.get("nationality"),
        "position": position,
        **parsed,
    }


def _build_stats_dict(data: dict[str, Any]) -> dict[str, Any]:
    """Estrae dal dict completo solo i campi statistici (per upsert DB)."""
    return {field: data.get(field) for field in STATS_DB_FIELDS}


async def ingest_team_players(team_id: int, season: int, db: Session) -> int:
    """
    Ingestion rosa giocatori per una squadra e stagione.

    Flusso:
    1. Chiama API-Sports GET /players?team={team_id}&season={season} (paginato)
    2. Per ogni giocatore: seleziona la statistica della competizione giusta
    3. Upsert in players (anagrafica) e player_season_stats (statistiche complete)
    4. Commit in un'unica transaction
    5. Se un singolo giocatore fallisce, logga stacktrace e continua

    Ritorna il numero di giocatori processati con successo.
    """
    client = ApiSportsClient()

    logger.info(
        "=== INIZIO ingestion giocatori team_id=%s season=%s ===",
        team_id, season,
    )

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

    # --- Debug log del primo giocatore ---
    first = raw_players[0]
    first_info = first.get("player") or {}
    first_stats_list = first.get("statistics") or []
    logger.info(
        "DEBUG primo giocatore: id=%s name=%s, num_statistics=%s, leagues=%s",
        first_info.get("id"),
        first_info.get("name"),
        len(first_stats_list),
        [
            f"{(s.get('league') or {}).get('name', '?')} (id={((s.get('league') or {}).get('id', '?'))})"
            for s in first_stats_list
            if isinstance(s, dict)
        ],
    )
    if first_stats_list:
        best_first = _pick_best_stat(first_stats_list, team_id, season)
        parsed_first = _extract_stats_from_block(best_first)
        selected_league = (best_first.get("league") or {}).get("name", "?")
        logger.info(
            "DEBUG primo giocatore stat selezionata: league=%s, "
            "yellow=%s, fouls_committed=%s, tackles_total=%s, "
            "appearances=%s, minutes=%s, goals=%s, rating=%s",
            selected_league,
            parsed_first.get("yellow_cards"),
            parsed_first.get("fouls_committed"),
            parsed_first.get("tackles_total"),
            parsed_first.get("appearances"),
            parsed_first.get("minutes"),
            parsed_first.get("goals"),
            parsed_first.get("rating"),
        )

    processed = 0
    skipped = 0
    errors = 0

    for idx, item in enumerate(raw_players):
        try:
            data = _extract_player_data(item, team_id=team_id, season=season)
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
