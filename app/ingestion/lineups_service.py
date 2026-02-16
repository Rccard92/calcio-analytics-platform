"""
Ingestion formazioni da API-Football per fixture Serie A.
Idempotente: usa ON CONFLICT su (fixture_id, api_player_id).
Calcola minutes_played da eventi di sostituzione.
"""

import asyncio
import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.services.api_sports_client import ApiSportsClient

logger = logging.getLogger(__name__)

SERIE_A_LEAGUE_ID = 135
DELAY_BETWEEN_REQUESTS = 1.2  # secondi tra richieste API (rate limit safe)


def _get_finished_fixture_ids(season: int, db: Session) -> list[int]:
    """Ritorna gli ID di tutte le fixture FT della stagione."""
    rows = db.execute(
        text("""
            SELECT id FROM fixtures
            WHERE league_id = :league AND season = :season AND status = 'FT'
            ORDER BY id
        """),
        {"league": SERIE_A_LEAGUE_ID, "season": season},
    ).fetchall()
    return [r[0] for r in rows]


def _get_already_ingested_fixture_ids(db: Session, table: str = "fixture_lineups") -> set[int]:
    """Ritorna fixture_id gia' presenti nella tabella."""
    try:
        rows = db.execute(
            text(f"SELECT DISTINCT fixture_id FROM {table}")
        ).fetchall()
        return {r[0] for r in rows}
    except Exception:
        return set()


def _map_position_code(pos_code: str | None) -> str:
    """Converte codice posizione API (G/D/M/F) in nome completo."""
    mapping = {"G": "Goalkeeper", "D": "Defender", "M": "Midfielder", "F": "Attacker"}
    return mapping.get(pos_code or "", "")


async def ingest_lineups_for_season(
    season: int,
    db: Session,
    batch_size: int = 50,
) -> dict[str, Any]:
    """
    Scarica e salva le formazioni per tutte le fixture FT di una stagione.
    Idempotente, incrementale, rate-limit safe.

    Returns: { fixtures_processed, lineups_inserted, skipped, errors }
    """
    all_fixture_ids = _get_finished_fixture_ids(season, db)
    already_done = _get_already_ingested_fixture_ids(db, "fixture_lineups")
    pending = [fid for fid in all_fixture_ids if fid not in already_done]

    logger.info(
        "Lineups ingestion season=%s: %d fixture totali, %d gia' presenti, %d da processare",
        season, len(all_fixture_ids), len(already_done), len(pending),
    )

    if not pending:
        return {
            "fixtures_processed": 0, "lineups_inserted": 0,
            "skipped": len(already_done), "errors": 0,
        }

    if batch_size > 0:
        pending = pending[:batch_size]

    client = ApiSportsClient()
    processed = 0
    inserted = 0
    errors = 0

    for fixture_id in pending:
        try:
            raw_lineups = await client.get_fixture_lineups(fixture_id)
            raw_events = await client.get_fixture_events(fixture_id)

            sub_map = _build_substitution_map(raw_events, fixture_id)

            for team_entry in raw_lineups:
                team_info = team_entry.get("team", {})
                team_id = _resolve_team_id(team_info.get("id"), db)
                if team_id is None:
                    continue

                for player_entry in team_entry.get("startXI", []):
                    p = player_entry.get("player", {})
                    api_pid = p.get("id")
                    if not api_pid:
                        continue
                    mins = _calc_minutes(api_pid, True, sub_map, fixture_id)
                    _upsert_lineup(
                        db, fixture_id, team_id, api_pid,
                        p.get("name"), _map_position_code(p.get("pos")),
                        True, mins,
                    )
                    inserted += 1

                for player_entry in team_entry.get("substitutes", []):
                    p = player_entry.get("player", {})
                    api_pid = p.get("id")
                    if not api_pid:
                        continue
                    mins = _calc_minutes(api_pid, False, sub_map, fixture_id)
                    _upsert_lineup(
                        db, fixture_id, team_id, api_pid,
                        p.get("name"), _map_position_code(p.get("pos")),
                        False, mins,
                    )
                    inserted += 1

            db.commit()
            processed += 1

        except Exception as e:
            logger.warning("Errore lineups fixture_id=%s: %s", fixture_id, e)
            db.rollback()
            errors += 1

        await asyncio.sleep(DELAY_BETWEEN_REQUESTS)

    result = {
        "fixtures_processed": processed,
        "lineups_inserted": inserted,
        "skipped": len(already_done),
        "errors": errors,
    }
    logger.info("Lineups ingestion completata: %s", result)
    return result


def _build_substitution_map(
    raw_events: list[dict[str, Any]], fixture_id: int,
) -> dict[int, int]:
    """
    Dai raw events, costruisce mappa: api_player_id -> minuto sostituzione.
    Il 'player' nell'evento subst e' chi esce, 'assist' e' chi entra.
    """
    sub_map: dict[int, int] = {}
    for ev in raw_events:
        if ev.get("type") != "subst":
            continue
        minute = (ev.get("time") or {}).get("elapsed") or 90
        player_out = (ev.get("player") or {}).get("id")
        player_in = (ev.get("assist") or {}).get("id")
        if player_out:
            sub_map[player_out] = minute
        if player_in:
            sub_map[player_in] = minute
    return sub_map


def _calc_minutes(
    api_player_id: int,
    is_starter: bool,
    sub_map: dict[int, int],
    fixture_id: int,
) -> int | None:
    """Calcola minuti giocati dal giocatore nella partita."""
    sub_minute = sub_map.get(api_player_id)
    if is_starter:
        return sub_minute if sub_minute else 90
    else:
        if sub_minute:
            return 90 - sub_minute
        return None


def _resolve_team_id(api_team_id: int | None, db: Session) -> int | None:
    """Risolve api_team_id al team.id interno. None se non trovato."""
    if not api_team_id:
        return None
    row = db.execute(
        text("SELECT id FROM teams WHERE id = :tid"),
        {"tid": api_team_id},
    ).fetchone()
    return row[0] if row else None


def _upsert_lineup(
    db: Session,
    fixture_id: int,
    team_id: int,
    api_player_id: int,
    player_name: str | None,
    position: str | None,
    is_starter: bool,
    minutes_played: int | None,
) -> None:
    """Upsert singola riga fixture_lineups con ON CONFLICT."""
    db.execute(
        text("""
            INSERT INTO fixture_lineups
                (fixture_id, team_id, api_player_id, player_name, position, is_starter, minutes_played)
            VALUES
                (:fixture_id, :team_id, :api_player_id, :player_name, :position, :is_starter, :minutes_played)
            ON CONFLICT (fixture_id, api_player_id)
            DO UPDATE SET
                team_id = EXCLUDED.team_id,
                player_name = EXCLUDED.player_name,
                position = EXCLUDED.position,
                is_starter = EXCLUDED.is_starter,
                minutes_played = EXCLUDED.minutes_played
        """),
        {
            "fixture_id": fixture_id, "team_id": team_id,
            "api_player_id": api_player_id, "player_name": player_name,
            "position": position, "is_starter": is_starter,
            "minutes_played": minutes_played,
        },
    )
