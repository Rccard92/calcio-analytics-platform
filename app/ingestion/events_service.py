"""
Ingestion eventi da API-Football per fixture Serie A.
Idempotente: cancella e reinserisce gli eventi per ogni fixture processata.
"""

import asyncio
import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.services.api_sports_client import ApiSportsClient

logger = logging.getLogger(__name__)

SERIE_A_LEAGUE_ID = 135
DELAY_BETWEEN_REQUESTS = 1.2


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


def _get_already_ingested_fixture_ids(db: Session) -> set[int]:
    """Ritorna fixture_id gia' presenti in fixture_events."""
    try:
        rows = db.execute(
            text("SELECT DISTINCT fixture_id FROM fixture_events")
        ).fetchall()
        return {r[0] for r in rows}
    except Exception:
        return set()


async def ingest_events_for_season(
    season: int,
    db: Session,
    batch_size: int = 50,
) -> dict[str, Any]:
    """
    Scarica e salva gli eventi per tutte le fixture FT di una stagione.
    Idempotente, incrementale, rate-limit safe.

    Returns: { fixtures_processed, events_inserted, skipped, errors }
    """
    all_fixture_ids = _get_finished_fixture_ids(season, db)
    already_done = _get_already_ingested_fixture_ids(db)
    pending = [fid for fid in all_fixture_ids if fid not in already_done]

    logger.info(
        "Events ingestion season=%s: %d fixture totali, %d gia' presenti, %d da processare",
        season, len(all_fixture_ids), len(already_done), len(pending),
    )

    if not pending:
        return {
            "fixtures_processed": 0, "events_inserted": 0,
            "skipped": len(already_done), "errors": 0,
        }

    if batch_size > 0:
        pending = pending[:batch_size]

    client = ApiSportsClient()
    processed = 0
    total_events = 0
    errors = 0

    for fixture_id in pending:
        try:
            raw_events = await client.get_fixture_events(fixture_id)

            db.execute(
                text("DELETE FROM fixture_events WHERE fixture_id = :fid"),
                {"fid": fixture_id},
            )

            for ev in raw_events:
                team_info = ev.get("team", {})
                team_id = _resolve_team_id(team_info.get("id"), db)
                if team_id is None:
                    continue

                time_info = ev.get("time", {})
                player_info = ev.get("player", {})
                assist_info = ev.get("assist", {})

                db.execute(
                    text("""
                        INSERT INTO fixture_events
                            (fixture_id, team_id, minute, extra_minute,
                             type, detail,
                             api_player_id, player_name,
                             api_assist_player_id, assist_player_name)
                        VALUES
                            (:fixture_id, :team_id, :minute, :extra_minute,
                             :type, :detail,
                             :api_player_id, :player_name,
                             :api_assist_player_id, :assist_player_name)
                    """),
                    {
                        "fixture_id": fixture_id,
                        "team_id": team_id,
                        "minute": time_info.get("elapsed"),
                        "extra_minute": time_info.get("extra"),
                        "type": ev.get("type", "Unknown"),
                        "detail": ev.get("detail"),
                        "api_player_id": player_info.get("id"),
                        "player_name": player_info.get("name"),
                        "api_assist_player_id": assist_info.get("id"),
                        "assist_player_name": assist_info.get("name"),
                    },
                )
                total_events += 1

            db.commit()
            processed += 1

        except Exception as e:
            logger.warning("Errore events fixture_id=%s: %s", fixture_id, e)
            db.rollback()
            errors += 1

        await asyncio.sleep(DELAY_BETWEEN_REQUESTS)

    result = {
        "fixtures_processed": processed,
        "events_inserted": total_events,
        "skipped": len(already_done),
        "errors": errors,
    }
    logger.info("Events ingestion completata: %s", result)
    return result


def _resolve_team_id(api_team_id: int | None, db: Session) -> int | None:
    """Risolve api_team_id al team.id interno."""
    if not api_team_id:
        return None
    row = db.execute(
        text("SELECT id FROM teams WHERE id = :tid"),
        {"tid": api_team_id},
    ).fetchone()
    return row[0] if row else None
