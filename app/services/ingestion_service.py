"""
Servizio di ingestion: un job per stagione, esecuzione in background.
Nessuna logica esposta direttamente negli endpoint; sessioni DB dedicate al job.
"""

import logging
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from app.core.database import SessionLocal
from app.models import Fixture, IngestionJob, League, Team, TeamMatchStats
from app.services.api_sports_client import ApiSportsClient

logger = logging.getLogger(__name__)

SERIE_A_LEAGUE_ID = 135


def _stat_value(statistics: list[dict], key: str) -> int | float | None:
    """Estrae un valore numerico dalle statistiche API (es. 'Shots on Goal' -> int)."""
    for s in statistics:
        if s.get("type") == key:
            try:
                v = str(s.get("value", "")).replace("%", "").strip()
                return int(v) if v.isdigit() else float(v) if v else None
            except (ValueError, TypeError):
                return None
    return None


def _map_api_stats_to_model(statistics: list[dict]) -> dict[str, Any]:
    """Mappa le statistiche API-Sports ai campi di TeamMatchStats."""
    return {
        "shots_total": _stat_value(statistics, "Total Shots") or _stat_value(statistics, "Shots Total"),
        "shots_on_target": _stat_value(statistics, "Shots on Goal") or _stat_value(statistics, "Shots on target"),
        "possession": _stat_value(statistics, "Ball Possession"),
        "fouls": _stat_value(statistics, "Fouls"),
        "corners": _stat_value(statistics, "Corner Kicks") or _stat_value(statistics, "Corners"),
        "yellow_cards": _stat_value(statistics, "Yellow Cards"),
        "red_cards": _stat_value(statistics, "Red Cards"),
    }


class IngestionService:
    """Gestisce l'ingestion di una stagione: fixture, squadre, statistiche."""

    def __init__(self, api_key: str | None = None):
        self._client = ApiSportsClient(api_key=api_key)

    def start_ingestion(self, season: int, force: bool = False) -> int:
        """
        Crea un job in stato pending e ritorna job_id (solo DB, sync).
        Se esiste già un job in esecuzione per la stagione → ValueError.
        Se esiste un job completato per la stagione e force=False → ValueError (usare force=True per riavviare).
        """
        db = SessionLocal()
        try:
            running = (
                db.query(IngestionJob)
                .filter(IngestionJob.season == season, IngestionJob.status == "running")
                .first()
            )
            if running:
                raise ValueError(f"È già in esecuzione un job per la stagione {season} (job_id={running.id})")
            completed = (
                db.query(IngestionJob)
                .filter(IngestionJob.season == season, IngestionJob.status == "completed")
                .first()
            )
            if completed and not force:
                raise ValueError(
                    f"Stagione {season} già completata (job_id={completed.id}). Usare force=true per riavviare."
                )
            job = IngestionJob(season=season, status="pending", total_fixtures=0, processed_fixtures=0)
            db.add(job)
            db.commit()
            db.refresh(job)
            return job.id
        finally:
            db.close()

    async def repair_fixture(self, fixture_id: int) -> dict[str, Any]:
        """
        Riparazione chirurgica: cancella stats esistenti per la fixture,
        richiede statistiche alla API e salva. Non tocca ingestion principale.
        Ritorna fixture_id, status, stats_saved; se API vuota stats_saved=0.
        """
        db = SessionLocal()
        try:
            fixture = db.query(Fixture).filter(Fixture.id == fixture_id).first()
            if not fixture:
                raise ValueError(f"Fixture {fixture_id} non trovata")
            deleted = db.query(TeamMatchStats).filter(TeamMatchStats.fixture_id == fixture_id).delete()
            db.commit()
            logger.info("repair_fixture fixture_id=%s: eliminate %s stats esistenti", fixture_id, deleted)
            raw = await self._client.get_fixture_statistics(fixture_id)
            if not raw:
                logger.warning("repair_fixture fixture_id=%s: API ha restituito lista vuota", fixture_id)
                return {
                    "fixture_id": fixture_id,
                    "status": "repaired",
                    "stats_saved": 0,
                    "message": "API non ha restituito statistiche",
                }
            for team_block in raw:
                team_info = team_block.get("team", {})
                team_id = team_info.get("id")
                if not team_id:
                    continue
                stats_list = team_block.get("statistics", [])
                mapped = _map_api_stats_to_model(stats_list)
                db.add(
                    TeamMatchStats(
                        fixture_id=fixture_id,
                        team_id=team_id,
                        **mapped,
                    )
                )
            db.commit()
            saved = len(raw)
            logger.info("repair_fixture fixture_id=%s: salvate %s stats", fixture_id, saved)
            return {
                "fixture_id": fixture_id,
                "status": "repaired",
                "stats_saved": saved,
            }
        except ValueError:
            raise
        except Exception as e:
            logger.exception("repair_fixture fixture_id=%s errore: %s", fixture_id, e)
            raise
        finally:
            db.close()

    def _update_job(
        self,
        db: Session,
        job_id: int,
        status: str | None = None,
        total_fixtures: int | None = None,
        processed_fixtures: int | None = None,
        error_message: str | None = None,
    ) -> None:
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if not job:
            return
        if status is not None:
            job.status = status
        if total_fixtures is not None:
            job.total_fixtures = total_fixtures
        if processed_fixtures is not None:
            job.processed_fixtures = processed_fixtures
        if error_message is not None:
            job.error_message = error_message
        job.updated_at = datetime.utcnow()
        db.commit()

    async def process_season(self, job_id: int) -> None:
        """
        Esegue l'ingestion per il job: fixture API -> DB, statistiche per ogni fixture.
        Usa una sessione DB dedicata; committa spesso per non tenere transazioni lunghe.
        In caso di errore imposta status=failed e error_message.
        """
        db = SessionLocal()
        try:
            job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
            if not job:
                logger.error("Ingestion job_id=%s non trovato", job_id)
                return
            if job.status != "pending":
                logger.warning("Job %s non in pending, skip", job_id)
                return

            self._update_job(db, job_id, status="running")
            logger.info("Ingestion job_id=%s season=%s avviato", job_id, job.season)

            fixtures_data = await self._client.get_fixtures(league=SERIE_A_LEAGUE_ID, season=job.season)
            total = len(fixtures_data)
            self._update_job(db, job_id, total_fixtures=total)

            # Assicura che la league esista
            league = db.query(League).filter(League.id == SERIE_A_LEAGUE_ID).first()
            if not league:
                first = fixtures_data[0] if fixtures_data else {}
                league_info = first.get("league", {})
                league = League(
                    id=SERIE_A_LEAGUE_ID,
                    name=league_info.get("name", "Serie A"),
                    country=league_info.get("country", "Italy"),
                )
                db.add(league)
                db.commit()

            processed = 0
            for item in fixtures_data:
                try:
                    self._upsert_fixture_and_teams(db, job.season, item)
                    fixture_api_id = item.get("fixture", {}).get("id")
                    if fixture_api_id:
                        await self._fetch_and_save_statistics(db, int(fixture_api_id))
                    processed += 1
                    self._update_job(db, job_id, processed_fixtures=processed)
                except Exception as e:
                    logger.exception("Errore elaborazione fixture job_id=%s: %s", job_id, e)
                    self._update_job(
                        db,
                        job_id,
                        status="failed",
                        processed_fixtures=processed,
                        error_message=f"{type(e).__name__}: {e}",
                    )
                    return

            self._update_job(db, job_id, status="completed", processed_fixtures=processed)
            logger.info("Ingestion job_id=%s completato: %s/%s", job_id, processed, total)
        except Exception as e:
            logger.exception("Ingestion job_id=%s fallito: %s", job_id, e)
            self._update_job(
                db,
                job_id,
                status="failed",
                error_message=f"{type(e).__name__}: {e}",
            )
        finally:
            db.close()

    def _upsert_fixture_and_teams(self, db: Session, season: int, item: dict) -> None:
        """Inserisce o aggiorna teams e fixture a partire dalla risposta API."""
        fixture_obj = item.get("fixture", {})
        league_obj = item.get("league", {})
        teams_obj = item.get("teams", {})
        goals_obj = item.get("goals", {})

        home = teams_obj.get("home", {})
        away = teams_obj.get("away", {})
        home_id = home.get("id")
        away_id = away.get("id")
        if not home_id or not away_id:
            return

        for tid, tdata in [(home_id, home), (away_id, away)]:
            team = db.query(Team).filter(Team.id == tid).first()
            if not team:
                db.add(Team(id=tid, name=tdata.get("name", ""), logo=tdata.get("logo")))
        db.flush()

        date_str = fixture_obj.get("date")
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00")) if date_str else datetime.utcnow()
        round_name = league_obj.get("round") or fixture_obj.get("round")
        status_short = (fixture_obj.get("status") or {}).get("short")

        fixture_id = fixture_obj.get("id")
        if not fixture_id:
            return
        existing = db.query(Fixture).filter(Fixture.id == fixture_id).first()
        if existing:
            existing.season = season
            existing.date = dt
            existing.round = round_name
            existing.status = status_short
            existing.home_team_id = home_id
            existing.away_team_id = away_id
            existing.home_goals = goals_obj.get("home") if goals_obj else None
            existing.away_goals = goals_obj.get("away") if goals_obj else None
        else:
            db.add(
                Fixture(
                    id=fixture_id,
                    league_id=SERIE_A_LEAGUE_ID,
                    season=season,
                    date=dt,
                    round=round_name,
                    status=status_short,
                    home_team_id=home_id,
                    away_team_id=away_id,
                    home_goals=goals_obj.get("home") if goals_obj else None,
                    away_goals=goals_obj.get("away") if goals_obj else None,
                )
            )
        db.commit()

    async def _fetch_and_save_statistics(self, db: Session, fixture_id: int) -> None:
        """Recupera statistiche dalla API e salva TeamMatchStats per la fixture."""
        raw = await self._client.get_fixture_statistics(fixture_id)
        for team_block in raw:
            team_info = team_block.get("team", {})
            team_id = team_info.get("id")
            if not team_id:
                continue
            stats_list = team_block.get("statistics", [])
            mapped = _map_api_stats_to_model(stats_list)
            existing = (
                db.query(TeamMatchStats)
                .filter(TeamMatchStats.fixture_id == fixture_id, TeamMatchStats.team_id == team_id)
                .first()
            )
            if existing:
                for k, v in mapped.items():
                    setattr(existing, k, v)
            else:
                db.add(
                    TeamMatchStats(
                        fixture_id=fixture_id,
                        team_id=team_id,
                        **mapped,
                    )
                )
        db.commit()
