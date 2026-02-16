"""
Router per avvio e stato dei job di ingestion.
La logica è nel service; il server non si blocca (background task).
Include ingestion lineups e events per Serie A.
"""

import logging

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.ingestion.events_service import ingest_events_for_season
from app.ingestion.lineups_service import ingest_lineups_for_season
from app.models import IngestionJob
from app.services.api_sports_client import ApiSportsClient
from app.services.ingestion_service import IngestionService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ingestion", tags=["ingestion"])

SERIE_A_LEAGUE_ID = 135


async def _run_ingestion_background(job_id: int) -> None:
    """Esegue process_season in background senza bloccare il server."""
    try:
        service = IngestionService()
        await service.process_season(job_id)
    except Exception as e:
        logger.exception("Background ingestion job_id=%s errore: %s", job_id, e)


@router.get("/seasons")
async def get_seasons():
    """
    Restituisce le stagioni disponibili per la Serie A (league_id=135).
    Popola la dropdown senza hardcodare gli anni.
    """
    try:
        client = ApiSportsClient()
        seasons = await client.get_league_seasons(league_id=SERIE_A_LEAGUE_ID)
        return {"league_id": SERIE_A_LEAGUE_ID, "seasons": seasons}
    except RuntimeError as e:
        logger.warning("get_seasons config error: %s", e)
        raise HTTPException(
            status_code=503,
            detail="API non configurata (API_SPORTS_KEY?). Impossibile recuperare le stagioni.",
        )
    except Exception as e:
        logger.exception("get_seasons failed: %s", e)
        raise HTTPException(status_code=502, detail=f"Errore recupero stagioni: {e}")


@router.post("/start")
async def start_ingestion(
    season: int,
    force: bool = False,
    background_tasks: BackgroundTasks = None,
):
    """
    Avvia l'ingestion per la stagione selezionata.
    Valida che la stagione sia tra quelle disponibili; un solo job in esecuzione per stagione.
    Con force=true è possibile riavviare una stagione già completata.
    """
    try:
        client = ApiSportsClient()
        available = await client.get_league_seasons(league_id=SERIE_A_LEAGUE_ID)
        if season not in available:
            raise HTTPException(
                status_code=400,
                detail=f"Stagione {season} non disponibile. Stagioni valide: {available}",
            )
    except HTTPException:
        raise
    except RuntimeError as e:
        logger.exception("Start ingestion config: %s", e)
        raise HTTPException(status_code=503, detail="Ingestion non configurata (API_SPORTS_KEY?)")
    except Exception as e:
        logger.exception("Start ingestion pre-check: %s", e)
        raise HTTPException(status_code=502, detail=f"Errore validazione stagione: {e}")

    try:
        service = IngestionService()
        job_id = service.start_ingestion(season=season, force=force)
    except ValueError as e:
        logger.warning("Start ingestion rifiutato: %s", e)
        raise HTTPException(status_code=409, detail=str(e))

    background_tasks.add_task(_run_ingestion_background, job_id)
    return {"job_id": job_id, "status": "started", "season": season}


@router.post("/repair-fixture/{fixture_id}")
async def repair_fixture(fixture_id: int):
    """
    Riparazione chirurgica: elimina stats esistenti per la fixture,
    richiede statistiche alla API e le salva. Non rifà l'ingestion.
    """
    try:
        service = IngestionService()
        result = await service.repair_fixture(fixture_id)
        return result
    except ValueError as e:
        logger.warning("repair_fixture %s: %s", fixture_id, e)
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        logger.exception("repair_fixture config: %s", e)
        raise HTTPException(status_code=503, detail="API non configurata (API_SPORTS_KEY?)")
    except Exception as e:
        logger.exception("repair_fixture fixture_id=%s: %s", fixture_id, e)
        raise HTTPException(status_code=502, detail=f"Errore riparazione: {e}")


@router.get("/status/{job_id}")
def ingestion_status(job_id: int, db: Session = Depends(get_db)):
    """
    Ritorna stato del job: progress_percentage, error_message se failed.
    """
    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job non trovato")
    total = job.total_fixtures or 0
    processed = job.processed_fixtures or 0
    progress = round(processed / total * 100, 2) if total else 0.0
    return {
        "job_id": job.id,
        "season": job.season,
        "status": job.status,
        "total_fixtures": total,
        "processed_fixtures": processed,
        "progress_percentage": progress,
        "error_message": job.error_message,
    }


# -----------------------------------------------------------------------
# Ingestion lineups e events per Serie A
# -----------------------------------------------------------------------


@router.post("/lineups/{season}")
async def ingest_lineups(
    season: int,
    batch_size: int = 50,
    db: Session = Depends(get_db),
):
    """
    Scarica formazioni per tutte le fixture FT della stagione.
    Incrementale: salta fixture gia' processate.
    batch_size: numero massimo di fixture per chiamata (0 = tutte).
    """
    try:
        result = await ingest_lineups_for_season(
            season=season, db=db, batch_size=batch_size,
        )
        return {"season": season, **result}
    except Exception as e:
        logger.exception("Errore ingestion lineups season=%s: %s", season, e)
        raise HTTPException(status_code=500, detail=f"Errore ingestion lineups: {e}")


@router.post("/events/{season}")
async def ingest_events(
    season: int,
    batch_size: int = 50,
    db: Session = Depends(get_db),
):
    """
    Scarica eventi per tutte le fixture FT della stagione.
    Incrementale: salta fixture gia' processate.
    batch_size: numero massimo di fixture per chiamata (0 = tutte).
    """
    try:
        result = await ingest_events_for_season(
            season=season, db=db, batch_size=batch_size,
        )
        return {"season": season, **result}
    except Exception as e:
        logger.exception("Errore ingestion events season=%s: %s", season, e)
        raise HTTPException(status_code=500, detail=f"Errore ingestion events: {e}")
