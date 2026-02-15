"""
Router per avvio e stato dei job di ingestion.
La logica è nel service; il server non si blocca (background task).
"""

import logging

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.models import IngestionJob
from app.services.ingestion_service import IngestionService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ingestion", tags=["ingestion"])

DEFAULT_SEASON = 2026


async def _run_ingestion_background(job_id: int) -> None:
    """Esegue process_season in background senza bloccare il server."""
    try:
        service = IngestionService()
        await service.process_season(job_id)
    except Exception as e:
        logger.exception("Background ingestion job_id=%s errore: %s", job_id, e)


@router.post("/start")
def start_ingestion(
    season: int = DEFAULT_SEASON,
    background_tasks: BackgroundTasks = None,
):
    """
    Avvia l'ingestion per la stagione in background.
    Ritorna subito con job_id e status 'started'.
    Se per la stessa stagione c'è già un job in esecuzione → 409.
    """
    try:
        service = IngestionService()
        job_id = service.start_ingestion(season)
    except ValueError as e:
        logger.warning("Start ingestion rifiutato: %s", e)
        raise HTTPException(status_code=409, detail=str(e))
    except RuntimeError as e:
        logger.exception("Config errata per ingestion: %s", e)
        raise HTTPException(status_code=503, detail="Ingestion non configurata (API_SPORTS_KEY?)")

    background_tasks.add_task(_run_ingestion_background, job_id)
    return {"job_id": job_id, "status": "started"}


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
