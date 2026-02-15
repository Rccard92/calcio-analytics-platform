"""
Endpoint per le stagioni disponibili (Serie A).
Usato dalla dashboard per popolare la dropdown; non tocca l'ingestion.
"""

import logging

from fastapi import APIRouter, HTTPException

from app.services.api_sports_client import ApiSportsClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/leagues", tags=["leagues"])

SERIE_A_LEAGUE_ID = 135


@router.get("/seasons")
async def get_leagues_seasons():
    """
    Restituisce le stagioni disponibili per la Serie A (league_id=135).
    Chiama API-Sports /leagues?id=135 ed estrae la lista anni.
    """
    try:
        client = ApiSportsClient()
        seasons = await client.get_league_seasons(league_id=SERIE_A_LEAGUE_ID)
        return {"seasons": seasons}
    except RuntimeError as e:
        logger.warning("get_leagues_seasons config: %s", e)
        raise HTTPException(
            status_code=503,
            detail="API non configurata (API_SPORTS_KEY?). Impossibile recuperare le stagioni.",
        )
    except Exception as e:
        logger.exception("get_leagues_seasons failed: %s", e)
        raise HTTPException(status_code=502, detail=f"Errore recupero stagioni: {e}")
