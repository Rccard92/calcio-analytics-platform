"""
Endpoint di test connessione API-Sports.
Leggero, non avvia ingestion, sicuro per free plan.
"""

import logging

from fastapi import APIRouter, HTTPException

from app.services.api_sports_client import ApiSportsClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["api"])


@router.get("/test")
async def api_test():
    """
    Testa la connessione a API-Sports (endpoint /status).
    Restituisce status HTTP e header di rate limit. Non consuma quota pesante.
    """
    try:
        client = ApiSportsClient()
        result = await client.test_connection()
        return result
    except RuntimeError as e:
        logger.warning("api_test config error: %s", e)
        raise HTTPException(
            status_code=503,
            detail={"ok": False, "error": str(e), "hint": "Verificare API_SPORTS_KEY"},
        )
    except Exception as e:
        logger.exception("api_test failed: %s", e)
        raise HTTPException(
            status_code=502,
            detail={"ok": False, "error": f"{type(e).__name__}: {e}"},
        )
