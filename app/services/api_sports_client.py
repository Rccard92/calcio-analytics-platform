"""
Client per API-Sports (api-football v3).
Usato solo dal layer di ingestion, mai direttamente dagli endpoint.
"""

import logging
from typing import Any

import httpx

from app.core.config import get_api_sports_key

logger = logging.getLogger(__name__)

BASE_URL = "https://v3.football.api-sports.io"


class ApiSportsClient:
    """Client async per API-Sports. League 135 = Serie A."""

    def __init__(self, api_key: str | None = None):
        self._api_key = api_key or get_api_sports_key()

    def _headers(self) -> dict[str, str]:
        return {"x-apisports-key": self._api_key}

    async def get_fixtures(self, league: int = 135, season: int = 2026) -> list[dict[str, Any]]:
        """
        Ritorna l'elenco delle fixture per league/season.
        Formato: lista di dict con fixture, league, teams, goals, ecc.
        """
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.get(
                f"{BASE_URL}/fixtures",
                params={"league": league, "season": season},
                headers=self._headers(),
            )
            r.raise_for_status()
            data = r.json()
        errors = data.get("errors", {})
        if errors:
            logger.warning("API-Sports errors: %s", errors)
        response = data.get("response", [])
        logger.info("get_fixtures league=%s season=%s -> %s fixture", league, season, len(response))
        return response

    async def get_fixture_statistics(self, fixture_id: int) -> list[dict[str, Any]]:
        """
        Ritorna le statistiche per la fixture (una entry per squadra).
        Ogni entry ha 'team' e 'statistics' (lista di {type, value}).
        """
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.get(
                f"{BASE_URL}/fixtures/statistics",
                params={"fixture": fixture_id},
                headers=self._headers(),
            )
            r.raise_for_status()
            data = r.json()
        response = data.get("response", [])
        logger.info("get_fixture_statistics fixture=%s -> %s teams", fixture_id, len(response))
        return response
