"""
Client per API-Sports (api-football v3).
Usato solo dal layer di ingestion e dall'endpoint di test, mai direttamente dagli endpoint utente.
"""

import logging
from typing import Any

import httpx

from app.core.config import get_api_sports_key

logger = logging.getLogger(__name__)

BASE_URL = "https://v3.football.api-sports.io"


def _get_header(headers: httpx.Headers, *keys: str) -> str | int | None:
    """Restituisce il valore del primo header trovato (case-insensitive)."""
    for key in keys:
        for h, v in headers.items():
            if h.lower() == key.lower():
                try:
                    return int(v) if v.isdigit() else v
                except (ValueError, AttributeError):
                    return v
    return None


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

    async def test_connection(self) -> dict[str, Any]:
        """
        Test connessione leggero: chiama /status (non consuma quota giornaliera).
        Restituisce status HTTP, header di rate limit e breve riepilogo.
        Nessuna ingestion, sicuro per free plan.
        """
        headers_lower: dict[str, str] = {}
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.get(
                    f"{BASE_URL}/status",
                    headers=self._headers(),
                )
                for name, value in r.headers.items():
                    headers_lower[name.lower()] = value

                limit = _get_header(
                    r.headers,
                    "x-ratelimit-limit",
                    "x-ratelimit-limit-minute",
                )
                remaining = _get_header(
                    r.headers,
                    "x-ratelimit-remaining",
                    "x-ratelimit-remaining-minute",
                )
                if limit is None and remaining is None:
                    limit = headers_lower.get("x-ratelimit-limit")
                    remaining = headers_lower.get("x-ratelimit-remaining")

                try:
                    rate_limit_per_minute = int(limit) if limit is not None else None
                except (TypeError, ValueError):
                    rate_limit_per_minute = limit
                try:
                    remaining_requests = int(remaining) if remaining is not None else None
                except (TypeError, ValueError):
                    remaining_requests = remaining

                return {
                    "status_code": r.status_code,
                    "rate_limit_per_minute": rate_limit_per_minute,
                    "remaining_requests": remaining_requests,
                    "ok": 200 <= r.status_code < 300,
                    "headers": dict(headers_lower),
                }
        except httpx.HTTPStatusError as e:
            logger.warning("test_connection HTTP error: %s", e)
            for name, value in e.response.headers.items():
                headers_lower[name.lower()] = value
            limit = _get_header(e.response.headers, "x-ratelimit-limit")
            remaining = _get_header(e.response.headers, "x-ratelimit-remaining")
            return {
                "status_code": e.response.status_code,
                "rate_limit_per_minute": int(limit) if limit is not None else None,
                "remaining_requests": int(remaining) if remaining is not None else None,
                "ok": False,
                "headers": dict(headers_lower),
                "error": str(e),
            }
        except Exception as e:
            logger.exception("test_connection failed: %s", e)
            raise
