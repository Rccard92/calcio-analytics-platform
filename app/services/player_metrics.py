"""
Metriche derivate per giocatori.
Calcola statistiche per-90-minuti e percentuali di successo
a partire dai dati grezzi di player_season_stats.
NON salva nel DB — solo calcolo puro per uso futuro in scoring/ranking.
"""

from dataclasses import dataclass
from typing import Any


@dataclass
class DerivedPlayerMetrics:
    """Metriche derivate calcolate dai dati grezzi di una stagione."""
    shots_per_90: float | None = None
    goals_per_90: float | None = None
    assists_per_90: float | None = None
    key_passes_per_90: float | None = None
    duel_win_pct: float | None = None
    dribble_success_pct: float | None = None


def _per_90(value: int | None, minutes: int | None) -> float | None:
    """
    Calcola una metrica normalizzata a 90 minuti.
    Ritorna None se mancano i dati o se i minuti sono 0 (evita division by zero).
    """
    if value is None or minutes is None or minutes == 0:
        return None
    return round((value / minutes) * 90, 2)


def _percentage(numerator: int | None, denominator: int | None) -> float | None:
    """
    Calcola una percentuale (0-100).
    Ritorna None se mancano i dati o se il denominatore è 0.
    """
    if numerator is None or denominator is None or denominator == 0:
        return None
    return round((numerator / denominator) * 100, 1)


def calculate_derived_player_metrics(stats: dict[str, Any]) -> DerivedPlayerMetrics:
    """
    Calcola metriche derivate dai dati grezzi di player_season_stats.

    Parametro `stats` è un dict con le chiavi del modello DB:
        minutes, shots_total, goals, assists, key_passes,
        duels_total, duels_won, dribbles_attempts, dribbles_success

    Gestisce in modo sicuro valori None e minuti a 0 (nessun division by zero).

    Esempio d'uso:
        row = db.query(PlayerSeasonStats).filter(...).first()
        metrics = calculate_derived_player_metrics({
            "minutes": row.minutes,
            "shots_total": row.shots_total,
            "goals": row.goals,
            "assists": row.assists,
            "key_passes": row.key_passes,
            "duels_total": row.duels_total,
            "duels_won": row.duels_won,
            "dribbles_attempts": row.dribbles_attempts,
            "dribbles_success": row.dribbles_success,
        })
    """
    minutes = stats.get("minutes")

    return DerivedPlayerMetrics(
        shots_per_90=_per_90(stats.get("shots_total"), minutes),
        goals_per_90=_per_90(stats.get("goals"), minutes),
        assists_per_90=_per_90(stats.get("assists"), minutes),
        key_passes_per_90=_per_90(stats.get("key_passes"), minutes),
        duel_win_pct=_percentage(stats.get("duels_won"), stats.get("duels_total")),
        dribble_success_pct=_percentage(stats.get("dribbles_success"), stats.get("dribbles_attempts")),
    )
