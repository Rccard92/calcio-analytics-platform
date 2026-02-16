"""
Metriche derivate e scoring composito per giocatori.
Calcola statistiche per-90-minuti, percentuali di successo e punteggi pesati per ruolo.
NON salva nel DB — calcolo puro nel service layer per ogni richiesta.
"""

from typing import Any

# Soglia minima di minuti per calcolare metriche per-90 affidabili
MIN_MINUTES_FOR_PER90 = 300

# ---------------------------------------------------------------------------
# Pesi per posizione (facilmente configurabili)
# ---------------------------------------------------------------------------

POSITION_WEIGHTS: dict[str, dict[str, float]] = {
    "Attacker":   {"offensive": 0.55, "defensive": 0.15, "discipline": 0.30},
    "Midfielder": {"offensive": 0.35, "defensive": 0.35, "discipline": 0.30},
    "Defender":   {"offensive": 0.15, "defensive": 0.55, "discipline": 0.30},
    "Goalkeeper":  {"offensive": 0.05, "defensive": 0.65, "discipline": 0.30},
}
DEFAULT_WEIGHTS: dict[str, float] = {
    "offensive": 0.35, "defensive": 0.35, "discipline": 0.30,
}

# ---------------------------------------------------------------------------
# Benchmark per normalizzazione (valore = "100 punti" su scala 0-100)
# ---------------------------------------------------------------------------

OFFENSIVE_BENCHMARKS: dict[str, float] = {
    "goals_per_90": 0.80,
    "assists_per_90": 0.50,
    "shots_on_per_90": 2.0,
    "shot_accuracy_pct": 55.0,
    "key_passes_per_90": 2.5,
    "dribbles_success_pct": 65.0,
}

DEFENSIVE_BENCHMARKS: dict[str, float] = {
    "tackles_per_90": 3.0,
    "interceptions_per_90": 2.0,
    "duels_won_pct": 65.0,
}

DISCIPLINE_BENCHMARKS: dict[str, float] = {
    "yellow_per_90": 0.40,
    "red_per_90": 0.10,
    "fouls_per_90": 1.5,
}


# ---------------------------------------------------------------------------
# Funzioni helper
# ---------------------------------------------------------------------------

def _per_90(value: int | float | None, minutes: int | None) -> float | None:
    """Metrica normalizzata a 90 min. None se dati mancanti o minuti < soglia."""
    if value is None or minutes is None or minutes < MIN_MINUTES_FOR_PER90:
        return None
    return round((value / minutes) * 90, 2)


def _percentage(
    numerator: int | float | None,
    denominator: int | float | None,
) -> float | None:
    """Percentuale 0-100. None se dati mancanti o denominatore 0."""
    if numerator is None or denominator is None or denominator == 0:
        return None
    return round((numerator / denominator) * 100, 1)


def _normalize(
    value: float | None,
    benchmark: float,
    inverse: bool = False,
) -> float | None:
    """
    Normalizza un valore nella scala 0-100 rispetto a un benchmark.
    Se inverse=True, valori bassi danno punteggio alto (es. cartellini).
    """
    if value is None:
        return None
    if inverse:
        if benchmark <= 0:
            return 100.0
        return round(max(0.0, 100.0 - (value / benchmark) * 100.0), 1)
    if benchmark <= 0:
        return 0.0
    return round(min(100.0, (value / benchmark) * 100.0), 1)


def _weighted_avg(components: list[float | None]) -> float | None:
    """Media dei soli componenti non-None."""
    valid = [c for c in components if c is not None]
    if not valid:
        return None
    return round(sum(valid) / len(valid), 1)


# ---------------------------------------------------------------------------
# API pubblica
# ---------------------------------------------------------------------------

def calculate_derived_metrics(stats: dict[str, Any]) -> dict[str, Any]:
    """
    Calcola metriche derivate (per-90, percentuali) dai dati grezzi.
    Non calcola per-90 se minutes < MIN_MINUTES_FOR_PER90 (300).

    Parametro: dict con chiavi del modello DB (minutes, goals, assists, ...).
    Ritorna: dict con goals_per_90, assists_per_90, shot_accuracy_pct, ecc.
    """
    minutes = stats.get("minutes")

    return {
        "goals_per_90": _per_90(stats.get("goals"), minutes),
        "assists_per_90": _per_90(stats.get("assists"), minutes),
        "shots_per_90": _per_90(stats.get("shots_total"), minutes),
        "shots_on_per_90": _per_90(stats.get("shots_on"), minutes),
        "shot_accuracy_pct": _percentage(
            stats.get("shots_on"), stats.get("shots_total"),
        ),
        "key_passes_per_90": _per_90(stats.get("key_passes"), minutes),
        "tackles_per_90": _per_90(stats.get("tackles_total"), minutes),
        "interceptions_per_90": _per_90(stats.get("interceptions"), minutes),
        "duels_won_pct": _percentage(
            stats.get("duels_won"), stats.get("duels_total"),
        ),
        "dribbles_success_pct": _percentage(
            stats.get("dribbles_success"), stats.get("dribbles_attempts"),
        ),
    }


def calculate_player_score(stats: dict[str, Any]) -> dict[str, float | None]:
    """
    Calcola punteggio composito (0-100) pesato per posizione.

    Ritorna:
        {
            "overall_score":    float | None,
            "offensive_score":  float | None,
            "defensive_score":  float | None,
            "discipline_score": float | None,
        }

    Tutti None se minutes < 300 (campione insufficiente).
    I valori null nei dati grezzi vengono ignorati (non penalizzano).
    """
    minutes = stats.get("minutes")
    position = stats.get("position") or ""

    null_result: dict[str, float | None] = {
        "overall_score": None,
        "offensive_score": None,
        "defensive_score": None,
        "discipline_score": None,
    }

    if minutes is None or minutes < MIN_MINUTES_FOR_PER90:
        return null_result

    derived = calculate_derived_metrics(stats)

    # --- Offensive score ---
    off_components: list[float | None] = [
        _normalize(derived["goals_per_90"], OFFENSIVE_BENCHMARKS["goals_per_90"]),
        _normalize(derived["assists_per_90"], OFFENSIVE_BENCHMARKS["assists_per_90"]),
        _normalize(derived["shots_on_per_90"], OFFENSIVE_BENCHMARKS["shots_on_per_90"]),
        _normalize(derived["shot_accuracy_pct"], OFFENSIVE_BENCHMARKS["shot_accuracy_pct"]),
        _normalize(derived["key_passes_per_90"], OFFENSIVE_BENCHMARKS["key_passes_per_90"]),
        _normalize(
            derived["dribbles_success_pct"],
            OFFENSIVE_BENCHMARKS["dribbles_success_pct"],
        ),
    ]
    rating = stats.get("rating")
    if rating is not None and rating > 0:
        rating_norm = min(100.0, max(0.0, (rating - 5.5) / (8.0 - 5.5) * 100.0))
        off_components.append(round(rating_norm, 1))

    offensive_score = _weighted_avg(off_components)

    # --- Defensive score ---
    def_components: list[float | None] = [
        _normalize(derived["tackles_per_90"], DEFENSIVE_BENCHMARKS["tackles_per_90"]),
        _normalize(
            derived["interceptions_per_90"],
            DEFENSIVE_BENCHMARKS["interceptions_per_90"],
        ),
        _normalize(derived["duels_won_pct"], DEFENSIVE_BENCHMARKS["duels_won_pct"]),
    ]
    defensive_score = _weighted_avg(def_components)

    # --- Discipline score (inversamente proporzionale) ---
    yellow_per_90 = _per_90(stats.get("yellow_cards"), minutes)
    red_per_90 = _per_90(stats.get("red_cards"), minutes)
    fouls_per_90 = _per_90(stats.get("fouls_committed"), minutes)

    disc_components: list[float | None] = [
        _normalize(yellow_per_90, DISCIPLINE_BENCHMARKS["yellow_per_90"], inverse=True),
        _normalize(red_per_90, DISCIPLINE_BENCHMARKS["red_per_90"], inverse=True),
        _normalize(fouls_per_90, DISCIPLINE_BENCHMARKS["fouls_per_90"], inverse=True),
    ]
    discipline_score = _weighted_avg(disc_components)

    # --- Overall score (pesato per posizione) ---
    weights = POSITION_WEIGHTS.get(position, DEFAULT_WEIGHTS)

    parts: list[tuple[float, float]] = []
    if offensive_score is not None:
        parts.append((offensive_score, weights["offensive"]))
    if defensive_score is not None:
        parts.append((defensive_score, weights["defensive"]))
    if discipline_score is not None:
        parts.append((discipline_score, weights["discipline"]))

    if parts:
        total_weight = sum(w for _, w in parts)
        overall_score = (
            round(sum(v * w for v, w in parts) / total_weight, 1)
            if total_weight > 0
            else None
        )
    else:
        overall_score = None

    return {
        "overall_score": overall_score,
        "offensive_score": offensive_score,
        "defensive_score": defensive_score,
        "discipline_score": discipline_score,
    }
