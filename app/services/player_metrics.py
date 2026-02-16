"""
Player Rating Engine v2 — League-Wide Normalization.

Rating 0-100 calcolato con:
  1. Metriche per-90 e percentuali dal DB
  2. Z-score rispetto alla distribuzione della lega (media e deviazione standard)
  3. Percentile via CDF distribuzione normale (math.erf, zero dipendenze esterne)
  4. Media pesata per posizione su 4 categorie: offensive, playmaking, defensive, discipline

NON salva nel DB. Calcola dinamicamente ad ogni richiesta.
"""

import math
from typing import Any

# ---------------------------------------------------------------------------
# Costanti
# ---------------------------------------------------------------------------

MIN_MINUTES_FOR_PER90 = 300

# Type alias per la distribuzione della lega: metrica → (media, std)
LeagueDistribution = dict[str, tuple[float, float]]

# ---------------------------------------------------------------------------
# Metriche per categoria (usate per scoring e distribuzione)
# ---------------------------------------------------------------------------

OFFENSIVE_METRICS = ["goals_per_90", "shots_on_per_90", "shot_accuracy_pct"]
PLAYMAKING_METRICS = ["assists_per_90", "key_passes_per_90", "dribbles_success_pct", "pass_accuracy"]
DEFENSIVE_METRICS = ["tackles_per_90", "interceptions_per_90", "duels_won_pct"]
DISCIPLINE_METRICS = ["yellow_per_90", "red_per_90", "fouls_per_90"]

ALL_SCORED_METRICS = (
    OFFENSIVE_METRICS + PLAYMAKING_METRICS + DEFENSIVE_METRICS + DISCIPLINE_METRICS + ["rating"]
)

# ---------------------------------------------------------------------------
# Pesi per posizione (4 categorie, facilmente configurabili)
# ---------------------------------------------------------------------------

POSITION_WEIGHTS: dict[str, dict[str, float]] = {
    "Attacker":    {"offensive": 0.40, "playmaking": 0.20, "defensive": 0.10, "discipline": 0.30},
    "Midfielder":  {"offensive": 0.20, "playmaking": 0.30, "defensive": 0.25, "discipline": 0.25},
    "Defender":    {"offensive": 0.10, "playmaking": 0.10, "defensive": 0.50, "discipline": 0.30},
    "Goalkeeper":  {"offensive": 0.05, "playmaking": 0.05, "defensive": 0.60, "discipline": 0.30},
}
DEFAULT_WEIGHTS: dict[str, float] = {
    "offensive": 0.25, "playmaking": 0.20, "defensive": 0.25, "discipline": 0.30,
}


# ---------------------------------------------------------------------------
# Helper matematici
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


def _norm_cdf(z: float) -> float:
    """CDF della distribuzione normale standard. Equivalente a scipy.stats.norm.cdf(z)."""
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def _z_score(value: float | None, mean: float, std: float) -> float | None:
    """Z-score: (valore - media) / deviazione standard. None se valore mancante."""
    if value is None:
        return None
    if std <= 0:
        return 0.0
    return (value - mean) / std


def _percentile_score(
    value: float | None,
    league_dist: LeagueDistribution,
    metric: str,
    inverse: bool = False,
) -> float | None:
    """
    Trasforma un valore in punteggio 0-100 usando z-score league-wide + CDF.
    Se inverse=True (es. cartellini), valori bassi danno punteggio alto.
    """
    if value is None or metric not in league_dist:
        return None
    mean, std = league_dist[metric]
    z = _z_score(value, mean, std)
    if z is None:
        return None
    if inverse:
        z = -z
    return round(_norm_cdf(z) * 100, 1)


def _weighted_avg(components: list[float | None]) -> float | None:
    """Media dei soli componenti non-None."""
    valid = [c for c in components if c is not None]
    if not valid:
        return None
    return round(sum(valid) / len(valid), 1)


# ---------------------------------------------------------------------------
# Metriche derivate
# ---------------------------------------------------------------------------

def calculate_derived_metrics(stats: dict[str, Any]) -> dict[str, Any]:
    """
    Calcola TUTTE le metriche derivate (per-90, percentuali, passthrough)
    dai dati grezzi di un giocatore.

    Restituisce un dict usato sia per la tabella frontend sia per lo scoring.
    Non calcola per-90 se minutes < MIN_MINUTES_FOR_PER90.
    """
    minutes = stats.get("minutes")
    rating = stats.get("rating")

    return {
        # --- Mostrate in tabella ---
        "goals_per_90": _per_90(stats.get("goals"), minutes),
        "assists_per_90": _per_90(stats.get("assists"), minutes),
        "shots_per_90": _per_90(stats.get("shots_total"), minutes),
        "shots_on_per_90": _per_90(stats.get("shots_on"), minutes),
        "shot_accuracy_pct": _percentage(stats.get("shots_on"), stats.get("shots_total")),
        "key_passes_per_90": _per_90(stats.get("key_passes"), minutes),
        "tackles_per_90": _per_90(stats.get("tackles_total"), minutes),
        "interceptions_per_90": _per_90(stats.get("interceptions"), minutes),
        "duels_won_pct": _percentage(stats.get("duels_won"), stats.get("duels_total")),
        "dribbles_success_pct": _percentage(
            stats.get("dribbles_success"), stats.get("dribbles_attempts"),
        ),
        # --- Solo per scoring ---
        "pass_accuracy": stats.get("pass_accuracy"),
        "yellow_per_90": _per_90(stats.get("yellow_cards"), minutes),
        "red_per_90": _per_90(stats.get("red_cards"), minutes),
        "fouls_per_90": _per_90(stats.get("fouls_committed"), minutes),
        "rating": float(rating) if rating is not None and rating > 0 else None,
    }


# ---------------------------------------------------------------------------
# Distribuzione lega
# ---------------------------------------------------------------------------

def build_league_distribution(
    all_player_stats: list[dict[str, Any]],
) -> LeagueDistribution:
    """
    Costruisce la distribuzione della lega da una lista di dati grezzi.
    Ogni dict deve avere le chiavi del modello DB + 'pass_accuracy'.
    Solo giocatori con >= MIN_MINUTES_FOR_PER90 minuti vengono inclusi.

    Ritorna: { metrica: (media, std) } per ogni metrica con almeno 2 valori.
    """
    metric_values: dict[str, list[float]] = {m: [] for m in ALL_SCORED_METRICS}

    for stats in all_player_stats:
        minutes = stats.get("minutes")
        if minutes is None or minutes < MIN_MINUTES_FOR_PER90:
            continue

        derived = calculate_derived_metrics(stats)

        for metric in ALL_SCORED_METRICS:
            val = derived.get(metric)
            if val is not None:
                metric_values[metric].append(val)

    dist: LeagueDistribution = {}
    for metric, values in metric_values.items():
        n = len(values)
        if n >= 2:
            mean = sum(values) / n
            variance = sum((v - mean) ** 2 for v in values) / n
            std = max(variance ** 0.5, 1e-10)
            dist[metric] = (round(mean, 6), round(std, 6))

    return dist


# ---------------------------------------------------------------------------
# Scoring composito
# ---------------------------------------------------------------------------

def calculate_player_score(
    stats: dict[str, Any],
    league_dist: LeagueDistribution,
) -> dict[str, float | None]:
    """
    Calcola punteggio 0-100 normalizzato rispetto alla lega.

    Flusso per ogni metrica:
      1. Calcola metrica derivata (per-90 o percentuale)
      2. z = (valore - media_lega) / std_lega
      3. percentile = CDF_normale(z) * 100
      4. Media pesata per categoria e posizione

    Ritorna:
        overall_score, offensive_score, defensive_score,
        playmaking_score, discipline_score  (tutti float | None)

    None se minutes < 300 o distribuzione lega vuota.
    """
    minutes = stats.get("minutes")
    position = stats.get("position") or ""

    null_result: dict[str, float | None] = {
        "overall_score": None,
        "offensive_score": None,
        "defensive_score": None,
        "playmaking_score": None,
        "discipline_score": None,
    }

    if minutes is None or minutes < MIN_MINUTES_FOR_PER90:
        return null_result

    if not league_dist:
        return null_result

    derived = calculate_derived_metrics(stats)

    # --- Offensive ---
    off_components = [
        _percentile_score(derived.get(m), league_dist, m)
        for m in OFFENSIVE_METRICS
    ]
    if derived.get("rating") is not None and "rating" in league_dist:
        off_components.append(_percentile_score(derived["rating"], league_dist, "rating"))
    offensive_score = _weighted_avg(off_components)

    # --- Playmaking ---
    play_components = [
        _percentile_score(derived.get(m), league_dist, m)
        for m in PLAYMAKING_METRICS
    ]
    playmaking_score = _weighted_avg(play_components)

    # --- Defensive ---
    def_components = [
        _percentile_score(derived.get(m), league_dist, m)
        for m in DEFENSIVE_METRICS
    ]
    defensive_score = _weighted_avg(def_components)

    # --- Discipline (inversamente proporzionale: meno cartellini = punteggio alto) ---
    disc_components = [
        _percentile_score(derived.get(m), league_dist, m, inverse=True)
        for m in DISCIPLINE_METRICS
    ]
    discipline_score = _weighted_avg(disc_components)

    # --- Overall (media pesata per posizione) ---
    weights = POSITION_WEIGHTS.get(position, DEFAULT_WEIGHTS)

    parts: list[tuple[float, float]] = []
    for sub_name, sub_score in [
        ("offensive", offensive_score),
        ("playmaking", playmaking_score),
        ("defensive", defensive_score),
        ("discipline", discipline_score),
    ]:
        if sub_score is not None:
            parts.append((sub_score, weights[sub_name]))

    if parts:
        total_w = sum(w for _, w in parts)
        overall_score = (
            round(sum(v * w for v, w in parts) / total_w, 1)
            if total_w > 0
            else None
        )
    else:
        overall_score = None

    return {
        "overall_score": overall_score,
        "offensive_score": offensive_score,
        "defensive_score": defensive_score,
        "playmaking_score": playmaking_score,
        "discipline_score": discipline_score,
    }
