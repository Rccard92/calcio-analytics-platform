"""
Player Rating Engine v3 — FIFA-style, per-role, tier-based.

Architettura:
  1. Metriche per-90 e percentuali dai dati grezzi
  2. Distribuzione empirica PER RUOLO (rank-based percentile, no CDF gaussiana)
  3. Shrinkage affidabilita': score = 50 + reliability * (percentile - 50)
     con reliability = min(1, minutes / 1200)
  4. Combinazione con pesi a Tier (A=performance, B=affidabilita', C=impatto)
  5. Malus disciplina separato (yellow/red per 90)

Output per giocatore:
  overall_score (0-100), attack_score, creation_score, defense_score,
  impact_score, discipline_malus, breakdown per metrica

NON salva nel DB. Calcola dinamicamente ad ogni richiesta.
"""

import bisect
import logging
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Costanti
# ---------------------------------------------------------------------------

MIN_MINUTES = 300
RELIABILITY_MINUTES = 1200

# Metriche inverse: percentile basso = buono
INVERSE_METRICS = frozenset({"goals_conceded_per_90"})

# Metriche con score diretto (non passano per percentile empirico)
DIRECT_SCORE_METRICS = frozenset({"captain"})
CAPTAIN_SCORE_YES = 85.0
CAPTAIN_SCORE_NO = 40.0

# Type alias
RoleDistributions = dict[str, dict[str, list[float]]]

# ---------------------------------------------------------------------------
# Configurazione pesi per ruolo — Tier A/B/C + Malus
#
# I pesi interni di ogni Tier sono punti assoluti.
# TierA(70) + TierB(20) + TierC(10) = 100 punti base.
# Malus disciplina sottrae fino a -10 punti.
# ---------------------------------------------------------------------------

ATTACKER_CONFIG: dict[str, Any] = {
    "tier_a": {
        "weight": 70,
        "metrics": {
            "goals_per_90": 25,
            "shots_on_per_90": 8,
            "shot_accuracy_pct": 7,
            "assists_per_90": 10,
            "key_passes_per_90": 7,
            "dribbles_success_pct": 5,
            "pass_accuracy": 8,
        },
    },
    "tier_b": {
        "weight": 20,
        "metrics": {"minutes": 8, "rating": 8, "appearances": 4},
    },
    "tier_c": {
        "weight": 10,
        "metrics": {
            "match_winning_goals": 6,
            "captain": 2,
            "points_contribution": 2,
        },
    },
    "malus": {"yellow_per_90": -4, "red_per_90": -6},
}

MIDFIELDER_CONFIG: dict[str, Any] = {
    "tier_a": {
        "weight": 70,
        "metrics": {
            "goals_per_90": 8,
            "assists_per_90": 12,
            "key_passes_per_90": 15,
            "pass_accuracy": 12,
            "dribbles_success_pct": 8,
            "duels_won_pct": 8,
            "tackles_per_90": 4,
            "interceptions_per_90": 3,
        },
    },
    "tier_b": {
        "weight": 20,
        "metrics": {"minutes": 8, "rating": 8, "appearances": 4},
    },
    "tier_c": {
        "weight": 10,
        "metrics": {
            "match_winning_goals": 4,
            "captain": 3,
            "points_contribution": 3,
        },
    },
    "malus": {"yellow_per_90": -4, "red_per_90": -6},
}

DEFENDER_CONFIG: dict[str, Any] = {
    "tier_a": {
        "weight": 70,
        "metrics": {
            "tackles_per_90": 15,
            "interceptions_per_90": 12,
            "blocks_per_90": 8,
            "duels_won_pct": 15,
            "pass_accuracy": 10,
            "goals_per_90": 4,
            "assists_per_90": 3,
            "key_passes_per_90": 3,
        },
    },
    "tier_b": {
        "weight": 20,
        "metrics": {"minutes": 8, "rating": 8, "appearances": 4},
    },
    "tier_c": {
        "weight": 10,
        "metrics": {
            "match_winning_goals": 2,
            "captain": 4,
            "points_contribution": 4,
        },
    },
    "malus": {"yellow_per_90": -4, "red_per_90": -6},
}

GOALKEEPER_CONFIG: dict[str, Any] = {
    "tier_a": {
        "weight": 70,
        "metrics": {
            "saves_per_90": 20,
            "goals_conceded_per_90": 20,
            "pass_accuracy": 15,
            "duels_won_pct": 15,
        },
    },
    "tier_b": {
        "weight": 20,
        "metrics": {"minutes": 8, "rating": 8, "appearances": 4},
    },
    "tier_c": {
        "weight": 10,
        "metrics": {"captain": 4, "points_contribution": 6},
    },
    "malus": {"yellow_per_90": -4, "red_per_90": -6},
}

ROLE_CONFIGS: dict[str, dict[str, Any]] = {
    "Attacker": ATTACKER_CONFIG,
    "Midfielder": MIDFIELDER_CONFIG,
    "Defender": DEFENDER_CONFIG,
    "Goalkeeper": GOALKEEPER_CONFIG,
}

# ---------------------------------------------------------------------------
# Raggruppamento metriche per punteggio di categoria (breakdown)
# ---------------------------------------------------------------------------

CATEGORY_METRICS: dict[str, list[str]] = {
    "attack": ["goals_per_90", "shots_on_per_90", "shot_accuracy_pct"],
    "creation": [
        "assists_per_90", "key_passes_per_90",
        "dribbles_success_pct", "pass_accuracy",
    ],
    "defense": [
        "tackles_per_90", "interceptions_per_90", "duels_won_pct",
        "blocks_per_90", "saves_per_90", "goals_conceded_per_90",
    ],
    "impact": [
        "minutes", "rating", "appearances", "captain",
        "match_winning_goals", "points_contribution",
    ],
}


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _per_90(value: int | float | None, minutes: int | None) -> float | None:
    """Metrica normalizzata a 90 min. None se dati mancanti o minuti < soglia."""
    if value is None or minutes is None or minutes < MIN_MINUTES:
        return None
    return round((value / minutes) * 90, 3)


def _pct(numerator: int | float | None, denominator: int | float | None) -> float | None:
    """Percentuale 0-100. None se dati mancanti o denominatore 0."""
    if numerator is None or denominator is None or denominator == 0:
        return None
    return round((numerator / denominator) * 100, 1)


def _empirical_percentile(value: float, sorted_values: list[float]) -> float:
    """
    Percentile empirico rank-based con midrank per i pareggi.
    Restituisce 0-100. Se lista vuota restituisce 50 (neutrale).
    """
    n = len(sorted_values)
    if n == 0:
        return 50.0
    below = bisect.bisect_left(sorted_values, value)
    above = bisect.bisect_right(sorted_values, value)
    equal = above - below
    return round((below + 0.5 * equal) / n * 100, 1)


def _shrink(percentile: float, minutes: int) -> float:
    """Shrinkage: tira verso 50 i giocatori con pochi minuti."""
    reliability = min(1.0, minutes / RELIABILITY_MINUTES)
    return round(50.0 + reliability * (percentile - 50.0), 1)


# ---------------------------------------------------------------------------
# Metriche derivate
# ---------------------------------------------------------------------------

def calculate_all_metrics(stats: dict[str, Any]) -> dict[str, Any]:
    """
    Calcola TUTTE le metriche derivate dai dati grezzi di un giocatore.
    Usato sia per il display tabella sia per lo scoring engine.
    """
    minutes = stats.get("minutes")
    rating = stats.get("rating")

    return {
        # --- Per-90 ---
        "goals_per_90": _per_90(stats.get("goals"), minutes),
        "assists_per_90": _per_90(stats.get("assists"), minutes),
        "shots_per_90": _per_90(stats.get("shots_total"), minutes),
        "shots_on_per_90": _per_90(stats.get("shots_on"), minutes),
        "key_passes_per_90": _per_90(stats.get("key_passes"), minutes),
        "tackles_per_90": _per_90(stats.get("tackles_total"), minutes),
        "interceptions_per_90": _per_90(stats.get("interceptions"), minutes),
        "blocks_per_90": _per_90(stats.get("blocks"), minutes),
        "saves_per_90": _per_90(stats.get("saves"), minutes),
        "goals_conceded_per_90": _per_90(stats.get("goals_conceded"), minutes),
        "yellow_per_90": _per_90(stats.get("yellow_cards"), minutes),
        "red_per_90": _per_90(stats.get("red_cards"), minutes),
        # --- Percentuali ---
        "shot_accuracy_pct": _pct(stats.get("shots_on"), stats.get("shots_total")),
        "pass_accuracy": stats.get("pass_accuracy"),
        "duels_won_pct": _pct(stats.get("duels_won"), stats.get("duels_total")),
        "dribbles_success_pct": _pct(
            stats.get("dribbles_success"), stats.get("dribbles_attempts"),
        ),
        # --- Raw (usati in TierB) ---
        "minutes": minutes if minutes is not None and minutes >= MIN_MINUTES else None,
        "appearances": stats.get("appearances"),
        "rating": float(rating) if rating is not None and rating > 0 else None,
        # --- Score diretto ---
        "captain": CAPTAIN_SCORE_YES if stats.get("captain") else CAPTAIN_SCORE_NO,
        # --- Stubs (TierC) - da implementare con events ingestion ---
        "match_winning_goals": None,
        "points_contribution": None,
    }


# alias per retrocompatibilita' con player_service (display per tabella)
calculate_derived_metrics = calculate_all_metrics


# ---------------------------------------------------------------------------
# Distribuzioni per ruolo
# ---------------------------------------------------------------------------

def build_role_distributions(
    all_player_stats: list[dict[str, Any]],
) -> RoleDistributions:
    """
    Raggruppa giocatori per ruolo, calcola metriche derivate,
    raccoglie valori ordinati per ogni metrica.
    Solo giocatori con >= MIN_MINUTES minuti.

    Returns: { role: { metric: [sorted_values] } }
    """
    role_players: dict[str, list[dict[str, Any]]] = {
        "Goalkeeper": [], "Defender": [], "Midfielder": [], "Attacker": [],
    }

    for stats in all_player_stats:
        minutes = stats.get("minutes")
        if minutes is None or minutes < MIN_MINUTES:
            continue
        role = stats.get("position") or "Midfielder"
        if role not in role_players:
            role = "Midfielder"
        derived = calculate_all_metrics(stats)
        role_players[role].append(derived)

    distributions: RoleDistributions = {}
    for role, players in role_players.items():
        metrics_dist: dict[str, list[float]] = {}
        all_keys: set[str] = set()
        for p in players:
            all_keys.update(p.keys())

        for metric in all_keys:
            if metric in DIRECT_SCORE_METRICS:
                continue
            values = [p[metric] for p in players if p.get(metric) is not None]
            if values:
                metrics_dist[metric] = sorted(values)

        distributions[role] = metrics_dist

    for role, dist in distributions.items():
        n_players = len(role_players[role])
        n_metrics = len(dist)
        logger.info(
            "Distribuzione ruolo %s: %d giocatori qualificati, %d metriche",
            role, n_players, n_metrics,
        )

    return distributions


# ---------------------------------------------------------------------------
# Scoring engine
# ---------------------------------------------------------------------------

def _compute_tier_score(
    tier_metrics: dict[str, int],
    metric_scores: dict[str, float],
) -> float | None:
    """
    Media pesata all'interno di un Tier.
    Se una metrica e' None, il suo peso viene redistribuito alle altre.
    Restituisce None se nessuna metrica e' disponibile.
    """
    components: list[tuple[float, int]] = []
    for metric, weight in tier_metrics.items():
        score = metric_scores.get(metric)
        if score is not None:
            components.append((score, weight))

    if not components:
        return None

    total_w = sum(w for _, w in components)
    return sum(s * w for s, w in components) / total_w


def calculate_player_score(
    stats: dict[str, Any],
    role_dists: RoleDistributions,
) -> dict[str, Any]:
    """
    Calcola score FIFA-style normalizzato per ruolo con Tier A/B/C + malus.

    Flusso:
      1. Calcola tutte le metriche derivate
      2. Per ogni metrica nei Tier: percentile empirico nella distribuzione del ruolo
      3. Applica shrinkage per affidabilita'
      4. Combina Tier con pesi globali (A=70%, B=20%, C=10%)
      5. Sottrai malus disciplina

    Returns: dict con overall_score, category scores, discipline_malus, breakdown
    """
    minutes = stats.get("minutes")
    position = stats.get("position") or "Midfielder"

    null_result: dict[str, Any] = {
        "overall_score": None,
        "attack_score": None,
        "creation_score": None,
        "defense_score": None,
        "impact_score": None,
        "discipline_malus": None,
        "breakdown": None,
    }

    if minutes is None or minutes < MIN_MINUTES:
        return null_result

    if position not in ROLE_CONFIGS:
        position = "Midfielder"

    config = ROLE_CONFIGS[position]
    dist = role_dists.get(position, {})

    if not dist:
        return null_result

    derived = calculate_all_metrics(stats)
    reliability = min(1.0, minutes / RELIABILITY_MINUTES)

    # --- 1. Calcola score per ogni metrica ---
    metric_scores: dict[str, float] = {}
    raw_percentiles: dict[str, float] = {}
    breakdown: dict[str, dict[str, Any]] = {}

    all_tier_metrics: set[str] = set()
    for tier_name in ("tier_a", "tier_b", "tier_c"):
        if tier_name in config:
            all_tier_metrics.update(config[tier_name]["metrics"].keys())

    for metric in all_tier_metrics:
        value = derived.get(metric)
        if value is None:
            continue

        if metric in DIRECT_SCORE_METRICS:
            pct = value
            score = _shrink(value, minutes)
        else:
            sorted_vals = dist.get(metric, [])
            if not sorted_vals:
                continue
            pct = _empirical_percentile(value, sorted_vals)
            if metric in INVERSE_METRICS:
                pct = round(100.0 - pct, 1)
            score = _shrink(pct, minutes)

        metric_scores[metric] = score
        raw_percentiles[metric] = pct

        tier_for_metric = ""
        weight_for_metric = 0
        for tn in ("tier_a", "tier_b", "tier_c"):
            if tn in config and metric in config[tn]["metrics"]:
                tier_for_metric = tn
                weight_for_metric = config[tn]["metrics"][metric]
                break

        breakdown[metric] = {
            "value": round(value, 3) if isinstance(value, float) else value,
            "percentile": round(pct, 1),
            "score": round(score, 1),
            "weight": weight_for_metric,
            "tier": tier_for_metric,
        }

    # --- 2. Combina Tier ---
    tier_scores: dict[str, float] = {}
    tier_active_weights: dict[str, int] = {}

    for tier_name in ("tier_a", "tier_b", "tier_c"):
        if tier_name not in config:
            continue
        tier_info = config[tier_name]
        ts = _compute_tier_score(tier_info["metrics"], metric_scores)
        if ts is not None:
            tier_scores[tier_name] = ts
            tier_active_weights[tier_name] = tier_info["weight"]

    if not tier_scores:
        return null_result

    total_tier_weight = sum(tier_active_weights.values())
    base_score = sum(
        tier_scores[t] * tier_active_weights[t]
        for t in tier_scores
    ) / total_tier_weight

    # --- 3. Malus disciplina ---
    malus = 0.0
    malus_config = config.get("malus", {})
    for metric, max_penalty in malus_config.items():
        value = derived.get(metric)
        if value is None:
            continue
        sorted_vals = dist.get(metric, [])
        if not sorted_vals:
            continue
        pct = _empirical_percentile(value, sorted_vals)
        contribution = max_penalty * (pct / 100) * reliability
        malus += contribution

        breakdown[metric] = {
            "value": round(value, 3) if isinstance(value, float) else value,
            "percentile": round(pct, 1),
            "malus_contribution": round(contribution, 2),
            "max_penalty": max_penalty,
            "tier": "malus",
        }

    discipline_malus = round(max(-10.0, malus), 1)
    overall_score = round(max(0.0, min(100.0, base_score + discipline_malus)), 1)

    # --- 4. Punteggi per categoria (media dei metric_scores nella categoria) ---
    category_scores: dict[str, float | None] = {}
    for cat_name, cat_metrics in CATEGORY_METRICS.items():
        scores_in_cat = [metric_scores[m] for m in cat_metrics if m in metric_scores]
        if scores_in_cat:
            category_scores[cat_name] = round(sum(scores_in_cat) / len(scores_in_cat), 1)
        else:
            category_scores[cat_name] = None

    return {
        "overall_score": overall_score,
        "attack_score": category_scores.get("attack"),
        "creation_score": category_scores.get("creation"),
        "defense_score": category_scores.get("defense"),
        "impact_score": category_scores.get("impact"),
        "discipline_malus": discipline_malus,
        "breakdown": breakdown,
    }
