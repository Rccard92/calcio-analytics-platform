"""
Attribution Engine — Player Rating FIFA-style per ruolo.

Architettura:
  1. Percentile empirico PER RUOLO (rank-based, winsorized 1-99)
  2. Shrinkage: score = 50 + reliability * (percentile - 50)
     reliability = min(1, minutes / 1200)
  3. Pesi a Tier: A (performance), B (affidabilita'), C (impatto)
  4. Malus disciplina separato (max -10 punti)

Metriche specifiche per ruolo:
  - Goalkeeper: saves, goals_conceded (inv), clean_sheet_rate, penalty_saved
  - Defender: tackles, interceptions, blocks, duels, clean_sheet_involvement
  - Midfielder: key_passes, assists, pass_accuracy, tackles, duels
  - Attacker: goals, shots_on, assists, dribbles

Ogni metrica non disponibile viene saltata e il peso ridistribuito nel Tier.

Output:
  overall_score (0-100), attack_score, creation_score, defense_score,
  impact_score, discipline_malus, reliability_index, breakdown
"""

import logging
from typing import Any

from app.analytics.league_distribution import (
    RoleDistributions,
    _empirical_percentile,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Costanti
# ---------------------------------------------------------------------------

MIN_MINUTES = 300
RELIABILITY_MINUTES = 1200

INVERSE_METRICS = frozenset({"goals_conceded_per_90"})
DIRECT_SCORE_METRICS = frozenset({"captain"})
CAPTAIN_SCORE_YES = 85.0
CAPTAIN_SCORE_NO = 40.0

# ---------------------------------------------------------------------------
# Configurazione pesi per ruolo
# ---------------------------------------------------------------------------

GOALKEEPER_CONFIG: dict[str, Any] = {
    "tier_a": {
        "weight": 70,
        "metrics": {
            "saves_per_90": 25,
            "goals_conceded_per_90": 25,
            "clean_sheet_rate": 15,
            "penalty_saved_rate": 5,
        },
    },
    "tier_b": {
        "weight": 20,
        "metrics": {
            "pass_accuracy": 8,
            "distribution_quality": 5,
            "minutes": 7,
        },
    },
    "tier_c": {
        "weight": 10,
        "metrics": {
            "match_decisive_saves": 10,
        },
    },
    "malus": {"yellow_per_90": -4, "red_per_90": -6},
}

DEFENDER_CONFIG: dict[str, Any] = {
    "tier_a": {
        "weight": 65,
        "metrics": {
            "tackles_per_90": 15,
            "interceptions_per_90": 12,
            "blocks_per_90": 8,
            "duels_won_pct": 15,
            "clean_sheet_rate": 15,
        },
    },
    "tier_b": {
        "weight": 20,
        "metrics": {
            "pass_accuracy": 8,
            "progressive_passes": 5,
            "minutes": 7,
        },
    },
    "tier_c": {
        "weight": 15,
        "metrics": {
            "goals_per_90": 8,
            "match_decisive_actions": 7,
        },
    },
    "malus": {"yellow_per_90": -4, "red_per_90": -6},
}

MIDFIELDER_CONFIG: dict[str, Any] = {
    "tier_a": {
        "weight": 60,
        "metrics": {
            "key_passes_per_90": 14,
            "assists_per_90": 12,
            "pass_accuracy": 12,
            "tackles_per_90": 10,
            "duels_won_pct": 12,
        },
    },
    "tier_b": {
        "weight": 25,
        "metrics": {
            "progressive_actions": 8,
            "ball_recoveries": 8,
            "minutes": 9,
        },
    },
    "tier_c": {
        "weight": 15,
        "metrics": {
            "goals_per_90": 8,
            "match_impact_index": 7,
        },
    },
    "malus": {"yellow_per_90": -4, "red_per_90": -6},
}

ATTACKER_CONFIG: dict[str, Any] = {
    "tier_a": {
        "weight": 70,
        "metrics": {
            "goals_per_90": 25,
            "shots_on_per_90": 12,
            "xG_per_90": 10,
            "assists_per_90": 13,
            "dribbles_success_pct": 10,
        },
    },
    "tier_b": {
        "weight": 20,
        "metrics": {
            "key_passes_per_90": 8,
            "pass_accuracy": 5,
            "minutes": 7,
        },
    },
    "tier_c": {
        "weight": 10,
        "metrics": {
            "match_winning_goals": 6,
            "points_contribution": 4,
        },
    },
    "malus": {"yellow_per_90": -4, "red_per_90": -6},
}

ROLE_CONFIGS: dict[str, dict[str, Any]] = {
    "Goalkeeper": GOALKEEPER_CONFIG,
    "Defender": DEFENDER_CONFIG,
    "Midfielder": MIDFIELDER_CONFIG,
    "Attacker": ATTACKER_CONFIG,
}

# ---------------------------------------------------------------------------
# Raggruppamento metriche per punteggio di categoria
# ---------------------------------------------------------------------------

CATEGORY_METRICS: dict[str, list[str]] = {
    "attack": [
        "goals_per_90", "shots_on_per_90", "shot_accuracy_pct",
        "xG_per_90", "dribbles_success_pct",
    ],
    "creation": [
        "assists_per_90", "key_passes_per_90", "pass_accuracy",
        "progressive_passes", "progressive_actions",
        "distribution_quality",
    ],
    "defense": [
        "tackles_per_90", "interceptions_per_90", "duels_won_pct",
        "blocks_per_90", "saves_per_90", "goals_conceded_per_90",
        "clean_sheet_rate", "ball_recoveries",
    ],
    "impact": [
        "minutes", "rating", "appearances",
        "match_winning_goals", "points_contribution",
        "match_decisive_saves", "match_decisive_actions",
        "match_impact_index", "captain",
    ],
}

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _shrink(percentile: float, minutes: int) -> float:
    """Shrinkage: tira verso 50 i giocatori con pochi minuti."""
    reliability = min(1.0, minutes / RELIABILITY_MINUTES)
    return round(50.0 + reliability * (percentile - 50.0), 1)


def _compute_tier_score(
    tier_metrics: dict[str, int],
    metric_scores: dict[str, float],
) -> float | None:
    """Media pesata nel Tier. None se nessuna metrica disponibile."""
    components = [
        (metric_scores[m], w)
        for m, w in tier_metrics.items()
        if m in metric_scores
    ]
    if not components:
        return None
    total_w = sum(w for _, w in components)
    return sum(s * w for s, w in components) / total_w


# ---------------------------------------------------------------------------
# Scoring engine principale
# ---------------------------------------------------------------------------

def calculate_player_score(
    player_metrics: dict[str, Any],
    role_dists: RoleDistributions,
    position: str | None = None,
) -> dict[str, Any]:
    """
    Calcola score FIFA-style normalizzato per ruolo.

    Args:
        player_metrics: dict con tutte le metriche derivate del giocatore
        role_dists: distribuzioni per ruolo (da build_role_distributions)
        position: ruolo del giocatore (override opzionale)

    Returns:
        dict con overall_score, category scores, discipline_malus,
        reliability_index e breakdown per metrica
    """
    pos = position or player_metrics.get("position") or "Midfielder"
    minutes = player_metrics.get("minutes")

    null_result: dict[str, Any] = {
        "overall_score": None,
        "attack_score": None,
        "creation_score": None,
        "defense_score": None,
        "impact_score": None,
        "discipline_malus": None,
        "reliability_index": None,
        "breakdown": None,
    }

    if minutes is None or minutes < MIN_MINUTES:
        return null_result

    if pos not in ROLE_CONFIGS:
        pos = "Midfielder"

    config = ROLE_CONFIGS[pos]
    dist = role_dists.get(pos, {})
    if not dist:
        return null_result

    reliability = min(1.0, minutes / RELIABILITY_MINUTES)

    # --- 1. Score per metrica ---
    metric_scores: dict[str, float] = {}
    breakdown: dict[str, dict[str, Any]] = {}

    all_tier_metrics: set[str] = set()
    for tier_name in ("tier_a", "tier_b", "tier_c"):
        if tier_name in config:
            all_tier_metrics.update(config[tier_name]["metrics"].keys())

    for metric in all_tier_metrics:
        value = player_metrics.get(metric)
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
        ts = _compute_tier_score(config[tier_name]["metrics"], metric_scores)
        if ts is not None:
            tier_scores[tier_name] = ts
            tier_active_weights[tier_name] = config[tier_name]["weight"]

    if not tier_scores:
        return null_result

    total_tier_weight = sum(tier_active_weights.values())
    base_score = sum(
        tier_scores[t] * tier_active_weights[t]
        for t in tier_scores
    ) / total_tier_weight

    # --- 3. Malus disciplina (separato dai Tier) ---
    malus = 0.0
    malus_config = config.get("malus", {})
    for metric, max_penalty in malus_config.items():
        value = player_metrics.get(metric)
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

    # --- 4. Punteggi per categoria ---
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
        "reliability_index": round(reliability * 100, 1),
        "breakdown": breakdown,
    }
