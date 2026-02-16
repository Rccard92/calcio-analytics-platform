"""
Distribuzione metriche per ruolo — baseline per il percentile empirico.

Flusso:
  1. Carica tutti i player_season_stats con >= 300 minuti
  2. Normalizza posizioni API in ruoli (Goalkeeper/Defender/Midfielder/Attacker)
  3. Calcola metriche derivate per ogni giocatore
  4. Winsorize al 1 e 99 percentile per tagliare outlier
  5. Salva valori ordinati per ogni metrica per lookup percentile O(log n)

Tutto in-memory, niente DB. Cache opzionale in futuro.
"""

import bisect
import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Costanti
# ---------------------------------------------------------------------------

MIN_MINUTES = 300

# Type alias: { role: { metric: sorted_values } }
RoleDistributions = dict[str, dict[str, list[float]]]

# ---------------------------------------------------------------------------
# Normalizzazione posizioni API-Football -> ruolo
# ---------------------------------------------------------------------------

_POSITION_MAP: dict[str, str] = {
    "goalkeeper": "Goalkeeper",
    "defender": "Defender",
    "centre-back": "Defender",
    "center-back": "Defender",
    "right-back": "Defender",
    "left-back": "Defender",
    "midfielder": "Midfielder",
    "defensive midfield": "Midfielder",
    "central midfield": "Midfielder",
    "attacking midfield": "Midfielder",
    "right midfield": "Midfielder",
    "left midfield": "Midfielder",
    "attacker": "Attacker",
    "forward": "Attacker",
    "striker": "Attacker",
    "centre-forward": "Attacker",
    "right winger": "Attacker",
    "left winger": "Attacker",
    "second striker": "Attacker",
    "winger": "Attacker",
}

VALID_ROLES = frozenset({"Goalkeeper", "Defender", "Midfielder", "Attacker"})


def normalize_position(raw: str | None) -> str:
    """
    Mappa posizioni dettagliate API-Football in uno dei 4 ruoli canonici.
    Fallback: Midfielder se non riconosciuto.
    """
    if not raw:
        return "Midfielder"
    key = raw.strip().lower()
    mapped = _POSITION_MAP.get(key)
    if mapped:
        return mapped
    if raw in VALID_ROLES:
        return raw
    return "Midfielder"


# ---------------------------------------------------------------------------
# Metriche distribuite
# ---------------------------------------------------------------------------

DISTRIBUTABLE_METRICS = frozenset({
    "goals_per_90", "assists_per_90", "shots_on_per_90",
    "key_passes_per_90", "tackles_per_90", "interceptions_per_90",
    "blocks_per_90", "saves_per_90", "goals_conceded_per_90",
    "yellow_per_90", "red_per_90",
    "shot_accuracy_pct", "pass_accuracy", "duels_won_pct",
    "dribbles_success_pct",
    "minutes", "appearances", "rating",
    "clean_sheet_rate", "penalty_saved_rate",
    "save_pct", "goals_conceded_adjusted",
})


# ---------------------------------------------------------------------------
# Query SQL
# ---------------------------------------------------------------------------

SEASON_STATS_SQL = text("""
SELECT
  COALESCE(p.position, '') AS position,
  s.appearances,
  s.minutes,
  s.goals,
  s.assists,
  s.shots_total,
  s.shots_on,
  s.passes_accuracy,
  s.rating,
  s.yellow_cards,
  s.red_cards,
  s.tackles_total,
  s.interceptions,
  s.duels_total,
  s.duels_won,
  s.dribbles_attempts,
  s.dribbles_success,
  s.key_passes,
  s.fouls_committed,
  s.captain,
  s.blocks,
  s.saves,
  s.goals_conceded,
  s.penalty_saved,
  p.api_player_id
FROM player_season_stats s
INNER JOIN players p ON p.id = s.player_id
WHERE s.season = :season
  AND s.minutes IS NOT NULL
  AND s.minutes >= :min_minutes
""")


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _per_90(value: int | float | None, minutes: int | None) -> float | None:
    if value is None or minutes is None or minutes < MIN_MINUTES:
        return None
    return round((value / minutes) * 90, 3)


def _pct(num: int | float | None, denom: int | float | None) -> float | None:
    if num is None or denom is None or denom == 0:
        return None
    return round((num / denom) * 100, 1)


def _nullable_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v > 0 else None
    except (ValueError, TypeError):
        return None


def _winsorize(values: list[float], lower_pct: float = 1.0, upper_pct: float = 99.0) -> list[float]:
    """Taglia valori sotto 1° e sopra 99° percentile."""
    n = len(values)
    if n < 10:
        return values
    s = sorted(values)
    lo = s[max(0, int(n * lower_pct / 100))]
    hi = s[min(n - 1, int(n * upper_pct / 100))]
    return [max(lo, min(hi, v)) for v in values]


def _empirical_percentile(value: float, sorted_values: list[float]) -> float:
    """Percentile empirico rank-based con midrank. 0-100."""
    n = len(sorted_values)
    if n == 0:
        return 50.0
    below = bisect.bisect_left(sorted_values, value)
    above = bisect.bisect_right(sorted_values, value)
    equal = above - below
    return round((below + 0.5 * equal) / n * 100, 1)


# ---------------------------------------------------------------------------
# Metriche derivate per un giocatore
# ---------------------------------------------------------------------------

def compute_player_metrics(
    stats: dict[str, Any],
    clean_sheet_data: dict[int, tuple[int, int]] | None = None,
) -> dict[str, Any]:
    """
    Calcola tutte le metriche derivate per un giocatore.
    clean_sheet_data: { api_player_id: (clean_sheets, matches_played) }
    """
    minutes = stats.get("minutes")
    rating = stats.get("rating")
    api_pid = stats.get("api_player_id")

    cs_data = (clean_sheet_data or {}).get(api_pid, (None, None))
    cs_count, cs_matches = cs_data
    clean_sheet_rate = None
    if cs_count is not None and cs_matches and cs_matches > 0:
        clean_sheet_rate = round((cs_count / cs_matches) * 100, 1)

    penalty_saved = stats.get("penalty_saved")
    appearances = stats.get("appearances")
    penalty_saved_rate = None
    if penalty_saved is not None and appearances and appearances > 0:
        penalty_saved_rate = round((penalty_saved / appearances) * 100, 1)

    saves = stats.get("saves")
    goals_conceded = stats.get("goals_conceded")
    shots_faced = (saves or 0) + (goals_conceded or 0)
    save_pct = None
    goals_conceded_adjusted = None
    if shots_faced > 0:
        save_pct = round((saves or 0) / shots_faced * 100, 1)
        goals_conceded_adjusted = round((goals_conceded or 0) / shots_faced * 100, 1)

    return {
        # Per-90
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
        # Percentuali
        "shot_accuracy_pct": _pct(stats.get("shots_on"), stats.get("shots_total")),
        "pass_accuracy": stats.get("pass_accuracy"),
        "duels_won_pct": _pct(stats.get("duels_won"), stats.get("duels_total")),
        "dribbles_success_pct": _pct(
            stats.get("dribbles_success"), stats.get("dribbles_attempts"),
        ),
        # Raw
        "minutes": minutes if minutes is not None and minutes >= MIN_MINUTES else None,
        "appearances": stats.get("appearances"),
        "rating": _nullable_float(rating),
        # GK-specific
        "save_pct": save_pct,
        "goals_conceded_adjusted": goals_conceded_adjusted,
        # Impact (da lineups/fixtures)
        "clean_sheet_rate": clean_sheet_rate,
        "penalty_saved_rate": penalty_saved_rate,
        # Stubs
        "match_winning_goals": None,
        "points_contribution": None,
        "match_decisive_saves": None,
        "match_decisive_actions": None,
        "match_impact_index": None,
        "progressive_passes": None,
        "progressive_actions": None,
        "distribution_quality": None,
        "ball_recoveries": None,
        "xG_per_90": None,
    }


# ---------------------------------------------------------------------------
# Dati clean sheet da DB
# ---------------------------------------------------------------------------

CLEAN_SHEET_SQL = text("""
SELECT
  fl.api_player_id,
  COUNT(*) AS matches_played,
  SUM(CASE
    WHEN (fl.team_id = f.home_team_id AND f.away_goals = 0) OR
         (fl.team_id = f.away_team_id AND f.home_goals = 0)
    THEN 1 ELSE 0
  END) AS clean_sheets
FROM fixture_lineups fl
INNER JOIN fixtures f ON f.id = fl.fixture_id
WHERE f.season = :season AND f.status = 'FT'
  AND fl.is_starter = true
GROUP BY fl.api_player_id
""")


def load_clean_sheet_data(
    season: int, db: Session,
) -> dict[int, tuple[int, int]]:
    """
    Carica dati clean sheet per ogni giocatore titolare.
    Returns: { api_player_id: (clean_sheets, matches_played) }
    """
    try:
        rows = db.execute(
            CLEAN_SHEET_SQL, {"season": season},
        ).mappings().all()
        return {
            r["api_player_id"]: (r["clean_sheets"], r["matches_played"])
            for r in rows
        }
    except Exception as e:
        logger.warning("Clean sheet data non disponibile: %s", e)
        try:
            db.rollback()
        except Exception:
            pass
        return {}


# ---------------------------------------------------------------------------
# Match impact metrics da events
# ---------------------------------------------------------------------------

MATCH_WINNING_GOALS_SQL = text("""
WITH goal_events AS (
    SELECT
        e.fixture_id,
        e.team_id,
        e.api_player_id,
        e.minute,
        e.api_assist_player_id,
        f.home_team_id,
        f.away_team_id,
        f.home_goals AS final_home,
        f.away_goals AS final_away
    FROM fixture_events e
    INNER JOIN fixtures f ON f.id = e.fixture_id
    WHERE e.type = 'Goal'
      AND COALESCE(e.detail, '') NOT IN ('Missed Penalty', 'Own Goal')
      AND f.season = :season
      AND f.status = 'FT'
    ORDER BY e.fixture_id, e.minute
)
SELECT
    api_player_id,
    COUNT(*) AS match_winning_goals,
    COUNT(DISTINCT fixture_id) AS decisive_matches
FROM goal_events g
WHERE (
    (g.team_id = g.home_team_id AND g.final_home > g.final_away
     AND g.final_home - g.final_away <= 1)
    OR
    (g.team_id = g.away_team_id AND g.final_away > g.final_home
     AND g.final_away - g.final_home <= 1)
)
GROUP BY api_player_id
""")


def load_match_impact_data(
    season: int, db: Session,
) -> dict[int, dict[str, float | None]]:
    """
    Carica metriche di impatto da events.
    Returns: { api_player_id: { metric: value } }
    """
    result: dict[int, dict[str, float | None]] = {}
    try:
        rows = db.execute(
            MATCH_WINNING_GOALS_SQL, {"season": season},
        ).mappings().all()
        for r in rows:
            pid = r["api_player_id"]
            if pid not in result:
                result[pid] = {}
            result[pid]["match_winning_goals"] = r["match_winning_goals"]
    except Exception as e:
        logger.warning("Match impact data non disponibile: %s", e)
        try:
            db.rollback()
        except Exception:
            pass
    return result


# ---------------------------------------------------------------------------
# Build completo delle distribuzioni per ruolo
# ---------------------------------------------------------------------------

def build_role_distributions(
    season: int,
    db: Session,
) -> tuple[RoleDistributions, dict[int, dict[str, Any]]]:
    """
    Costruisce distribuzioni empiriche per ruolo da tutti i dati stagionali.

    Returns:
      (role_distributions, player_metrics_cache)
      - role_distributions: { role: { metric: [sorted_winsorized_values] } }
      - player_metrics_cache: { api_player_id: { all_derived_metrics } }
    """
    try:
        rows = db.execute(
            SEASON_STATS_SQL,
            {"season": season, "min_minutes": MIN_MINUTES},
        ).mappings().all()
    except Exception as e:
        logger.warning("Query distribuzione fallita season=%s: %s", season, e)
        try:
            db.rollback()
        except Exception:
            pass
        return {}, {}

    cs_data = load_clean_sheet_data(season, db)
    impact_data = load_match_impact_data(season, db)

    role_raw: dict[str, list[dict[str, Any]]] = {
        "Goalkeeper": [], "Defender": [], "Midfielder": [], "Attacker": [],
    }
    player_cache: dict[int, dict[str, Any]] = {}

    for row in rows:
        r = dict(row)
        r["pass_accuracy"] = _nullable_float(r.get("passes_accuracy"))
        role = normalize_position(r.get("position"))

        api_pid = r.get("api_player_id")
        derived = compute_player_metrics(r, cs_data)

        if api_pid and api_pid in impact_data:
            for k, v in impact_data[api_pid].items():
                derived[k] = v

        derived["position"] = role
        role_raw[role].append(derived)
        if api_pid:
            player_cache[api_pid] = derived

    distributions: RoleDistributions = {}
    for role, players in role_raw.items():
        metrics_dist: dict[str, list[float]] = {}
        for metric in DISTRIBUTABLE_METRICS:
            values = [p[metric] for p in players if p.get(metric) is not None]
            if len(values) >= 3:
                winsorized = _winsorize(values)
                metrics_dist[metric] = sorted(winsorized)
        distributions[role] = metrics_dist

        logger.info(
            "Distribuzione %s: %d giocatori, %d metriche",
            role, len(players), len(metrics_dist),
        )

    return distributions, player_cache
