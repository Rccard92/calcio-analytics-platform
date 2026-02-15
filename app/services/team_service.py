"""
Servizio dettaglio singola squadra: statistiche stagione, split casa/trasferta, form ultime 5.
Solo fixture concluse (FT). Query con CTE per evitare N+1.
"""

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.schemas.teams import (
    FormMatchItem,
    SeasonStatsBlock,
    TeamDetailResponse,
    TeamInfo,
)

# Una riga per team con tutte le aggregate: overall, home, away.
# CTE match_rows: ogni partita della squadra come una riga con gf, ga, is_home.
TEAM_DETAIL_STATS_SQL = text("""
WITH match_rows AS (
  SELECT
    f.id AS fixture_id,
    f.date,
    CASE WHEN f.home_team_id = :team_id THEN 1 ELSE 0 END AS is_home,
    CASE WHEN f.home_team_id = :team_id THEN COALESCE(f.home_goals, 0) ELSE COALESCE(f.away_goals, 0) END AS gf,
    CASE WHEN f.home_team_id = :team_id THEN COALESCE(f.away_goals, 0) ELSE COALESCE(f.home_goals, 0) END AS ga
  FROM fixtures f
  WHERE f.season = :season AND f.status = 'FT' AND (f.home_team_id = :team_id OR f.away_team_id = :team_id)
),
season_agg AS (
  SELECT
    COUNT(*)::INT AS played,
    SUM(CASE WHEN gf > ga THEN 1 ELSE 0 END)::INT AS wins,
    SUM(CASE WHEN gf = ga THEN 1 ELSE 0 END)::INT AS draws,
    SUM(CASE WHEN gf < ga THEN 1 ELSE 0 END)::INT AS losses,
    COALESCE(SUM(gf), 0)::INT AS goals_for,
    COALESCE(SUM(ga), 0)::INT AS goals_against
  FROM match_rows
),
home_agg AS (
  SELECT
    COUNT(*)::INT AS played,
    SUM(CASE WHEN gf > ga THEN 1 ELSE 0 END)::INT AS wins,
    SUM(CASE WHEN gf = ga THEN 1 ELSE 0 END)::INT AS draws,
    SUM(CASE WHEN gf < ga THEN 1 ELSE 0 END)::INT AS losses,
    COALESCE(SUM(gf), 0)::INT AS goals_for,
    COALESCE(SUM(ga), 0)::INT AS goals_against
  FROM match_rows WHERE is_home = 1
),
away_agg AS (
  SELECT
    COUNT(*)::INT AS played,
    SUM(CASE WHEN gf > ga THEN 1 ELSE 0 END)::INT AS wins,
    SUM(CASE WHEN gf = ga THEN 1 ELSE 0 END)::INT AS draws,
    SUM(CASE WHEN gf < ga THEN 1 ELSE 0 END)::INT AS losses,
    COALESCE(SUM(gf), 0)::INT AS goals_for,
    COALESCE(SUM(ga), 0)::INT AS goals_against
  FROM match_rows WHERE is_home = 0
)
SELECT
  t.id AS team_id, t.name AS team_name,
  COALESCE(s.played, 0)::INT AS s_played, COALESCE(s.wins, 0)::INT AS s_wins, COALESCE(s.draws, 0)::INT AS s_draws,
  COALESCE(s.losses, 0)::INT AS s_losses, COALESCE(s.goals_for, 0)::INT AS s_gf, COALESCE(s.goals_against, 0)::INT AS s_ga,
  ROUND(COALESCE(s.goals_for, 0)::NUMERIC / NULLIF(s.played, 0), 2)::FLOAT AS s_avg_gf,
  ROUND(COALESCE(s.goals_against, 0)::NUMERIC / NULLIF(s.played, 0), 2)::FLOAT AS s_avg_ga,
  COALESCE(h.played, 0)::INT AS h_played, COALESCE(h.wins, 0)::INT AS h_wins, COALESCE(h.draws, 0)::INT AS h_draws,
  COALESCE(h.losses, 0)::INT AS h_losses, COALESCE(h.goals_for, 0)::INT AS h_gf, COALESCE(h.goals_against, 0)::INT AS h_ga,
  ROUND(COALESCE(h.goals_for, 0)::NUMERIC / NULLIF(h.played, 0), 2)::FLOAT AS h_avg_gf,
  ROUND(COALESCE(h.goals_against, 0)::NUMERIC / NULLIF(h.played, 0), 2)::FLOAT AS h_avg_ga,
  COALESCE(a.played, 0)::INT AS a_played, COALESCE(a.wins, 0)::INT AS a_wins, COALESCE(a.draws, 0)::INT AS a_draws,
  COALESCE(a.losses, 0)::INT AS a_losses, COALESCE(a.goals_for, 0)::INT AS a_gf, COALESCE(a.goals_against, 0)::INT AS a_ga,
  ROUND(COALESCE(a.goals_for, 0)::NUMERIC / NULLIF(a.played, 0), 2)::FLOAT AS a_avg_gf,
  ROUND(COALESCE(a.goals_against, 0)::NUMERIC / NULLIF(a.played, 0), 2)::FLOAT AS a_avg_ga
FROM teams t
LEFT JOIN season_agg s ON TRUE
LEFT JOIN home_agg h ON TRUE
LEFT JOIN away_agg a ON TRUE
WHERE t.id = :team_id
""")

# Form ultime 5: fixture_id, result (W/D/L), goals_for, goals_against, ordinato per data DESC.
TEAM_FORM_LAST5_SQL = text("""
WITH match_rows AS (
  SELECT
    f.id AS fixture_id,
    f.date,
    CASE WHEN f.home_team_id = :team_id THEN COALESCE(f.home_goals, 0) ELSE COALESCE(f.away_goals, 0) END AS gf,
    CASE WHEN f.home_team_id = :team_id THEN COALESCE(f.away_goals, 0) ELSE COALESCE(f.home_goals, 0) END AS ga
  FROM fixtures f
  WHERE f.season = :season AND f.status = 'FT' AND (f.home_team_id = :team_id OR f.away_team_id = :team_id)
),
with_result AS (
  SELECT fixture_id, date, gf AS goals_for, ga AS goals_against,
         CASE WHEN gf > ga THEN 'W' WHEN gf < ga THEN 'L' ELSE 'D' END AS result
  FROM match_rows
)
SELECT fixture_id, result, goals_for, goals_against
FROM with_result
ORDER BY date DESC
LIMIT 5
""")


def _row_to_stats(r: dict, prefix: str) -> SeasonStatsBlock:
    played = r.get(prefix + "played") or 0
    goals_for = r.get(prefix + "gf") or 0
    goals_against = r.get(prefix + "ga") or 0
    return SeasonStatsBlock(
        played=played,
        wins=r.get(prefix + "wins") or 0,
        draws=r.get(prefix + "draws") or 0,
        losses=r.get(prefix + "losses") or 0,
        goals_for=goals_for,
        goals_against=goals_against,
        goal_diff=goals_for - goals_against,
        points=(r.get(prefix + "wins") or 0) * 3 + (r.get(prefix + "draws") or 0),
        avg_goals_for=round(float(r.get(prefix + "avg_gf") or 0), 2),
        avg_goals_against=round(float(r.get(prefix + "avg_ga") or 0), 2),
    )


def get_team_season_detail(team_id: int, season: int, db: Session) -> TeamDetailResponse | None:
    """
    Dettaglio squadra per stagione: team, season_stats, home_stats, away_stats, form_last5.
    Solo match FT. None se il team non esiste o non ha partite.
    """
    row = db.execute(TEAM_DETAIL_STATS_SQL, {"team_id": team_id, "season": season}).mappings().first()
    if not row:
        return None
    team_id_val = row["team_id"]
    team_name_val = row["team_name"] or ""
    if team_id_val is None:
        return None

    form_rows = db.execute(TEAM_FORM_LAST5_SQL, {"team_id": team_id, "season": season}).mappings().all()
    form_last5 = [
        FormMatchItem(
            fixture_id=r["fixture_id"],
            result=r["result"] or "D",
            goals_for=r["goals_for"] or 0,
            goals_against=r["goals_against"] or 0,
        )
        for r in form_rows
    ]

    return TeamDetailResponse(
        team=TeamInfo(team_id=team_id_val, team_name=team_name_val),
        season_stats=_row_to_stats(dict(row), "s_"),
        home_stats=_row_to_stats(dict(row), "h_"),
        away_stats=_row_to_stats(dict(row), "a_"),
        form_last5=form_last5,
    )
