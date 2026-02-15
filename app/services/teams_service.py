"""
Servizio per aggregazioni squadre per stagione.
Query SQL unica (CTE) per performance; preparata per filtro league_id futuro.
"""

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.schemas.teams import TeamSeasonOverviewRow


# Query unica: espande ogni fixture FT in due righe (casa/trasferta), aggrega per team.
# EXPLAIN: CTE match_rows fa due scan su fixtures (home/away), agg raggruppa per team_id, join teams per nome.
# Per filtro lega futuro: in entrambe le SELECT del CTE aggiungere AND f.league_id = :league_id
TEAMS_SEASON_OVERVIEW_SQL = text("""
WITH match_rows AS (
  SELECT f.home_team_id AS team_id,
         COALESCE(f.home_goals, 0)::INT AS gf,
         COALESCE(f.away_goals, 0)::INT AS ga,
         (COALESCE(f.home_goals, 0) + COALESCE(f.away_goals, 0))::INT AS total_goals
  FROM fixtures f
  WHERE f.season = :season AND f.status = 'FT'
  UNION ALL
  SELECT f.away_team_id,
         COALESCE(f.away_goals, 0),
         COALESCE(f.home_goals, 0),
         (COALESCE(f.home_goals, 0) + COALESCE(f.away_goals, 0))::INT
  FROM fixtures f
  WHERE f.season = :season AND f.status = 'FT'
),
agg AS (
  SELECT
    team_id,
    COUNT(*)::INT AS played,
    COALESCE(SUM(gf), 0)::INT AS goals_for,
    COALESCE(SUM(ga), 0)::INT AS goals_against,
    SUM(CASE WHEN gf > ga THEN 1 ELSE 0 END)::INT AS wins,
    SUM(CASE WHEN gf = ga THEN 1 ELSE 0 END)::INT AS draws,
    SUM(CASE WHEN gf < ga THEN 1 ELSE 0 END)::INT AS losses,
    SUM(CASE WHEN ga = 0 THEN 1 ELSE 0 END)::INT AS clean_sheets,
    SUM(CASE WHEN gf > 0 AND ga > 0 THEN 1 ELSE 0 END)::INT AS btts_count,
    SUM(CASE WHEN total_goals >= 3 THEN 1 ELSE 0 END)::INT AS over25_count
  FROM match_rows
  GROUP BY team_id
)
SELECT t.id AS team_id, t.name AS team_name,
  a.played, a.wins, a.draws, a.losses,
  a.goals_for, a.goals_against, (a.goals_for - a.goals_against) AS goal_diff,
  (a.wins * 3 + a.draws)::INT AS points,
  ROUND(a.goals_for::NUMERIC / NULLIF(a.played, 0), 2)::FLOAT AS avg_goals_for,
  ROUND(a.goals_against::NUMERIC / NULLIF(a.played, 0), 2)::FLOAT AS avg_goals_against,
  a.clean_sheets,
  ROUND(100.0 * a.btts_count / NULLIF(a.played, 0), 2)::FLOAT AS btts_pct,
  ROUND(100.0 * a.over25_count / NULLIF(a.played, 0), 2)::FLOAT AS over25_pct
FROM teams t
JOIN agg a ON a.team_id = t.id
ORDER BY points DESC, goal_diff DESC, goals_for DESC
""")


def get_teams_season_overview(season: int, db: Session, league_id: int | None = None) -> list[TeamSeasonOverviewRow]:
    """
    Restituisce una riga per team con statistiche aggregate sulla stagione.
    Solo match conclusi (FT). league_id opzionale per filtri futuri (ignorato per ora).
    """
    params = {"season": season}
    # Future: if league_id is not None: add to params and use TEAMS_SEASON_OVERVIEW_BY_LEAGUE_SQL
    result = db.execute(TEAMS_SEASON_OVERVIEW_SQL, params)
    rows = result.mappings().all()
    return [
        TeamSeasonOverviewRow(
            team_id=r["team_id"],
            team_name=r["team_name"] or "",
            played=r["played"] or 0,
            wins=r["wins"] or 0,
            draws=r["draws"] or 0,
            losses=r["losses"] or 0,
            goals_for=r["goals_for"] or 0,
            goals_against=r["goals_against"] or 0,
            goal_diff=r["goal_diff"] or 0,
            points=r["points"] or 0,
            avg_goals_for=round(float(r["avg_goals_for"] or 0), 2),
            avg_goals_against=round(float(r["avg_goals_against"] or 0), 2),
            clean_sheets=r["clean_sheets"] or 0,
            btts_pct=round(float(r["btts_pct"] or 0), 2),
            over25_pct=round(float(r["over25_pct"] or 0), 2),
        )
        for r in rows
    ]
