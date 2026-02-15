"""
Servizio rosa giocatori per squadra e stagione.
Query unica: join players + player_season_stats filtrato per team_id e season.
"""

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.schemas.teams import PlayerSeasonRow


# Una query: join players e player_season_stats per team_id e season.
TEAM_PLAYERS_SQL = text("""
SELECT
  p.id AS player_id,
  p.name AS name,
  COALESCE(p.position, '') AS position,
  COALESCE(s.minutes, 0)::INT AS minutes,
  COALESCE(s.goals, 0)::INT AS goals,
  COALESCE(s.assists, 0)::INT AS assists,
  COALESCE(s.shots, 0)::INT AS shots,
  ROUND(COALESCE(s.passes_accuracy, 0)::NUMERIC, 2)::FLOAT AS pass_accuracy,
  ROUND(COALESCE(s.rating, 0)::NUMERIC, 2)::FLOAT AS rating
FROM players p
INNER JOIN player_season_stats s ON s.player_id = p.id
WHERE s.team_id = :team_id AND s.season = :season
ORDER BY p.name
""")


def get_team_players(team_id: int, season: int, db: Session) -> list[PlayerSeasonRow]:
    """Rosa giocatori con statistiche stagionali per team_id e season."""
    rows = db.execute(TEAM_PLAYERS_SQL, {"team_id": team_id, "season": season}).mappings().all()
    return [
        PlayerSeasonRow(
            player_id=r["player_id"],
            name=r["name"] or "",
            position=r["position"] or "",
            minutes=r["minutes"] or 0,
            goals=r["goals"] or 0,
            assists=r["assists"] or 0,
            shots=r["shots"] or 0,
            pass_accuracy=round(float(r["pass_accuracy"] or 0), 2),
            rating=round(float(r["rating"] or 0), 2),
        )
        for r in rows
    ]
