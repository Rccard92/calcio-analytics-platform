"""
Servizio rosa giocatori per squadra e stagione.
Query unica: join players + player_season_stats filtrato per team_id e season.
"""

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.schemas.teams import PlayerSeasonRow


TEAM_PLAYERS_SQL = text("""
SELECT
  p.id AS player_id,
  p.name AS name,
  COALESCE(p.position, '') AS position,
  COALESCE(s.appearances, 0)::INT AS appearances,
  COALESCE(s.minutes, 0)::INT AS minutes,
  COALESCE(s.goals, 0)::INT AS goals,
  COALESCE(s.assists, 0)::INT AS assists,
  COALESCE(s.shots_total, 0)::INT AS shots,
  ROUND(COALESCE(s.passes_accuracy, 0)::NUMERIC, 2)::FLOAT AS pass_accuracy,
  ROUND(COALESCE(s.rating, 0)::NUMERIC, 2)::FLOAT AS rating,
  COALESCE(s.yellow_cards, 0)::INT AS yellow_cards,
  COALESCE(s.red_cards, 0)::INT AS red_cards
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
            appearances=r["appearances"] or 0,
            minutes=r["minutes"] or 0,
            goals=r["goals"] or 0,
            assists=r["assists"] or 0,
            shots=r["shots"] or 0,
            pass_accuracy=round(float(r["pass_accuracy"] or 0), 2),
            rating=round(float(r["rating"] or 0), 2),
            yellow_cards=r["yellow_cards"] or 0,
            red_cards=r["red_cards"] or 0,
        )
        for r in rows
    ]
