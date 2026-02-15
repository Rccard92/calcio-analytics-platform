"""Pydantic schemas per API Teams."""

from pydantic import BaseModel


class TeamSeasonOverviewRow(BaseModel):
    team_id: int
    team_name: str
    played: int
    wins: int
    draws: int
    losses: int
    goals_for: int
    goals_against: int
    goal_diff: int
    points: int
    avg_goals_for: float
    avg_goals_against: float
    clean_sheets: int
    btts_pct: float
    over25_pct: float

    class Config:
        from_attributes = True


class TeamSeasonOverviewResponse(BaseModel):
    season: int
    teams: list[TeamSeasonOverviewRow]
