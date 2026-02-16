"""Pydantic schemas per API Teams."""

from pydantic import BaseModel


# --- Team Detail (Step 2) ---


class TeamInfo(BaseModel):
    team_id: int
    team_name: str


class SeasonStatsBlock(BaseModel):
    """Blocco statistiche: season, home o away."""
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


class FormMatchItem(BaseModel):
    fixture_id: int
    result: str  # "W" | "D" | "L"
    goals_for: int
    goals_against: int


class TeamDetailResponse(BaseModel):
    team: TeamInfo
    season_stats: SeasonStatsBlock
    home_stats: SeasonStatsBlock
    away_stats: SeasonStatsBlock
    form_last5: list[FormMatchItem]


# --- Team Season Overview ---


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


# --- Players (Step 2) ---


class PlayerSeasonRow(BaseModel):
    """Riga rosa: giocatore con statistiche stagionali."""
    player_id: int
    name: str
    position: str
    minutes: int
    goals: int
    assists: int
    shots: int
    pass_accuracy: float
    rating: float

    class Config:
        from_attributes = True


# --- Player Ingestion Response ---


class PlayerIngestionResponse(BaseModel):
    """Risposta dell'endpoint POST ingest-players."""
    team_id: int
    season: int
    players_ingested: int
