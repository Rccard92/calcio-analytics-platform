"""
Player season statistics: una riga per giocatore per stagione per squadra.
Schema espanso con tutte le metriche disponibili da API-Football v3.
"""

from sqlalchemy import Boolean, Column, DateTime, Float, ForeignKey, Index, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class PlayerSeasonStats(Base):
    __tablename__ = "player_season_stats"

    id = Column(Integer, primary_key=True, index=True)
    player_id = Column(Integer, ForeignKey("players.id", ondelete="CASCADE"), nullable=False, index=True)
    team_id = Column(Integer, ForeignKey("teams.id", ondelete="CASCADE"), nullable=False, index=True)
    season = Column(Integer, nullable=False, index=True)

    # --- GAMES ---
    appearances = Column(Integer, nullable=True)
    lineups = Column(Integer, nullable=True)
    minutes = Column(Integer, nullable=True)
    rating = Column(Float, nullable=True)
    captain = Column(Boolean, nullable=True)

    # --- SHOTS ---
    shots_total = Column(Integer, nullable=True)
    shots_on = Column(Integer, nullable=True)

    # --- GOALS ---
    goals = Column(Integer, nullable=True)
    assists = Column(Integer, nullable=True)
    goals_conceded = Column(Integer, nullable=True)
    saves = Column(Integer, nullable=True)

    # --- PASSES ---
    passes_total = Column(Integer, nullable=True)
    key_passes = Column(Integer, nullable=True)
    passes_accuracy = Column(Float, nullable=True)

    # --- TACKLES ---
    tackles_total = Column(Integer, nullable=True)
    blocks = Column(Integer, nullable=True)
    interceptions = Column(Integer, nullable=True)

    # --- DUELS ---
    duels_total = Column(Integer, nullable=True)
    duels_won = Column(Integer, nullable=True)

    # --- DRIBBLES ---
    dribbles_attempts = Column(Integer, nullable=True)
    dribbles_success = Column(Integer, nullable=True)
    dribbled_past = Column(Integer, nullable=True)

    # --- FOULS ---
    fouls_drawn = Column(Integer, nullable=True)
    fouls_committed = Column(Integer, nullable=True)

    # --- CARDS ---
    yellow_cards = Column(Integer, nullable=True)
    red_cards = Column(Integer, nullable=True)

    # --- PENALTY ---
    penalty_won = Column(Integer, nullable=True)
    penalty_committed = Column(Integer, nullable=True)
    penalty_scored = Column(Integer, nullable=True)
    penalty_missed = Column(Integer, nullable=True)
    penalty_saved = Column(Integer, nullable=True)

    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # --- RELAZIONI ---
    team = relationship("Team", backref="player_season_stats")

    # --- INDICI ---
    __table_args__ = (
        Index("ix_player_season_stats_team_season", "team_id", "season"),
    )
