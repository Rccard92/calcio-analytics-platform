"""
Eventi di una fixture: gol, cartellini, sostituzioni, VAR.
Usato per calcolare metriche di impatto (match-winning goals, points contribution).
"""

from sqlalchemy import Column, ForeignKey, Index, Integer, String
from sqlalchemy.orm import relationship

from app.core.database import Base


class FixtureEvent(Base):
    __tablename__ = "fixture_events"

    id = Column(Integer, primary_key=True, index=True)
    fixture_id = Column(
        Integer, ForeignKey("fixtures.id", ondelete="CASCADE"),
        nullable=False,
    )
    team_id = Column(
        Integer, ForeignKey("teams.id", ondelete="CASCADE"),
        nullable=False,
    )
    minute = Column(Integer, nullable=True)
    extra_minute = Column(Integer, nullable=True)
    type = Column(String(64), nullable=False)
    detail = Column(String(128), nullable=True)
    api_player_id = Column(Integer, nullable=True)
    player_name = Column(String(255), nullable=True)
    api_assist_player_id = Column(Integer, nullable=True)
    assist_player_name = Column(String(255), nullable=True)

    # --- Relazioni ---
    fixture = relationship("Fixture", backref="events")
    team = relationship("Team")

    # --- Indici ---
    __table_args__ = (
        Index("ix_fixture_events_fixture_id", "fixture_id"),
        Index("ix_fixture_events_player", "api_player_id"),
        Index("ix_fixture_events_type", "type"),
    )
