"""
Formazioni per singola fixture: un record per ogni giocatore schierato.
Usato per calcolare metriche di impatto (clean sheet involvement, minuti effettivi).
"""

from sqlalchemy import Boolean, Column, ForeignKey, Index, Integer, String
from sqlalchemy.orm import relationship

from app.core.database import Base


class FixtureLineup(Base):
    __tablename__ = "fixture_lineups"

    id = Column(Integer, primary_key=True, index=True)
    fixture_id = Column(
        Integer, ForeignKey("fixtures.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    team_id = Column(
        Integer, ForeignKey("teams.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    api_player_id = Column(Integer, nullable=False, index=True)
    player_name = Column(String(255), nullable=True)
    position = Column(String(32), nullable=True)
    is_starter = Column(Boolean, nullable=False, default=False)
    minutes_played = Column(Integer, nullable=True)

    # --- Relazioni ---
    fixture = relationship("Fixture", backref="lineups")
    team = relationship("Team")

    # --- Vincoli ---
    __table_args__ = (
        Index(
            "uq_fixture_lineups_fixture_player",
            "fixture_id", "api_player_id",
            unique=True,
        ),
    )
