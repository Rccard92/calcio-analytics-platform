"""Team match statistics ORM model."""

from sqlalchemy import Column, Float, ForeignKey, Integer
from sqlalchemy.orm import relationship

from app.core.database import Base


class TeamMatchStats(Base):
    __tablename__ = "team_match_stats"

    id = Column(Integer, primary_key=True, index=True)
    fixture_id = Column(
        Integer,
        ForeignKey("fixtures.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=False, index=True)
    shots_total = Column(Integer, nullable=True)
    shots_on_target = Column(Integer, nullable=True)
    possession = Column(Float, nullable=True)
    fouls = Column(Integer, nullable=True)
    corners = Column(Integer, nullable=True)
    yellow_cards = Column(Integer, nullable=True)
    red_cards = Column(Integer, nullable=True)

    fixture = relationship("Fixture", backref="team_match_stats")
    team = relationship("Team", backref="team_match_stats")
