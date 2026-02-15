"""Player ORM model. Dati anagrafici giocatore (API-Football)."""

from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship

from app.core.database import Base


class Player(Base):
    __tablename__ = "players"

    id = Column(Integer, primary_key=True, index=True)
    api_player_id = Column(Integer, unique=True, nullable=True, index=True)
    name = Column(String(255), nullable=False)
    age = Column(Integer, nullable=True)
    position = Column(String(64), nullable=True)
    nationality = Column(String(128), nullable=True)

    player_season_stats = relationship("PlayerSeasonStats", backref="player", lazy="selectin")
