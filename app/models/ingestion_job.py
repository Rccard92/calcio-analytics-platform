"""Modello per i job di ingestion in background."""

from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.sql import func

from app.core.database import Base


class IngestionJob(Base):
    __tablename__ = "ingestion_jobs"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    season = Column(Integer, nullable=False, index=True)
    status = Column(String(32), nullable=False, default="pending", index=True)
    total_fixtures = Column(Integer, nullable=False, default=0)
    processed_fixtures = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    error_message = Column(Text, nullable=True)
