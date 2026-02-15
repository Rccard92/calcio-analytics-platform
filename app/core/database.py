"""SQLAlchemy engine, session, and dependency. No auto-create in production."""

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from app.core.config import get_database_url

engine = create_engine(
    get_database_url(),
    pool_pre_ping=True,
    echo=False,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    """Dependency that yields a DB session. Caller must close."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    """
    Crea tutte le tabelle (Base.metadata.create_all).
    I modelli devono essere importati prima per registrare i metadata.
    Temporaneo per sviluppo; in produzione usare migrazioni.
    """
    from app.models import fixture, league, team, team_match_stats  # noqa: F401

    Base.metadata.create_all(bind=engine)
