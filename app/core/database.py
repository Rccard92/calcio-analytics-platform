"""SQLAlchemy engine, session, dependency e migrazione automatica."""

import logging

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import declarative_base, sessionmaker

from app.core.config import get_database_url

logger = logging.getLogger(__name__)

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


def _migrate_player_season_stats() -> None:
    """
    Migrazione automatica per player_season_stats.
    Gestisce il passaggio dal vecchio schema (shots, shots_on_target)
    al nuovo schema espanso (shots_total, shots_on, + 20 nuovi campi).

    Idempotente: controlla quali colonne esistono prima di agire.
    Gira all'avvio — se la tabella non esiste ancora, create_all() la crea
    con lo schema corretto e questa funzione non fa nulla.
    """
    insp = inspect(engine)
    if "player_season_stats" not in insp.get_table_names():
        return

    existing_cols = {col["name"] for col in insp.get_columns("player_season_stats")}
    logger.info("player_season_stats: colonne esistenti = %s", sorted(existing_cols))

    with engine.begin() as conn:
        # --- RINOMINA colonne legacy ---
        if "shots" in existing_cols and "shots_total" not in existing_cols:
            conn.execute(text("ALTER TABLE player_season_stats RENAME COLUMN shots TO shots_total"))
            logger.info("Rinominata colonna: shots → shots_total")
            existing_cols.discard("shots")
            existing_cols.add("shots_total")

        if "shots_on_target" in existing_cols and "shots_on" not in existing_cols:
            conn.execute(text("ALTER TABLE player_season_stats RENAME COLUMN shots_on_target TO shots_on"))
            logger.info("Rinominata colonna: shots_on_target → shots_on")
            existing_cols.discard("shots_on_target")
            existing_cols.add("shots_on")

        # --- AGGIUNGI colonne mancanti ---
        new_columns: dict[str, str] = {
            "lineups": "INTEGER",
            "captain": "BOOLEAN",
            "shots_total": "INTEGER",
            "shots_on": "INTEGER",
            "goals_conceded": "INTEGER",
            "saves": "INTEGER",
            "passes_total": "INTEGER",
            "key_passes": "INTEGER",
            "tackles_total": "INTEGER",
            "blocks": "INTEGER",
            "interceptions": "INTEGER",
            "duels_total": "INTEGER",
            "duels_won": "INTEGER",
            "dribbles_attempts": "INTEGER",
            "dribbles_success": "INTEGER",
            "dribbled_past": "INTEGER",
            "fouls_drawn": "INTEGER",
            "fouls_committed": "INTEGER",
            "yellow_cards": "INTEGER",
            "red_cards": "INTEGER",
            "penalty_won": "INTEGER",
            "penalty_committed": "INTEGER",
            "penalty_scored": "INTEGER",
            "penalty_missed": "INTEGER",
            "penalty_saved": "INTEGER",
        }

        added = []
        for col_name, col_type in new_columns.items():
            if col_name not in existing_cols:
                conn.execute(text(
                    f"ALTER TABLE player_season_stats ADD COLUMN {col_name} {col_type}"
                ))
                added.append(col_name)

        if added:
            logger.info("player_season_stats: aggiunte %s colonne: %s", len(added), added)
        else:
            logger.info("player_season_stats: schema già aggiornato, nessuna modifica")

        # --- INDICE composito ---
        idx_rows = conn.execute(text(
            "SELECT 1 FROM pg_indexes WHERE indexname = 'ix_player_season_stats_team_season'"
        )).fetchone()
        if not idx_rows:
            conn.execute(text(
                "CREATE INDEX ix_player_season_stats_team_season "
                "ON player_season_stats (team_id, season)"
            ))
            logger.info("Creato indice composito ix_player_season_stats_team_season")


def init_db() -> None:
    """
    Crea tutte le tabelle e applica migrazioni automatiche.
    I modelli devono essere importati prima per registrare i metadata.
    """
    from app.models import (  # noqa: F401
        fixture,
        ingestion_job,
        league,
        player,
        player_season_stats,
        team,
        team_match_stats,
    )

    Base.metadata.create_all(bind=engine)
    logger.info("create_all completato")

    try:
        _migrate_player_season_stats()
    except Exception as e:
        logger.exception("Errore durante migrazione player_season_stats: %s", e)
