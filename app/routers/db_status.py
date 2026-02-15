"""Endpoint temporaneo di debug per verificare le tabelle nel database."""

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_db

router = APIRouter(tags=["debug"])


@router.get("/db-status")
def db_status(db: Session = Depends(get_db)):
    """
    Restituisce l'elenco delle tabelle nello schema public.
    Solo per sviluppo/debug; non espone credenziali.
    """
    result = db.execute(
        text(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' ORDER BY table_name"
        )
    )
    tables = [row[0] for row in result.fetchall()]
    return {"tables": tables}
