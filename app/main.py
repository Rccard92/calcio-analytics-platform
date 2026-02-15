import os
from fastapi import FastAPI
from sqlalchemy import create_engine, text

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")

engine = None

if DATABASE_URL:
    engine = create_engine(DATABASE_URL)

@app.get("/")
def root():
    return {"status": "ok", "message": "Calcio Analytics Platform is running 🚀"}

@app.get("/health")
def health():
    return {"health": "healthy"}

@app.get("/db-test")
def db_test():
    if not engine:
        return {"error": "Database not configured"}

    with engine.connect() as connection:
        result = connection.execute(text("SELECT 1"))
        return {"db_response": result.scalar()}