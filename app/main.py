from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"status": "ok", "message": "Calcio Analytics Platform is running 🚀"}

@app.get("/health")
def health():
    return {"health": "healthy"}