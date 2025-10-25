from fastapi import FastAPI

app = FastAPI(title="TSMP-OOP API")

@app.get("/health")
def health():
    return {"status": "ok"}
