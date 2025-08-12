from fastapi import FastAPI

app = FastAPI(title="PwC Data Engineer SEC Filings")

@app.get("/")
def root():
    return {"message": "PwC Data Engineer Challenge - SEC Filings"}
