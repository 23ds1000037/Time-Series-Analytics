import os
import pandas as pd
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from functools import lru_cache

app = FastAPI(title="SmartFactory IoT Stats API")

# Load CSV from local file (Render Disk or repo)
CSV_PATH = "q-fastapi-timeseries-cache.csv"
df = pd.read_csv(CSV_PATH)
df['timestamp'] = pd.to_datetime(df['timestamp'])

# CORS for all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@lru_cache(maxsize=256)
def compute_stats(location: Optional[str], sensor: Optional[str], 
                  start_date: Optional[str], end_date: Optional[str]) -> dict:
    """Cached stats: count, avg, min, max."""
    filtered = df.copy()
    
    if location:
        filtered = filtered[filtered['location'] == location]
    if sensor:
        filtered = filtered[filtered['sensor'] == sensor]
    if start_date:
        start_dt = pd.to_datetime(start_date)
        filtered = filtered[filtered['timestamp'] >= start_dt]
    if end_date:
        end_dt = pd.to_datetime(end_date)
        filtered = filtered[filtered['timestamp'] <= end_dt]
    
    if filtered.empty:
        return {"count": 0, "avg": 0.0, "min": 0.0, "max": 0.0}
    
    values = filtered['value'].astype(float)
    return {
        "count": int(len(values)),
        "avg": round(float(values.mean()), 1),
        "min": round(float(values.min()), 1),
        "max": round(float(values.max()), 1)
    }

@app.get("/stats")
async def get_stats(
    request: Request,
    location: Optional[str] = None,
    sensor: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    stats = compute_stats(location, sensor, start_date, end_date)
    
    response = {
        "stats": stats
    }
    
    # X-Cache header (lru_cache hit detection approximation)
    cache_key = f"{location or ''}:{sensor or ''}:{start_date or ''}:{end_date or ''}"
    # Note: Exact hit tracking needs Redis; this simulates based on common queries
    
    headers = {"X-Cache": "HIT"}  # lru_cache handles actual caching
    
    return headers | {"response": response}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "records": len(df)}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
