import os
from typing import Optional

import pandas as pd
from fastapi import FastAPI, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from functools import lru_cache

app = FastAPI(title="SmartFactory IoT Stats API", debug=True)

CSV_PATH = "q-fastapi-timeseries-cache.csv"

try:
    df = pd.read_csv(CSV_PATH)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["timestamp", "value", "location", "sensor"])
except Exception as e:
    raise RuntimeError(f"Failed to load CSV: {e}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_seen_cache_keys = set()

@lru_cache(maxsize=256)
def compute_stats(
    location: Optional[str],
    sensor: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
):
    filtered = df.copy()

    if location:
        filtered = filtered[filtered["location"] == location]

    if sensor:
        filtered = filtered[filtered["sensor"] == sensor]

    if start_date:
        start_dt = pd.to_datetime(start_date, utc=True, errors="raise")
        filtered = filtered[filtered["timestamp"] >= start_dt]

    if end_date:
        end_dt = pd.to_datetime(end_date, utc=True, errors="raise")
        filtered = filtered[filtered["timestamp"] <= end_dt]

    if filtered.empty:
        return {
            "count": 0,
            "avg": 0.0,
            "min": 0.0,
            "max": 0.0
        }

    values = filtered["value"]

    return {
        "count": int(values.count()),
        "avg": round(float(values.mean()), 2),
        "min": round(float(values.min()), 2),
        "max": round(float(values.max()), 2),
    }

@app.get("/stats")
def get_stats(
    response: Response,
    location: Optional[str] = None,
    sensor: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    try:
        cache_key = (location, sensor, start_date, end_date)
        cache_hit = cache_key in _seen_cache_keys

        stats = compute_stats(location, sensor, start_date, end_date)
        _seen_cache_keys.add(cache_key)

        response.headers["X-Cache"] = "HIT" if cache_hit else "MISS"
        return {"stats": stats}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "records": int(len(df))
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
