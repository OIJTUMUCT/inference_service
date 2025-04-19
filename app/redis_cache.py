import redis
import json
import pandas as pd
from datetime import datetime

r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
REDIS_KEY = "cached_segments"
REDIS_META_UPDATED_AT_KEY = "meta:segments_updated_at"

def cache_segments_to_redis(df: pd.DataFrame):
    json_data = df.to_json(orient="records", force_ascii=False)
    r.set(REDIS_KEY, json_data)
    r.set(REDIS_META_UPDATED_AT_KEY, datetime.utcnow().isoformat())

def get_segments_json_from_redis() -> str:
    json_data = r.get(REDIS_KEY)
    if json_data is None:
        raise ValueError("Сегменты ещё не закэшированы")
    return json_data

def get_segments_updated_at() -> str | None:
    return r.get(REDIS_META_UPDATED_AT_KEY)