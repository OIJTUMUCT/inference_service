import redis
import json
import pandas as pd
from datetime import datetime
from typing import List

r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

# =====================
# Сегментация клиентов
# =====================
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


# =====================
# Когортный анализ
# =====================
# def cache_cohort_to_redis(
#     state_list: List[str],
#     retention: pd.DataFrame,
#     cohort_data: pd.DataFrame,
#     regional_cohort: pd.DataFrame
# ):
#     print("retention type", type(retention))
#     print("retention shape", retention.shape)
#     r.set("cohort:state_list", json.dumps(state_list, ensure_ascii=False))
#     r.set("cohort:retention", retention.to_json(orient="split"))
#     r.set("cohort:cohort_data", cohort_data.to_json(orient="split"))
#     r.set("cohort:regional_cohort", regional_cohort.to_json(orient="split"))
#     r.set("cohort:updated_at", datetime.utcnow().isoformat())


def load_cohort_from_redis() -> dict:
    def parse_json(key):
        raw = r.get(f"cohort:{key}")
        return json.loads(raw) if raw else {}

    return {
        "state_list": json.loads(r.get("cohort:state_list") or "[]"),
        "retention": parse_json("retention"),
        "cohort_data": parse_json("cohort_data"),
        "regional_cohort": parse_json("regional_cohort"),
        "updated_at": r.get("cohort:updated_at")
    }

def cache_cohort_to_redis(cohort_json: dict):
    
    
    r.set("cohort:state_list", json.dumps(cohort_json["state_list"], ensure_ascii=False))
    r.set("cohort:retention", json.dumps(cohort_json["retention"], ensure_ascii=False))
    r.set("cohort:cohort_data", json.dumps(cohort_json["cohort_data"], ensure_ascii=False))
    r.set("cohort:regional_cohort", json.dumps(cohort_json["regional_cohort"], ensure_ascii=False))
    r.set("cohort:updated_at", cohort_json["updated_at"])