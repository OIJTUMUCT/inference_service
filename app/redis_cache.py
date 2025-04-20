import redis
import json
import pandas as pd
from datetime import datetime
from typing import List

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

def cache_timeline_to_redis(by_segment_json: str,
                            by_churn_json: str):
    r.set("timeline:by_segment", by_segment_json)
    r.set("timeline:by_churn",   by_churn_json)
    r.set("timeline:updated_at", datetime.utcnow().isoformat())
    
def load_timeline_from_redis() -> dict:
    """
    Загружает из Redis ключи:
      - timeline:by_churn    (JSON‑строка)
      - timeline:by_segment  (JSON‑строка)
      - timeline:updated_at  (ISO‑строка)
    и возвращает уже готовый Python‑объект.
    """
    # читаем «сырые» байты/строки
    raw_churn = r.get("timeline:by_churn")
    raw_seg   = r.get("timeline:by_segment")
    raw_upd   = r.get("timeline:updated_at")

    # десериализуем JSON в списки словарей
    by_churn = json.loads(raw_churn)   if raw_churn else []
    by_segment = json.loads(raw_seg)   if raw_seg   else []

    # парсим updated_at в datetime (по желанию)
    updated_at = None
    if raw_upd:
        try:
            # если строка в формате ISO, например "2025-04-20T14:19:31.650075"
            updated_at = datetime.fromisoformat(raw_upd.decode() if isinstance(raw_upd, bytes) else raw_upd)
        except ValueError:
            updated_at = raw_upd  # оставим как строку, если не парсится

    return {
        "by_churn":    by_churn,
        "by_segment":  by_segment,
        "updated_at":  updated_at
    }