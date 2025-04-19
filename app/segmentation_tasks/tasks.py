from segmentation_tasks.celery_app import celery_app
from segmentation_tasks.segmentation_pipeline import load_data, run_segmentation_pipeline
from segmentation_tasks.cohort_pipeline import data_preprocessing
from redis_cache import cache_segments_to_redis, cache_cohort_to_redis

@celery_app.task
def run_segmentation():
    customers, geolocation, order_pay, reviews, orders, item, category_name, products, sellers = load_data()
    data_dict = {
        'customers': customers,
        'geolocation': geolocation,
        'order_payments': order_pay,
        'order_reviews': reviews,
        'orders': orders,
        'items': item,
        'category_translation': category_name,
        'products': products,
        'sellers': sellers
    }
    df = run_segmentation_pipeline(data_dict)
    cache_segments_to_redis(df)

@celery_app.task
def run_cohort_analysis():
    cohort_json = data_preprocessing()
    cache_cohort_to_redis(cohort_json)