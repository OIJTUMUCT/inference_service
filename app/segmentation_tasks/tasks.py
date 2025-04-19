from segmentation_tasks.celery_app import celery_app
from segmentation_tasks.segmentation_pipeline import load_data, run_segmentation_pipeline
from redis_cache import cache_segments_to_redis

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
