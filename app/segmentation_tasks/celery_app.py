from celery import Celery

celery_app = Celery(
    "segmentation",
    broker="redis://redis_cache:6379/0",
    backend="redis://redis_cache:6379/0"
)

celery_app.autodiscover_tasks(['segmentation_tasks'])