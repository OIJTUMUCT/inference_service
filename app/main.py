from flask import Flask, jsonify, Response
from flask_compress import Compress
from flasgger import Swagger
from redis_cache import get_segments_json_from_redis, get_segments_updated_at, load_cohort_from_redis, load_timeline_from_redis
from segmentation_tasks.tasks import run_segmentation, run_cohort_analysis, run_timeline
from celery.result import AsyncResult
import sys
import logging
from flask_cors import CORS
import json
from datetime import datetime

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
Compress(app)
CORS(app)

swagger = Swagger(app, parse=True, template={
    "swagger": "2.0",
    "info": {
        "title": "Segmentation API",
        "description": "API для оттока клиентов",
        "version": "1.0"
    }
})

with app.app_context():
    run_segmentation.delay()
    run_cohort_analysis.delay()
    run_timeline.delay()

@app.route("/ping", methods=["GET"])
def ping():
    """
    Проверка доступности сервиса
    ---
    responses:
      200:
        description: Сервис работает
    """
    return "Service is alive", 200

@app.route("/segments", methods=["GET"])
def segment():
    """
    Получить сегментированных клиентов
    ---
    responses:
      200:
        description: JSON с данными сегментов клиентов
        schema:
          type: array
          items:
            type: object
            properties:
              customer_unique_id:
                type: string
                example: "317cfc692e3f86c45c95697c61c853a6"
              recency:
                type: integer
                example: 4
              frequency:
                type: integer
                example: 2
              monetary:
                type: number
                example: 9.59
              r_quartile:
                type: integer
                example: 0
              f_quartile:
                type: integer
                example: 0
              m_quartile:
                type: integer
                example: 0
              rfm_score:
                type: integer
                example: 0
              RFM_Weighted:
                type: number
                example: 0.0
              Churn_Risk:
                type: string
                example: "Высокий риск"
              cumulative_value:
                type: number
                example: 19419155.91
              cumulative_percent:
                type: number
                example: 100.0
              abc_class:
                type: string
                example: "C"
              std_dev:
                type: number
                example: null
              x_category:
                type: string
                example: "Single Purchase"
              segment:
                type: string
                example: "C_Single Purchase"
              segment_description:
                type: string
                example: "Клиенты с одной покупкой, низкий денежный объем."
      500:
        description: Ошибка Redis или парсинга
    """

    try:
        raw_json = get_segments_json_from_redis()
        logging.info(f"Размер JSON в байтах: {sys.getsizeof(raw_json)}")
        return Response(raw_json, mimetype='application/json')
    except Exception as e:
        return jsonify({"error": f"Ошибка чтения сегментов: {str(e)}"}), 500


@app.route("/reload_segments", methods=["POST"])
def reload_segments():
    """
    Перезапуск сегментации в фоне
    ---
    responses:
      200:
        description: Запущена фоновая задача
        schema:
          type: object
          properties:
            status:
              type: string
              example: "ok"
            message:
              type: string
              example: "Запущена фоновая задача сегментации"
            task_id:
              type: string
              example: "b66fc3a9-932e-4d9f-8438-f7bdf80e09ac"
    """
    task = run_segmentation.delay()
    return {
        "status": "ok",
        "message": "Запущена фоновая задача сегментации",
        "task_id": task.id,
    }

@app.route("/status/<task_id>", methods=["GET"])
def get_status(task_id):
    """
    Получить статус фоновой задачи
    ---
    parameters:
      - name: task_id
        in: path
        type: string
        required: true
    responses:
      200:
        description: Статус задачи
    """
    result = AsyncResult(task_id)
    return jsonify({
        "state": result.state,
        "ready": result.ready(),
        "successful": result.successful(),
        "result": str(result.result) if result.ready() else None
    })

@app.route("/segments/meta", methods=["GET"])
def get_segments_meta():
    """
    Получить метаинформацию по сегментам
    ---
    responses:
      200:
        description: Дата последнего обновления сегментов
        schema:
          type: object
          properties:
            updated:
              type: string
              example: "2025-04-19T12:00:00"
      404:
        description: Данные ещё не были загружены
    """
    updated_at = get_segments_updated_at()
    if updated_at:
        return jsonify({"updated": updated_at})
    return jsonify({"error": "Метаданные не найдены"}), 404

@app.route("/cohort", methods=["GET"])
def get_latest_cohort_result():
    """
    Получить последние когортные данные
    ---
    responses:
      200:
        description: Успешное получение когортного анализа
        schema:
          type: object
          properties:
            updated_at:
              type: string
              example: "2025-04-20T14:40:40"
            state_list:
              type: array
              items:
                type: string
              example: ["SP", "RJ", "MG"]
            retention:
              type: array
              items:
                type: object
              example: [{"cohort": "2018-01", "0": 1.0, "1": 0.4}]
            cohort_data:
              type: array
              items:
                type: object
              example: [{"cohort": "2018-01", "customer_id": "abc", "month": 0}]
            regional_cohort:
              type: array
              items:
                type: object
              example: [{"state": "SP", "cohort": "2018-01", "retention": [1.0, 0.6, 0.3]}]
      500:
        description: Ошибка получения данных из Redis
    """
    try:
        result = load_cohort_from_redis()
        return jsonify({
            "updated_at": result["updated_at"],
            "state_list": result["state_list"],
            "retention": result["retention"],
            "cohort_data": result["cohort_data"],
            "regional_cohort": result["regional_cohort"]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/timeline", methods=["GET"])
def get_latest_timeline():
    """
    Получить временные данные сегментов и оттока
    ---
    responses:
      200:
        description: Успешное получение временных данных
        schema:
          type: object
          properties:
            updated_at:
              type: string
              example: "2025-04-20T14:40:40"
            by_churn:
              type: array
              items:
                type: object
                properties:
                  order_purchase_timestamp:
                    type: string
                    example: "2018-08"
                  Churn_Risk:
                    type: string
                    example: "Высокий риск"
                  count:
                    type: integer
                    example: 5041
            by_segment:
              type: array
              items:
                type: object
                properties:
                  order_purchase_timestamp:
                    type: string
                    example: "2018-08"
                  segment_description:
                    type: string
                    example: "Клиенты с одной покупкой, высокий денежный объем."
                  count:
                    type: integer
                    example: 2353
      500:
        description: Ошибка получения данных из Redis
    """

    try:
        result = load_timeline_from_redis()

        if isinstance(result.get("updated_at"), datetime):
            result["updated_at"] = result["updated_at"].isoformat()

        json_str = json.dumps(result, ensure_ascii=False)
        return Response(json_str, content_type="application/json; charset=utf-8")
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)