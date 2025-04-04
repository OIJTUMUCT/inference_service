from flask import Flask, request, jsonify
from flasgger import Swagger
from model_loader import ModelReloader
from predictor import prepare_input

app = Flask(__name__)
swagger = Swagger(app)
reloader = ModelReloader()

@app.route("/ping", methods=["GET"])
def ping():
    """Healthcheck endpoint
    ---
    responses:
      200:
        description: API is alive
    """
    return "Service is alive", 200

@app.route("/predict", methods=["POST"])
def predict():
    """Make prediction
    ---
    parameters:
      - name: body
        in: body
        required: true
        schema:
          type: object
          required:
            - features
          properties:
            features:
              type: array
              items:
                type: number
              example: [1.0, 2.0, 3.0]
    responses:
      200:
        description: Prediction result
        schema:
          type: object
          properties:
            label:
              type: integer
            probability:
              type: number
    """
    json_data = request.get_json()
    input_data = prepare_input(json_data)
    proba = reloader.predict(input_data)[0][1]  # вероятность класса 1
    label = int(proba > 0.5)
    return jsonify({"label": label, "probability": float(proba)})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)