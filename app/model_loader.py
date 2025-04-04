import joblib
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading
import time

MODEL_PATH = "app/model.pkl"

class ModelReloader(FileSystemEventHandler):
    def __init__(self):
        self.lock = threading.Lock()
        # self.model = self.load_model()
        self.start_watcher()

    def load_model(self):
        print("Загрузка модели...")
        return joblib.load(MODEL_PATH)

    def start_watcher(self):
        observer = Observer()
        observer.schedule(self, path='app/', recursive=False)
        observer.start()
        threading.Thread(target=observer.join, daemon=True).start()

    def on_modified(self, event):
        if event.src_path.endswith("model.pkl"):
            with self.lock:
                print("Обнаружено обновление модели, перезагрузка...")
                self.model = self.load_model()

    def predict(self, input_data = 0.1):
        return [[0.1, 0.9]]
        # with self.lock:
        #     return self.model.predict_proba(input_data)
