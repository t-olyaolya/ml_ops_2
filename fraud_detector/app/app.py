import os
import sys
import time
import logging
from datetime import datetime
from pathlib import Path
import matplotlib
import json

matplotlib.use("Agg")
import matplotlib.pyplot as plt

import pandas as pd
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

sys.path.append(os.path.abspath("./src"))
from preprocessing import load_train_data, run_preproc
from scorer import make_pred, top_importances_features

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("fraud‑service")


class ProcessingService:
    def __init__(self):
        logger.info("Initialising ProcessingService…")
        self.input_dir = "/app/input"
        self.output_dir = "/app/output"
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        self.train = load_train_data()
        logger.info("Service initialized. Observing %s", self.input_dir)

    def _file_to_dataframe(self, file_path: str | Path) -> pd.DataFrame:
        return pd.read_csv(file_path)

    def process_single_file(self, file_path: str | Path):
        try:
            logger.info("Processing %s", file_path)
            input_df = self._file_to_dataframe(file_path)
            logger.info('Starting preprocessing')
            processed_df = run_preproc(input_df)
            logger.info('Making prediction')
            submission, proba = make_pred(processed_df, file_path)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"predictions_{timestamp}_{Path(file_path).name}"
            submission.to_csv(Path(self.output_dir) / output_filename, index=False)
            logger.info("Predictions saved:  %s", output_filename)

            output_json_filename = f"importance_{timestamp}_{Path(file_path).stem}.json"
            with open(Path(self.output_dir) / output_json_filename, "w", encoding="utf‑8") as fp:
                json.dump(top_importances_features(5), fp, ensure_ascii=False, indent=2)
            logger.info("Feature Importance saved:  %s", output_filename)

            output_png_filename = f"Density_{timestamp}_{Path(file_path).stem}.png"
            self.build_plot(output_png_filename, proba)

            logger.info("Processing has been done")
        except Exception:
            logger.exception("Error while processing %s", file_path)

    def build_plot(self, output_png_filename, proba):
        plt.figure(figsize=(6, 4))
        plt.hist(proba, bins=50, density=True)
        plt.title("Density of prediction scores")
        plt.xlabel("Score")
        plt.ylabel("Density")
        plt.tight_layout()
        plt.savefig(Path(self.output_dir) / output_png_filename)
        plt.close()


class _FileHandler(FileSystemEventHandler):
    def __init__(self, service: ProcessingService):
        self._service = service

    #копирование файла в директорию input в контейнере
    # не распознается как on_created,
    # хотя при обычном запуске это работает, поэтому добавили on_any_event
    # def on_created(self, event):
    #   if not event.is_directory and event.src_path.endswith(".csv"):
    #      logger.debug('New file detected: %s', event.src_path)
    #     self.service.process_single_file(event.src_path)

    def on_any_event(self, event):
        if not event.is_directory or event.src_path.endswith(".csv"):
            logger.debug("Detected change: %s", event.src_path)
            self._service.process_single_file(event.src_path)


if __name__ == "__main__":
    logger.info('Starting ML scoring service...')
    service = ProcessingService()
    observer = Observer()
    observer.schedule(_FileHandler(service), path=service.input_dir, recursive=False)
    observer.start()
    logger.info("File observer started")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info('Service stopped by user')
        observer.stop()
    observer.join()
