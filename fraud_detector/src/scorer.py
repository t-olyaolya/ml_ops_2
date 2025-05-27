import os
import logging
from pathlib import Path
import lightgbm as lgb
from typing import Dict, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

MODEL_PATH = Path(os.getenv("MODEL_PATH", "./models/lgb.txt"))
THRESHOLD = float(os.getenv("MODEL_THR", "0.59"))

if not MODEL_PATH.exists():
    raise FileNotFoundError(
        f"Model file {MODEL_PATH} not found."
    )

logger.info("Loading model from %s…", MODEL_PATH)
model = lgb.Booster(model_file=str(MODEL_PATH))
logger.info("Model loaded — ready for inference.")

def make_pred(dt, source_info="kafka"):
    score = pd.Series(
        model.predict(dt, num_iteration=model.best_iteration),
        name="score",
    )
    preds = (score >= THRESHOLD).astype(int)
    submission = pd.DataFrame({"score": score, "fraud_flag": preds})
    logger.info(f'Prediction complete for data from {source_info}')
    return submission

def top_importances_features(n: int = 5) -> Dict[str, float]:
    gains = model.feature_importance(importance_type="gain")
    feats = model.feature_name()
    pairs = sorted(zip(feats, gains), key=lambda x: x[1], reverse=True)[:n]
    return {k: float(v) for k, v in pairs}
