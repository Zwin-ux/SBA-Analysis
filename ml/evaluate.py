"""Evaluation and artifact writers for the charge-off baseline.

All artifacts land in the gitignored artifacts/ directory:
- model_metrics.json: per-model, per-split discrimination and calibration metrics
- threshold_table.csv: precision/recall trade-offs at documented thresholds
- feature_effects.csv: logistic coefficients and tree feature importances
- calibration.csv: binned predicted-vs-observed charge-off rates
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    confusion_matrix,
    precision_score,
    recall_score,
    roc_auc_score,
)

DEFAULT_THRESHOLDS = (0.25, 0.5, 0.75)

METRICS_PATH = Path("artifacts") / "model_metrics.json"
THRESHOLD_TABLE_PATH = Path("artifacts") / "threshold_table.csv"
FEATURE_EFFECTS_PATH = Path("artifacts") / "feature_effects.csv"
CALIBRATION_PATH = Path("artifacts") / "calibration.csv"


def compute_classification_metrics(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    *,
    thresholds: tuple[float, ...] = DEFAULT_THRESHOLDS,
) -> dict[str, Any]:
    """Compute discrimination, calibration, and thresholded metrics for one split."""
    y_true = np.asarray(y_true, dtype=int)
    y_prob = np.asarray(y_prob, dtype=float)

    metrics: dict[str, Any] = {
        "samples": int(len(y_true)),
        "positives": int(y_true.sum()),
        "base_rate": float(y_true.mean()) if len(y_true) else float("nan"),
        "brier_score": float(brier_score_loss(y_true, y_prob)),
    }
    if len(np.unique(y_true)) > 1:
        metrics["roc_auc"] = float(roc_auc_score(y_true, y_prob))
        metrics["pr_auc"] = float(average_precision_score(y_true, y_prob))
    else:  # A single-class split cannot support ranking metrics.
        metrics["roc_auc"] = None
        metrics["pr_auc"] = None

    metrics["thresholds"] = {}
    for threshold in thresholds:
        y_pred = (y_prob >= threshold).astype(int)
        tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
        metrics["thresholds"][f"{threshold:.2f}"] = {
            "confusion_matrix": {
                "true_negative": int(tn),
                "false_positive": int(fp),
                "false_negative": int(fn),
                "true_positive": int(tp),
            },
            "precision": float(precision_score(y_true, y_pred, zero_division=0)),
            "recall": float(recall_score(y_true, y_pred, zero_division=0)),
            "flag_rate": float(y_pred.mean()) if len(y_pred) else float("nan"),
        }
    return metrics


def threshold_table(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    *,
    model: str,
    split: str,
    thresholds: tuple[float, ...] = DEFAULT_THRESHOLDS,
) -> pd.DataFrame:
    """Precision/recall trade-off rows for the documented thresholds."""
    y_true = np.asarray(y_true, dtype=int)
    y_prob = np.asarray(y_prob, dtype=float)
    rows = []
    for threshold in thresholds:
        y_pred = (y_prob >= threshold).astype(int)
        rows.append(
            {
                "model": model,
                "split": split,
                "threshold": threshold,
                "precision": float(precision_score(y_true, y_pred, zero_division=0)),
                "recall": float(recall_score(y_true, y_pred, zero_division=0)),
                "flagged": int(y_pred.sum()),
                "flag_rate": float(y_pred.mean()) if len(y_pred) else float("nan"),
            }
        )
    return pd.DataFrame(rows)


def calibration_table(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    *,
    model: str,
    split: str,
    bins: int = 10,
) -> pd.DataFrame:
    """Binned predicted-vs-observed rates for a reliability curve."""
    y_true = np.asarray(y_true, dtype=int)
    y_prob = np.asarray(y_prob, dtype=float)
    edges = np.linspace(0.0, 1.0, bins + 1)
    bin_index = np.clip(np.digitize(y_prob, edges[1:-1]), 0, bins - 1)
    rows = []
    for index in range(bins):
        mask = bin_index == index
        if not mask.any():
            continue
        rows.append(
            {
                "model": model,
                "split": split,
                "bin_lower": float(edges[index]),
                "bin_upper": float(edges[index + 1]),
                "mean_predicted": float(y_prob[mask].mean()),
                "observed_rate": float(y_true[mask].mean()),
                "count": int(mask.sum()),
            }
        )
    return pd.DataFrame(rows)


def feature_effects(pipeline: Any, *, model: str) -> pd.DataFrame:
    """Extract coefficients or importances mapped to transformed feature names."""
    preprocessor = pipeline.named_steps["preprocess"]
    estimator = pipeline.named_steps["model"]
    names = [str(name) for name in preprocessor.get_feature_names_out()]

    if hasattr(estimator, "coef_"):
        values = estimator.coef_.ravel()
        kind = "coefficient"
    elif hasattr(estimator, "feature_importances_"):
        values = estimator.feature_importances_
        kind = "importance"
    else:
        raise TypeError(f"Estimator {type(estimator).__name__} exposes no effects to report.")

    frame = pd.DataFrame(
        {
            "model": model,
            "feature": names,
            "kind": kind,
            "value": [float(value) for value in values],
        }
    )
    return frame.sort_values("value", key=lambda s: s.abs(), ascending=False, ignore_index=True)


def write_metrics_json(payload: dict[str, Any], path: Path = METRICS_PATH) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def write_table(frame: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return path
