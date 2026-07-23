"""Train and evaluate the leakage-safe charge-off baseline.

Usage:
    python -m ml.train --input data/processed/sba_loans_clean.csv

The command fails with instructions when the cleaned dataset is absent; the
full FOIA extract is intentionally not bundled with this repository.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd
import sklearn
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from ml import RANDOM_SEED
from ml.evaluate import (
    CALIBRATION_PATH,
    FEATURE_EFFECTS_PATH,
    METRICS_PATH,
    THRESHOLD_TABLE_PATH,
    calibration_table,
    compute_classification_metrics,
    feature_effects,
    threshold_table,
    write_metrics_json,
    write_table,
)
from ml.features import (
    CATEGORICAL_FEATURES,
    LABEL_MAP,
    NUMERIC_FEATURES,
    TARGET_COLUMN,
    build_feature_matrix,
    prepare_modeling_frame,
)
from ml.split import temporal_split

DEFAULT_INPUT = Path("data") / "processed" / "sba_loans_clean.csv"
MIN_MODELING_ROWS = 100

MISSING_INPUT_MESSAGE = """\
Input CSV not found: {path}

The full public FOIA extract is not bundled with this repository. To train on
real data:
  1. Download the public SBA 7(a) and 504 FOIA extracts from
     https://data.sba.gov/dataset/7-a-504-foia into data/raw/.
  2. Run the cleaning pipeline:  python src/clean.py
  3. Re-run this command:        python -m ml.train --input {path}
"""


def build_preprocessor() -> ColumnTransformer:
    """Impute+scale numeric features; impute+one-hot categorical features."""
    numeric = Pipeline(
        [
            ("impute", SimpleImputer(strategy="median")),
            ("scale", StandardScaler()),
        ]
    )
    categorical = Pipeline(
        [
            ("impute", SimpleImputer(strategy="constant", fill_value="missing")),
            ("encode", OneHotEncoder(handle_unknown="ignore")),
        ]
    )
    return ColumnTransformer(
        [
            ("numeric", numeric, list(NUMERIC_FEATURES)),
            ("categorical", categorical, list(CATEGORICAL_FEATURES)),
        ]
    )


def build_models(seed: int = RANDOM_SEED) -> dict[str, Pipeline]:
    """Two interpretable baselines with class weighting for imbalance."""
    return {
        "logistic_regression": Pipeline(
            [
                ("preprocess", build_preprocessor()),
                (
                    "model",
                    LogisticRegression(
                        C=1.0,
                        class_weight="balanced",
                        max_iter=5000,
                        random_state=seed,
                    ),
                ),
            ]
        ),
        "random_forest": Pipeline(
            [
                ("preprocess", build_preprocessor()),
                (
                    "model",
                    RandomForestClassifier(
                        n_estimators=200,
                        max_depth=5,
                        min_samples_leaf=10,
                        class_weight="balanced",
                        random_state=seed,
                        n_jobs=-1,
                    ),
                ),
            ]
        ),
    }


def run_training(
    input_path: Path,
    *,
    artifacts_dir: Path = Path("artifacts"),
    seed: int = RANDOM_SEED,
) -> dict[str, Any]:
    """Train both baselines with a temporal split and write evaluation artifacts."""
    df = pd.read_csv(input_path, low_memory=False)
    frame = prepare_modeling_frame(df)

    if len(frame) < MIN_MODELING_ROWS:
        raise ValueError(
            f"Only {len(frame)} rows have a terminal PIF/CHGOFF outcome; "
            f"at least {MIN_MODELING_ROWS} are required for a meaningful baseline."
        )

    excluded = df.loc[~df.index.isin(frame.index)]
    excluded_statuses = {
        str(status): int(count)
        for status, count in excluded["loan_status"].fillna("<missing>").value_counts().items()
    }

    split = temporal_split(frame)
    matrices = {
        "train": build_feature_matrix(split.train),
        "validation": build_feature_matrix(split.validation),
        "test": build_feature_matrix(split.test),
    }

    x_train, y_train = matrices["train"]
    if y_train.nunique() < 2:
        raise ValueError(
            "Training years contain a single outcome class; "
            "a charge-off baseline cannot be fit on this split."
        )

    models = build_models(seed)
    metrics_payload: dict[str, Any] = {}
    threshold_frames: list[pd.DataFrame] = []
    calibration_frames: list[pd.DataFrame] = []
    effects_frames: list[pd.DataFrame] = []

    for name, pipeline in models.items():
        pipeline.fit(x_train, y_train)
        metrics_payload[name] = {}
        for split_name, (x_part, y_part) in matrices.items():
            y_prob = pipeline.predict_proba(x_part)[:, 1]
            metrics_payload[name][split_name] = compute_classification_metrics(
                y_part.to_numpy(), y_prob
            )
            if split_name in ("validation", "test"):
                threshold_frames.append(
                    threshold_table(y_part.to_numpy(), y_prob, model=name, split=split_name)
                )
                calibration_frames.append(
                    calibration_table(y_part.to_numpy(), y_prob, model=name, split=split_name)
                )
        effects_frames.append(feature_effects(pipeline, model=name))

    payload: dict[str, Any] = {
        "run": {
            "input": str(input_path),
            "input_rows": int(len(df)),
            "modeled_rows": int(len(frame)),
            "excluded_status_counts": excluded_statuses,
            "label_definition": {
                "target": TARGET_COLUMN,
                "mapping": dict(LABEL_MAP),
                "excluded": "CANCLD, EXEMPT, missing, and any other non-terminal status",
            },
            "seed": seed,
            "sklearn_version": sklearn.__version__,
            "split": split.summary(),
        },
        "models": metrics_payload,
    }

    write_metrics_json(payload, artifacts_dir / METRICS_PATH.name)
    write_table(
        pd.concat(threshold_frames, ignore_index=True),
        artifacts_dir / THRESHOLD_TABLE_PATH.name,
    )
    write_table(
        pd.concat(calibration_frames, ignore_index=True),
        artifacts_dir / CALIBRATION_PATH.name,
    )
    write_table(
        pd.concat(effects_frames, ignore_index=True),
        artifacts_dir / FEATURE_EFFECTS_PATH.name,
    )
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train the leakage-safe charge-off baseline.")
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="Cleaned analytical CSV produced by src/clean.py.",
    )
    parser.add_argument(
        "--artifacts-dir",
        type=Path,
        default=Path("artifacts"),
        help="Directory for generated evaluation artifacts (gitignored).",
    )
    parser.add_argument("--seed", type=int, default=RANDOM_SEED, help="Random seed.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.input.exists():
        print(MISSING_INPUT_MESSAGE.format(path=args.input), file=sys.stderr)
        return 2

    payload = run_training(args.input, artifacts_dir=args.artifacts_dir, seed=args.seed)
    for name, splits in payload["models"].items():
        test_metrics = splits["test"]
        print(
            f"{name}: test ROC-AUC={test_metrics['roc_auc']}, "
            f"PR-AUC={test_metrics['pr_auc']}, Brier={test_metrics['brier_score']:.4f}, "
            f"base rate={test_metrics['base_rate']:.4f} on {test_metrics['samples']:,} rows"
        )
    print(f"Artifacts written to {args.artifacts_dir}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
