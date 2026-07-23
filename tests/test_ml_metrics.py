from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("sklearn")

from ml.evaluate import (  # noqa: E402
    calibration_table,
    compute_classification_metrics,
    threshold_table,
)
from ml.train import main, run_training  # noqa: E402


def test_metrics_computation_on_known_values() -> None:
    y_true = np.array([0, 0, 1, 1])
    y_prob = np.array([0.1, 0.4, 0.6, 0.9])

    metrics = compute_classification_metrics(y_true, y_prob)

    assert metrics["samples"] == 4
    assert metrics["positives"] == 2
    assert metrics["base_rate"] == 0.5
    assert metrics["roc_auc"] == 1.0
    assert metrics["pr_auc"] == 1.0
    at_half = metrics["thresholds"]["0.50"]
    assert at_half["confusion_matrix"] == {
        "true_negative": 2,
        "false_positive": 0,
        "false_negative": 0,
        "true_positive": 2,
    }
    assert at_half["precision"] == 1.0
    assert at_half["recall"] == 1.0


def test_single_class_split_reports_no_ranking_metrics() -> None:
    metrics = compute_classification_metrics(np.zeros(5, dtype=int), np.full(5, 0.2))

    assert metrics["roc_auc"] is None
    assert metrics["pr_auc"] is None
    assert metrics["base_rate"] == 0.0


def test_threshold_table_shape() -> None:
    y_true = np.array([0, 1, 0, 1, 1])
    y_prob = np.array([0.2, 0.7, 0.4, 0.9, 0.3])

    table = threshold_table(y_true, y_prob, model="m", split="test")

    assert list(table["threshold"]) == [0.25, 0.5, 0.75]
    assert (table["flag_rate"] <= 1).all()
    assert (table["precision"] <= 1).all()


def test_calibration_bins_are_consistent() -> None:
    rng = np.random.default_rng(0)
    y_prob = rng.uniform(size=200)
    y_true = (rng.uniform(size=200) < y_prob).astype(int)

    table = calibration_table(y_true, y_prob, model="m", split="test")

    assert len(table) <= 10
    assert table["count"].sum() == 200
    for row in table.itertuples():
        assert row.bin_lower <= row.mean_predicted <= row.bin_upper + 1e-9


def test_end_to_end_training_writes_artifacts(
    tmp_path, synthetic_loans: pd.DataFrame
) -> None:
    """SYNTHETIC-data smoke test proving mechanics only, never real metrics."""
    input_path = tmp_path / "synthetic_clean.csv"
    artifacts_dir = tmp_path / "artifacts"
    synthetic_loans.to_csv(input_path, index=False)

    payload = run_training(input_path, artifacts_dir=artifacts_dir)

    assert set(payload["models"]) == {"logistic_regression", "random_forest"}
    for splits in payload["models"].values():
        for split_name in ("train", "validation", "test"):
            metrics = splits[split_name]
            assert metrics["samples"] > 0
            assert 0 <= metrics["base_rate"] <= 1
            if metrics["roc_auc"] is not None:
                assert 0 <= metrics["roc_auc"] <= 1

    assert payload["run"]["modeled_rows"] < payload["run"]["input_rows"]
    assert payload["run"]["excluded_status_counts"]

    for filename in (
        "model_metrics.json",
        "threshold_table.csv",
        "feature_effects.csv",
        "calibration.csv",
    ):
        assert (artifacts_dir / filename).exists()

    thresholds = pd.read_csv(artifacts_dir / "threshold_table.csv")
    assert len(thresholds) == 12  # 2 models x 2 evaluation splits x 3 thresholds

    effects = pd.read_csv(artifacts_dir / "feature_effects.csv")
    assert set(effects["model"]) == {"logistic_regression", "random_forest"}
    assert set(effects["kind"]) == {"coefficient", "importance"}


def test_training_is_deterministic_across_runs(
    tmp_path, synthetic_loans: pd.DataFrame
) -> None:
    input_path = tmp_path / "synthetic_clean.csv"
    synthetic_loans.to_csv(input_path, index=False)

    run_training(input_path, artifacts_dir=tmp_path / "first")
    run_training(input_path, artifacts_dir=tmp_path / "second")

    first = (tmp_path / "first" / "model_metrics.json").read_text(encoding="utf-8")
    second = (tmp_path / "second" / "model_metrics.json").read_text(encoding="utf-8")
    first_payload = json.loads(first)

    assert first == second
    assert first_payload["run"]["seed"] == 42


def test_run_training_rejects_tiny_datasets(tmp_path) -> None:
    from tests.conftest import make_synthetic_loans

    tiny = make_synthetic_loans(rows=60)
    input_path = tmp_path / "tiny.csv"
    tiny.to_csv(input_path, index=False)

    with pytest.raises(ValueError, match="terminal"):
        run_training(input_path, artifacts_dir=tmp_path / "artifacts")


def test_cli_fails_clearly_when_input_is_missing(tmp_path, capsys) -> None:
    exit_code = main(["--input", str(tmp_path / "missing.csv")])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "not bundled" in captured.err
    assert "data/raw" in captured.err
    assert "src/clean.py" in captured.err
