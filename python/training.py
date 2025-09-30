#!/usr/bin/env python3
"""
training.py

Train a baseline classifier to predict next-30s direction using features built by features_build.py.

Inputs:
  - data/features.parquet (or data/features.csv)
  - models/feature_schema.json (from features_build.py)

Outputs:
  - models/model.pkl
  - reports/metrics.json
  - reports/feature_importance.json   (if tree model wins)
    or reports/coefs.json             (if logistic regression wins)
"""

from __future__ import annotations
import json
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
import joblib

DATA_DIR   = Path("data")
MODELS_DIR = Path("models")
REPORTS_DIR = Path("reports")
MODELS_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

RANDOM_STATE = 42
TEST_FRAC = 0.2  # final chronological holdout

# ---------------------- I/O helpers ----------------------

def _load_features() -> pd.DataFrame:
    p_parquet = DATA_DIR / "features.parquet"
    p_csv     = DATA_DIR / "features.csv"
    if p_parquet.exists():
        return pd.read_parquet(p_parquet)
    if p_csv.exists():
        return pd.read_csv(p_csv)
    raise FileNotFoundError("Expected data/features.parquet or data/features.csv")

def _load_schema() -> dict | None:
    p = MODELS_DIR / "feature_schema.json"
    if p.exists():
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def _chronological_split_index(df_meta: pd.DataFrame, test_frac: float) -> Tuple[pd.Index, pd.Index]:
    """
    df_meta must be sorted by ['symbol','bucket_ms'] before calling.
    Returns train_index, test_index (original positions aligned to the passed df_meta).
    """
    n = len(df_meta)
    n_test = max(1, int(np.floor(n * test_frac)))
    test_idx = df_meta.index[-n_test:]
    train_idx = df_meta.index[:-n_test]
    return train_idx, test_idx

def _safe_auc(y_true: np.ndarray, proba: np.ndarray) -> float:
    try:
        # AUC requires at least two classes present
        if len(np.unique(y_true)) < 2:
            return float("nan")
        return float(roc_auc_score(y_true, proba))
    except Exception:
        return float("nan")

# ---------------------- main ----------------------

def main() -> None:
    # Load data
    df = _load_features()
    if "direction_next_30s" not in df.columns:
        raise ValueError("Label 'direction_next_30s' not found in features file")

    # Use schema’s feature list if available, otherwise fall back to numeric columns except label & keys
    schema = _load_schema()
    if schema and "feature_cols" in schema:
        feature_cols: List[str] = [c for c in schema["feature_cols"] if c in df.columns]
    else:
        blacklist = {"symbol", "bucket_ms", "direction_next_30s"}
        feature_cols = [c for c in df.columns if c not in blacklist and pd.api.types.is_numeric_dtype(df[c])]

    if not feature_cols:
        raise ValueError("No feature columns found to train on.")

    # Binary target: up move = 1 if direction_next_30s > 0 else 0
    df = df.sort_values(["symbol", "bucket_ms"]).reset_index(drop=True)
    df["y"] = (df["direction_next_30s"] > 0).astype(np.int8)

    # Keep only rows with all features present
    mask = df[feature_cols].notna().all(axis=1) & df["y"].notna()
    df = df.loc[mask].reset_index(drop=True)

    if len(df) < 10:
        print(f"[training] WARNING: very small dataset (n={len(df)}). Results may be unstable.")

    # Chronological split (leakage-safe)
    meta = df[["symbol", "bucket_ms"]].copy()
    train_idx, test_idx = _chronological_split_index(meta, TEST_FRAC)
    X_train = df.loc[train_idx, feature_cols].copy()
    y_train = df.loc[train_idx, "y"].astype(int).copy()
    X_test  = df.loc[test_idx,  feature_cols].copy()
    y_test  = df.loc[test_idx,  "y"].astype(int).copy()

    # Build models
    lr_pipe = Pipeline(steps=[
        ("scale", StandardScaler()),
        ("clf", LogisticRegression(max_iter=2000, class_weight="balanced", solver="lbfgs", random_state=RANDOM_STATE))
    ])

    gb_pipe = Pipeline(steps=[
        ("clf", GradientBoostingClassifier(random_state=RANDOM_STATE))
    ])

    # Fit
    lr_pipe.fit(X_train, y_train)
    gb_pipe.fit(X_train, y_train)

    # Evaluate
    results = []
    for name, model in [("log_reg", lr_pipe), ("grad_boost", gb_pipe)]:
        if hasattr(model, "predict_proba"):
            proba = model.predict_proba(X_test)[:, 1]
        else:
            # decision_function fallback scaled to [0,1]
            raw = model.decision_function(X_test)
            mm = MinMaxScaler()
            proba = mm.fit_transform(raw.reshape(-1, 1)).ravel()
        auc = _safe_auc(y_test.to_numpy(), proba)
        # Derive class predictions at 0.5
        y_pred = (proba >= 0.5).astype(int)
        acc = float(accuracy_score(y_test, y_pred)) if len(y_test) else float("nan")
        f1  = float(f1_score(y_test, y_pred))       if len(y_test) else float("nan")
        results.append({
            "name": name,
            "model": model,
            "proba": proba,
            "auc": auc,
            "acc": acc,
            "f1": f1
        })

    # Pick best by AUC (fallback to accuracy if AUC NaN)
    best = None
    best_key = -np.inf
    for r in results:
        key = r["auc"]
        if np.isnan(key):
            # fallback to accuracy if auc is nan
            key = r["acc"]
        if key > best_key:
            best_key = key
            best = r
    assert best is not None
    best_name = best["name"]
    best_model = best["model"]
    proba = best["proba"]
    auc = best["auc"]
    acc = best["acc"]
    f1 = best["f1"]

    # Persist model
    model_path = MODELS_DIR / "model.pkl"
    joblib.dump(best_model, model_path)

    # Persist metrics
    metrics = {
        "chosen_model": best_name,
        "n_rows": int(len(df)),
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test)),
        "feature_count": int(len(feature_cols)),
        "roc_auc": float(auc) if not np.isnan(auc) else None,
        "accuracy": float(acc),
        "f1": float(f1),
        "test_pos_rate": float(y_test.mean()) if len(y_test) else None
    }
    with open(REPORTS_DIR / "metrics.json", "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    # Persist simple explainers
    if best_name == "grad_boost":
        # Feature importance
        clf = best_model.named_steps["clf"]
        importances = clf.feature_importances_.tolist()
        fi = {"features": feature_cols, "importances": importances}
        with open(REPORTS_DIR / "feature_importance.json", "w", encoding="utf-8") as f:
            json.dump(fi, f, indent=2)
    else:
        # Logistic regression coefficients (after StandardScaler)
        clf = best_model.named_steps["clf"]
        coefs = clf.coef_.ravel().tolist()
        lr_out = {"features": feature_cols, "coefs": coefs, "intercept": float(clf.intercept_[0])}
        with open(REPORTS_DIR / "coefs.json", "w", encoding="utf-8") as f:
            json.dump(lr_out, f, indent=2)

    print(
        f"[training] model={best_name}  "
        f"n_train={len(X_train)}  n_test={len(X_test)}  "
        f"acc={acc:.3f}  f1={f1:.3f}  auc={(auc if not np.isnan(auc) else float('nan')):.3f}"
    )


if __name__ == "__main__":
    main()
