#!/usr/bin/env python3
"""
train_baseline.py — First ML model: volatility prediction (5m range).
Usage: python3 Scripts/train_baseline.py

Reads from Data/Features/, prints results + feature importance.
Temporal split: train 2024, validation Jan-Jun 2025, test Jul 2025+.
Comparison vs naive baseline (MA of range).

Prerequisites: pip3 install xgboost scikit-learn --break-system-packages
"""
import polars as pl
from pathlib import Path
from datetime import datetime
import gc

DATA_DIR = Path(__file__).parent.parent / "Data"    # Eagle/Data/
FEATURES_DIR = DATA_DIR / "Features"
REPORTS_DIR = DATA_DIR / "Reports"

SYMBOLS = ["GC", "NQ"]

# Feature columns (must match build_features.py)
FEATURE_COLS = [
    "delta_cum_3", "delta_cum_6", "delta_cum_12",
    "vol_20", "vol_100", "vol_regime",
    "range_ratio",
    "imbalance_6", "imbalance_20",
    "hour_sin", "hour_cos",
    "day_sin", "day_cos",
    "trade_intensity", "volume_ratio",
    "ret_lag1", "ret_lag2", "ret_lag3", "ret_lag5",
    "range_lag1", "range_lag2",
]

TARGET_COL = "target_range_next"

# Temporal split
TRAIN_END = datetime(2025, 1, 1)       # train: everything before 2025
VAL_END = datetime(2025, 7, 1)         # val: Jan-Jun 2025
                                         # test: Jul 2025+


def load_and_split(symbol):
    """Load features and split temporally."""
    path = FEATURES_DIR / f"{symbol}_5m_features.parquet"
    if not path.exists():
        print(f"  {symbol}: file not found")
        return None, None, None

    df = pl.read_parquet(path)
    print(f"  {symbol}: {df.shape[0]:,} rows loaded")

    # Temporal split
    train = df.filter(pl.col("datetime_utc") < TRAIN_END)
    val = df.filter(
        (pl.col("datetime_utc") >= TRAIN_END) &
        (pl.col("datetime_utc") < VAL_END)
    )
    test = df.filter(pl.col("datetime_utc") >= VAL_END)

    print(f"    Train: {train.shape[0]:,} ({train['datetime_utc'].min()} → {train['datetime_utc'].max()})")
    print(f"    Val  : {val.shape[0]:,} ({val['datetime_utc'].min()} → {val['datetime_utc'].max()})")
    print(f"    Test : {test.shape[0]:,} ({test['datetime_utc'].min()} → {test['datetime_utc'].max()})")

    return train, val, test


def to_numpy(df, features, target):
    """Convert a Polars DataFrame to numpy arrays for ML."""
    X = df.select(features).to_numpy()
    y = df.select(target).to_numpy().ravel()
    return X, y


def evaluate(y_true, y_pred, label):
    """Compute MAE, RMSE, R² and print."""
    import numpy as np
    mae = np.mean(np.abs(y_true - y_pred))
    rmse = np.sqrt(np.mean((y_true - y_pred) ** 2))
    ss_res = np.sum((y_true - y_pred) ** 2)
    ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0
    print(f"    {label:10s}  MAE {mae:>8.4f}  RMSE {rmse:>8.4f}  R² {r2:>7.4f}")
    return {"mae": mae, "rmse": rmse, "r2": r2}


def train_symbol(symbol):
    """Train and evaluate the model for one symbol."""
    import numpy as np

    print(f"\n{'═'*65}")
    print(f"  {symbol} — Baseline model (XGBoost → target_range_next)")
    print(f"{'═'*65}")

    # Load and split
    train_df, val_df, test_df = load_and_split(symbol)
    if train_df is None:
        return

    X_train, y_train = to_numpy(train_df, FEATURE_COLS, TARGET_COL)
    X_val, y_val = to_numpy(val_df, FEATURE_COLS, TARGET_COL)
    X_test, y_test = to_numpy(test_df, FEATURE_COLS, TARGET_COL)

    # ═══════════════════════════════════════════════════════════════
    # NAIVE BASELINE: predict the range as MA(20) of range
    # ═══════════════════════════════════════════════════════════════
    print(f"\n  ── Naive baseline (range_lag1 = prediction) ──")

    # Simplest: predict the previous range
    y_naive_val = val_df["range_lag1"].to_numpy()
    y_naive_test = test_df["range_lag1"].to_numpy()

    naive_val = evaluate(y_val, y_naive_val, "Val naive")
    naive_test = evaluate(y_test, y_naive_test, "Test naive")

    # ═══════════════════════════════════════════════════════════════
    # XGBOOST
    # ═══════════════════════════════════════════════════════════════
    print(f"\n  ── XGBoost ──")

    try:
        from xgboost import XGBRegressor
    except ImportError:
        print("  ⚠ xgboost not installed. Install: pip3 install xgboost --break-system-packages")
        return

    model = XGBRegressor(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=10,
        reg_alpha=0.1,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=-1,
        tree_method="hist",
    )

    # Early stopping on validation
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=False,
    )

    # Predictions
    y_pred_train = model.predict(X_train)
    y_pred_val = model.predict(X_val)
    y_pred_test = model.predict(X_test)

    print(f"    Trees used: {model.best_iteration if hasattr(model, 'best_iteration') and model.best_iteration else model.n_estimators}")

    xgb_train = evaluate(y_train, y_pred_train, "Train")
    xgb_val = evaluate(y_val, y_pred_val, "Val")
    xgb_test = evaluate(y_test, y_pred_test, "Test")

    # ═══════════════════════════════════════════════════════════════
    # IMPROVEMENT VS BASELINE
    # ═══════════════════════════════════════════════════════════════
    print(f"\n  ── Improvement vs naive ──")
    mae_improve_val = (1 - xgb_val["mae"] / naive_val["mae"]) * 100
    mae_improve_test = (1 - xgb_test["mae"] / naive_test["mae"]) * 100
    print(f"    Val : MAE {mae_improve_val:+.1f}% vs naive")
    print(f"    Test: MAE {mae_improve_test:+.1f}% vs naive")

    if xgb_test["r2"] > naive_test["r2"]:
        print(f"    ✓ XGBoost beats the naive baseline on the test set")
    else:
        print(f"    ✗ The naive baseline is better — the model adds no value")

    # ═══════════════════════════════════════════════════════════════
    # FEATURE IMPORTANCE
    # ═══════════════════════════════════════════════════════════════
    print(f"\n  ── Feature Importance (gain) ──")
    importances = model.feature_importances_
    feat_imp = sorted(zip(FEATURE_COLS, importances), key=lambda x: -x[1])

    for name, imp in feat_imp:
        bar = "█" * int(imp / max(importances) * 30)
        print(f"    {name:20s}  {imp:.4f}  {bar}")

    # ═══════════════════════════════════════════════════════════════
    # ERROR ANALYSIS
    # ═══════════════════════════════════════════════════════════════
    print(f"\n  ── Error analysis (test set) ──")

    errors = np.abs(y_test - y_pred_test)
    median_target = np.median(y_test)

    # Error by volatility regime
    low_vol = y_test < median_target * 0.5
    mid_vol = (y_test >= median_target * 0.5) & (y_test <= median_target * 2.0)
    high_vol = y_test > median_target * 2.0

    if low_vol.sum() > 0:
        print(f"    Low vol   (range < {median_target*0.5:.1f}) : "
              f"MAE {errors[low_vol].mean():.4f}  n={low_vol.sum():,}")
    if mid_vol.sum() > 0:
        print(f"    Normal vol                          : "
              f"MAE {errors[mid_vol].mean():.4f}  n={mid_vol.sum():,}")
    if high_vol.sum() > 0:
        print(f"    High vol  (range > {median_target*2.0:.1f}) : "
              f"MAE {errors[high_vol].mean():.4f}  n={high_vol.sum():,}")

    # Error by hour (approximation via hour_sin)
    print(f"\n    Error per hour (top 3 best / worst):")
    test_with_err = test_df.with_columns(
        pl.Series("error", errors),
        pl.col("datetime_utc").dt.hour().alias("hour"),
    )
    hourly_err = test_with_err.group_by("hour").agg([
        pl.col("error").mean().alias("mae"),
        pl.col("error").len().alias("n"),
    ]).sort("mae")

    best = hourly_err.head(3)
    worst = hourly_err.tail(3).sort("mae", descending=True)

    for row in best.iter_rows(named=True):
        print(f"    ✓ {row['hour']:02d}h: MAE {row['mae']:.4f}  (n={row['n']:,})")
    for row in worst.iter_rows(named=True):
        print(f"    ✗ {row['hour']:02d}h: MAE {row['mae']:.4f}  (n={row['n']:,})")

    del train_df, val_df, test_df, model
    gc.collect()

    return {
        "symbol": symbol,
        "naive_test_mae": naive_test["mae"],
        "xgb_test_mae": xgb_test["mae"],
        "xgb_test_r2": xgb_test["r2"],
        "improvement": mae_improve_test,
    }


def main():
    print("=" * 65)
    print("  EAGLE — Train Baseline (Volatility Prediction)")
    print("=" * 65)
    print(f"  Features: {len(FEATURE_COLS)}")
    print(f"  Target  : {TARGET_COL}")
    print(f"  Split   : train <2025 | val Jan-Jun 2025 | test Jul 2025+")

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    results = []
    for symbol in SYMBOLS:
        r = train_symbol(symbol)
        if r:
            results.append(r)
        gc.collect()

    # Final summary
    if results:
        print(f"\n{'═'*65}")
        print(f"  Summary")
        print(f"{'═'*65}")
        for r in results:
            print(f"  {r['symbol']}: XGBoost MAE {r['xgb_test_mae']:.4f} "
                  f"(naive {r['naive_test_mae']:.4f}) "
                  f"→ {r['improvement']:+.1f}%  R²={r['xgb_test_r2']:.4f}")

    print(f"\n  Done.")


if __name__ == "__main__":
    main()
