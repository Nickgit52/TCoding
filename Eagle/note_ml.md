# Eagle — ML Notes
*Last updated 2026-05-11*

---

## 1. What the data tells us

### Datasets
- GC: 59M ticks, 10 contracts (GCG24 → GCM26), 932 days, 351 MB
- NQ: 268M ticks, 10 contracts (NQH24 → NQM26), 901 days, 1.4 GB
- Pre-aggregated candles: 1m / 5m / 15m / 1h / 1d per symbol
- Roll: daily volume crossover, irreversible (8 GC rolls, 9 NQ rolls)

### Return autocorrelation
| Timeframe | GC lag1 | Interpretation | NQ lag1 | Interpretation |
|-----------|---------|----------------|---------|----------------|
| 1m | +0.002 | neutral | +0.007 | neutral |
| 5m | +0.002 | neutral | -0.001 | neutral |
| 15m | -0.005 | neutral | -0.002 | neutral |
| 1h | **-0.044** | **mean-reversion** | +0.013 | neutral |

GC on 1h shows tradable mean-reversion. NQ is neutral everywhere — efficient market on returns.

### Volatility
| Metric | GC | NQ |
|--------|----|----|
| Range autocorr 1h (lag1) | 0.178 | **0.659** |
| High vol (>1.5x median) | 25.6% | 20.9% |
| Low vol (<0.6x median) | 14.8% | 10.1% |

NQ has very strong volatility persistence — today's vol predicts tomorrow's. GC is more moderate. Predicting future volatility is likely more feasible than predicting direction.

### Price impact (delta → return)
| Metric | GC | NQ |
|--------|----|----|
| Delta-return correlation | **0.354** | 0.117 |
| Avg return on large buy (P95) | +1.20 | +2.35 |
| Avg return on large sell (P5) | -1.34 | -2.35 |

GC has a strong order flow signal — delta predicts return. NQ much less so, likely because the market is more liquid and absorbs orders better.

### Intraday seasonality
Activity peak 13-15h UTC for both instruments (US cash open). Volume and range 3-4x higher than the Asia period (04h UTC).

### Distribution tails (fat tails)
| Timeframe | GC kurtosis | NQ kurtosis |
|-----------|-------------|-------------|
| 1m | 308 | 120 |
| 5m | 126 | 79 |
| 1h | 112 | 44 |
| 1d | 41 | 14 |

Extreme kurtosis especially on 1m — many small moves and a few large ones. ML models must be robust to outliers.

### Skewness
GC: negative skew on all timeframes (-2.3 to -3.5 on 1m-1h). Drops are more violent than rises.
NQ: slightly positive skew (+0.17 to +0.67). Rallies are more violent than drops.

### Daily patterns
| Metric | GC | NQ |
|--------|----|----|
| % positive days | 54.2% | 55.5% |
| Best day | Fri (+0.16%) | Tue (+0.26%) |
| Max up streak | 9d | 10d |
| Max down streak | 10d | 7d |

---

## 2. ML features — Priority

### Tier 1 — Strong signal, ready to build

**order_flow_delta_cum** — Cumulative delta over N periods (5m, 15m)
- Why: 0.35 delta-return correlation on GC, the strongest available
- Compute: rolling sum of (ask_vol - bid_vol) over 5/15 candles

**vol_regime** — Short/long-term volatility ratio (rolling 20 / rolling 100)
- Why: 0.66 persistence on NQ, implicit mean-reversion on GC
- Compute: std(return, 20) / std(return, 100)

**hour_sin / hour_cos** — Cyclic encoding of UTC hour
- Why: massive intraday seasonality (3-4x between trough and peak)
- Compute: sin(2π × hour/24), cos(2π × hour/24)

**bid_ask_imbalance** — Rolling (ask_vol - bid_vol) / (ask_vol + bid_vol)
- Why: proxy for instantaneous directional pressure
- Compute: rolling ratio over 5-20 candles

### Tier 2 — Probable signal, to validate

**trade_intensity** — Number of trades per minute (or ticks/candle)
- Why: market acceleration = event in progress
- Compute: num_trades / candle duration, normalized by hour

**range_ratio** — Current range / MA(20) of range
- Why: expansion = potential breakout, contraction = accumulation
- Compute: (high - low) / MA(20)(high - low)

**large_trade_flag** — Proportion of large trades (>P95 volume) in the window
- Why: institutional activity, conviction signal
- Compute: count(vol > P95) / count(total) over rolling window

**day_of_week** — Cyclic encoding of day
- Why: GC best on Friday, NQ best on Tuesday
- Compute: sin(2π × day/5), cos(2π × day/5)

### Tier 3 — Exploratory

**vwap_deviation** — Price deviation from session VWAP
- Why: institutional reference level, potential mean-reversion
- Compute: (close - VWAP) / VWAP × 100

**ret_autocorr_rolling** — Rolling autocorrelation over N periods
- Why: detects regime changes (trend vs chop)
- Compute: corr(return, return.shift(1)) over a 50-100 period window

**inter_arrival_z** — Normalized inter-tick time
- Why: abnormal silence = dried liquidity, acceleration = event
- Compute: (inter_time / hourly_average) — z-score per hour

---

## 3. Prediction target

### Option A — Direction of next return (classification)
- Target: return_next > 0 (binary)
- Pro: simple, directly tradable
- Con: returns are very noisy (autocorr ~0), hard to beat

### Option B — Future volatility (regression)
- Target: |return_next| or range of the next candle
- Pro: strong persistence (especially NQ 0.66), more predictable
- Use: position sizing, entry timing

### Option C — Probability of large move (imbalanced classification)
- Target: |return_next| > 2 × median (binary)
- Pro: combines direction and volatility
- Con: rare class (~10-15%), requires imbalance techniques

**Recommendation: start with Option B (volatility)** — it's the strongest signal available, and it's useful even without predicting direction.

---

## 4. Model architecture

### Phase 1 — Baseline
- Model: XGBoost (fast, robust to outliers, handles mixed features well)
- Features: Tier 1 only
- Timeframe: 5m candles (good noise/signal trade-off)
- Target: range of the next 5m candle
- Split: train on 2024, validation on Jan-Jun 2025, test on Jul 2025+
- Metrics: MAE, R², comparison vs naive baseline (MA of range)

### Phase 2 — Expansion
- Add Tier 2 features
- Test 15m and 1h in addition to 5m
- Feature importance → prune what doesn't help
- Temporal cross-validation (walk-forward)

### Phase 3 — Production
- Pipeline: build_history → build_candles → build_features → predict
- Dashboard integration: display predicted volatility score
- Alerts when the model predicts a high-volatility regime

---

## 5. Pitfalls to avoid

- **Lookahead bias**: never use future data to compute a feature (e.g., full-session VWAP to predict mid-session)
- **Overfitting**: extreme kurtosis (300+) means a few large moves dominate — the model can memorize outliers
- **Roll jumps**: the 8-9 rolls create 1-2% price gaps — exclude transition candles or flag them
- **Regime change**: the 2024 market (GC at 2000) is not the 2026 market (GC at 5000) — features must be relative/normalized, not absolute
- **Transaction cost**: even a perfect 51% directional signal is useless if the spread eats the edge

---

## 6. Market Profile

### Day types
| Type | GC (645d) | NQ (635d) | Characteristic |
|------|-----------|-----------|----------------|
| Normal Var | 41.9% | 50.9% | moderate IB, one-sided extension |
| Trend | 31.9% | 23.3% | narrow IB, strong extension (>2.5× IB) |
| Non-Trend | 12.4% | 7.7% | very narrow total range |
| Normal | 9.9% | 6.6% | wide IB, little extension |
| Neutral | 3.9% | 11.5% | extensions on both sides |

GC is more directional (32% trend days vs 23% NQ). NQ is more structured and efficient.

### Initial Balance
- GC: median IB 12.3 pts (COMEX open 13h UTC), median RTH range 26.6 pts
- NQ: median IB 116 pts (equity open 14h UTC), median RTH range 250 pts
- Thursday stands out for NQ: 32.5% trend days vs ~20% on other days

### Naked POCs
A POC is "naked" as long as price has not crossed that level in a subsequent session.

| Metric | GC | NQ |
|--------|----|----|
| Still naked | 89 / 645 (14%) | 30 / 635 (5%) |
| Filled ≤1 day | 49% | 63% |
| Filled ≤5 days | 78% | 85% |
| Filled ≤20 days | 96% | 94% |
| Avg duration (filled) | 5d | 7d |
| Median duration (filled) | 2d | 1d |

NQ is more efficient — it revisits its POCs faster (63% within 1 day). Recent naked POCs (<20d) are the most useful as S/R levels. Very old ones (>500d) are fossils of obsolete prices (GC at $1865-1900, NQ at 14400-15500).

### ML implications
- Day type as a feature: IB range + first-30-minute volume allows predicting day type in real time
- Naked POCs close to current price are attraction levels (mean-reversion targets)
- VA width / day range ratio measures volume concentration — useful for the vol regime

---

## 7. Order Flow Regimes

9 regimes detected per 5m candle, based on rolling z-scores (1h fast, 3h slow):

| Regime | GC | NQ | Signal |
|--------|----|----|--------|
| Absorption | 0.8% | 0.2% | Invisible wall — large volume, price stuck, opposing delta |
| Compression | 17.6% | 10.2% | Range contracts, volume falls — loading spring |
| Distribution | 1.5% | 2.5% | Large volume + indecision (body < 30% of range) |
| Aggression | 1.6% | 3.2% | Delta + range + volume explode together |
| Exhaustion | 3.1% | 4.1% | Volume climax + immediate reversal |
| Iceberg | 9.5% | 0.5% | Repeated hits at same price — hidden order |
| Sweep | 0.3% | 0.3% | Crosses levels then reverses — stop hunt |
| Initiative | 9.7% | 13.6% | Trading outside Value Area with conviction (delta confirms) |
| Rotation | 12.7% | 11.0% | Many levels visited — liquidity search |

### Hourly patterns
- GC: Rotation at the open (13-14h), Compression in the afternoon (16h+), Iceberg at session end
- NQ: Initiative Buy/Sell throughout the day, Exhaustion at 19h UTC (15h ET)

### ML implications
- Regime as a categorical feature: a regime change (Compression → Aggression) is a breakout signal
- Absorption often precedes a reversal — exploitable binary feature
- Exhaustion combined with volume z-score could predict intraday tops/bottoms

---

## 8. Per-contract stats (to implement)

| Stat | Utility |
|------|---------|
| Bid total vs Ask total (lifetime) | Structural buyer/seller bias |
| Price traveled vs net displacement | Directional efficiency ratio |
| Frequency of large trades per month | Institutional activity trend |
| Largest single trade | Max institutional footprint |
| Record volume day | Context of extremes |

---

## 9. Environment / Setup

| Item | Detail |
|---|---|
| Machine | MacBook Air M1, 16 GB RAM |
| OS | macOS Tahoe 26.3.1 |
| Python | 3.14 via `.venv/bin/python3` (project-dedicated venv) |
| Dependencies | see `requirements.txt` at the root |
| Sierra Chart | Windows 11 via Parallels — volume `/Volumes/[C] Windows 11/SierraChart/Data/` |
| External drive | **Sam128** (Samsung 128 GB USB-C) must be connected |

### Data locations (Path B, 2026-05-12)

- **.scid input**: `find_scid()` order is Sierra Chart live (`/Volumes/[C] Windows 11/SierraChart/Data/`) → Pulse working copy (`Pulse/Data/Scid_Data/`) → TC archive (`/Volumes/Sam128/TC_Sam128/`). Scripts use absolute paths — no symlinks.
- **Parquet output**: `/Volumes/Sam128/TC_Sam128/Ticks_Parquet/` (was `Eagle/Data/Ticks_Parquet/` via symlink before Path B).
- **Sync ownership**: Pulse owns `.scid` syncing (`pulse_sync_scid.py`, alias `sync`). Eagle no longer copies from Sierra — it reads from the three locations above in order.
- **Sam128 dependency**: Sam128 must be mounted before any Eagle batch script. Live dashboard works without it (reads Candles + Features which are Mac-local).

### Startup

```
cd ~/Documents/Projets/TCoding/Eagle
.venv/bin/python3 Scripts/eagle_start.py
```

Flags: `--no-build` (skip rebuilding ticks), `--no-analysis` (skip market_profile + orderflow), `--dashboard` (launch eagle_server.py on localhost:8888), `--sync-only` (sync .scid only).

---

*Living document — last updated on 2026-05-11.*
