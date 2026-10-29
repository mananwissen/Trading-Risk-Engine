# Real-Time AI Trading Risk Engine — Complete Local POC

## 1. Purpose and boundaries
This is a learning/portfolio proof of concept for a risk-control service placed between a hypothetical strategy and a hypothetical execution system. It uses synthetic data and simulated fills only. It does **not** connect to a broker/exchange, submit live orders, manage real money, or provide investment advice.

The core principle is **deterministic risk rules first, ML anomaly detection second**. A model can flag an unusual order; it cannot override a hard risk-limit breach.

## 2. End-to-end flow
Synthetic CSV/Excel or a single API request → request/schema validation → deterministic risk checks → anomaly score/flag → decision and reason → SQLite order/audit persistence → optional simulated fill/cancel → portfolio view and stress test. Optional Kafka publishes a decision event for a separate consumer; Kafka is not required for local use.

## 3. Stack: what each component does
- **Python**: core application and risk logic.
- **FastAPI**: REST API layer and interactive OpenAPI/Swagger UI at `/docs`.
- **Pydantic**: validates request shapes and types before risk logic runs.
- **pandas**: reads CSV/Excel batch inputs and creates batch results.
- **SQLite**: local file-based relational database; no separate database server needed.
- **SQL**: query audit/history and summarize rejections, exposure, and decisions.
- **NumPy + scikit-learn Isolation Forest**: illustrative unsupervised anomaly flag trained on synthetic baseline data.
- **pytest**: automated behavior checks.
- **Kafka (optional)**: event-streaming extension for consumers/replay; local Docker Compose config is provided.
- **Docker Compose (optional)**: starts Kafka-related local services when Docker Desktop is installed.

## 4. Windows local setup
Open PowerShell in this project folder:
```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
uvicorn app.main:app --reload
```
Open `http://127.0.0.1:8000/docs`. If PowerShell blocks activation, use Command Prompt and run `.venv\Scripts\activate.bat`, then install and start the app. Stop the server with Ctrl+C.

Run automated tests:
```powershell
pytest -q
```
Current result in this package: **5 passed**. This tests selected paths, not production readiness.

## 5. API tour
- `GET /health` — service health check.
- `GET /risk-config` — current configured risk thresholds.
- `GET /portfolio` — current local simulated portfolio and exposure view.
- `POST /orders/evaluate` — validate and evaluate one synthetic order.
- `GET /orders` — list order decisions.
- `GET /orders/{order_db_id}` — inspect one stored order.
- `POST /orders/batch-evaluate` — upload a CSV or Excel workbook for batch evaluation.
- `GET /batches/{batch_id}/results.csv` — download the result file for a batch.
- `POST /orders/{order_db_id}/simulate-fill` — simulate a fill for an eligible order and update the local portfolio.
- `POST /orders/{order_db_id}/cancel` — cancel an eligible simulated order.
- `POST /stress-test` — estimate impact under specified price shocks for the simplified spot portfolio.
- `POST /demo/set-daily-pnl` — demo-only helper to set daily P&L state for testing the daily-loss rule.
- `GET /audit` — inspect recorded audit events.
- `POST /demo/reset` — reset demo state; use only when you intend to clear local demo data.

Exact request bodies and response schemas are available in Swagger UI at `/docs`.

## 6. CSV/Excel inputs
Batch input must include columns `symbol`, `side`, `quantity`, and `price`. `order_id` is optional. `side` should be BUY or SELL. Use:
- `data/orders_sample.csv`
- `data/orders_sample.xlsx`

The portfolio and market-price CSVs are reference/demo datasets in this version; they are not automatically ingested by the batch endpoint. The batch endpoint processes orders. Workbook reading depends on the installed pandas Excel engine.

## 7. Risk checks explained
1. **Order value**: `quantity × price`; reject if a single order exceeds the configured order-value cap.
2. **Position value**: estimate post-order value for the symbol and compare with the per-instrument cap. In this simplified POC, accounting is not a complete trading ledger.
3. **Gross exposure**: sum of absolute position market values. It measures total risk-bearing position size without long/short netting.
4. **Leverage**: configured exposure measure divided by account equity. A production definition must explicitly state whether gross, net, or margin-based exposure is used.
5. **Daily loss**: blocks risk-increasing orders after a configured daily-loss threshold. Here daily P&L is set by a demo endpoint rather than calculated from fills and market prices.
6. **Order rate**: limits rapid order submissions to help detect accidental loops or bursts.
7. **ML anomaly flag**: Isolation Forest flags orders that differ from a synthetic baseline. This score is illustrative and is not calibrated on real order flow.
8. **Stress test**: estimates portfolio impact under user-specified spot-price shocks; this is a simplified scenario calculation, not a full derivatives risk engine.

A suspicious order is not automatically invalid. A real policy must define whether an anomaly means allow, flag, hold for review, or reject. Hard risk limits always take precedence over the ML result.

## 8. SQL and persistence
SQLite stores local order decisions, positions, audit events, application state, and batch results. It is useful for learning SQL without running a separate server. The database is local to the app and is not intended for multi-instance production use.

Example: rejected orders by symbol:
```sql
SELECT symbol, COUNT(*) AS rejected_count,
       SUM(order_value) AS rejected_value
FROM orders
WHERE status = 'REJECTED'
GROUP BY symbol
ORDER BY rejected_value DESC;
```

Example: decision distribution:
```sql
SELECT status, COUNT(*) AS order_count
FROM orders
GROUP BY status
ORDER BY order_count DESC;
```

Example: recent audit trail:
```sql
SELECT *
FROM audit_events
ORDER BY created_at DESC
LIMIT 50;
```
Table names/columns are implementation details and can evolve; inspect `app/storage.py` before building external queries against them.

## 9. Simulated fill vs approval
An **approved** order is only a risk decision. It is not an execution. The POC updates positions only when the simulated-fill endpoint is called for an eligible order. In real systems, the execution venue can partially fill, reject, or delay an order; portfolio state must be reconciled to actual fills.

## 10. Optional Kafka
The application works without Kafka. Kafka can later carry decision events to independent consumers such as monitoring, audit export, or dashboards. To try the optional setup, install Docker Desktop, run `docker compose up -d`, set `KAFKA_BOOTSTRAP_SERVERS=localhost:9092` in `.env`, and restart the API.

The included publisher is best-effort: event publication is not transactionally coupled to the database decision. Production reliability would require an outbox pattern, retries, dead-letter handling, event schemas, idempotent consumers, and operational monitoring.

## 11. Suggested demonstration sequence
1. Start the app and open `/docs`.
2. Call `GET /health`, `GET /risk-config`, and `GET /portfolio`.
3. Evaluate a small order with `POST /orders/evaluate`.
4. Evaluate an oversized order and inspect the rejection reason.
5. Upload `data/orders_sample.csv` and `data/orders_sample.xlsx` with `POST /orders/batch-evaluate`.
6. Download the batch CSV result using the returned batch ID/path.
7. Simulate a fill for an eligible approved order, then compare `/portfolio` before and after.
8. Run a stress test and inspect `/audit`.
9. Query the SQLite tables with SQL.
10. Only after the basic flow works, optionally enable Kafka.

## 12. Known limitations and production gaps
- Synthetic input only; no live market-data feed or broker/exchange integration.
- Not an investment or trading recommendation.
- SQLite and local synchronization are appropriate for a single-process demo, not a distributed concurrent trading system.
- No robust reservation/locking design to prevent two concurrent orders from both passing against the same remaining capacity.
- Daily P&L is injected for demonstration rather than calculated from realized/unrealized P&L, fees, corporate actions, and a defined reset boundary.
- The anomaly model is synthetic, uncalibrated, and may generate false positives/negatives.
- Stress testing is simplified and spot-position oriented; derivatives need notional, delta, Greeks, margin, and contract-specific logic.
- No production authentication/authorization, secrets management, deployment hardening, monitoring, alerting, disaster recovery, or full execution reconciliation.
- Kafka publication is best-effort and not an atomic part of the database transaction.
- Sample portfolio and market-price CSVs are reference datasets; automatic ingestion/seeding is a future extension.

## 13. Sensible next steps
1. Make risk rules and configuration explicit, versioned, and testable.
2. Add idempotency keys and atomic capacity reservations.
3. Calculate P&L from simulated fills and marked prices.
4. Add data freshness checks and fail-safe behavior for missing/stale state.
5. Add reconciliation for simulated fills, partial fills, cancels, and rejected executions.
6. Move from SQLite to PostgreSQL when multi-user or multi-process access is needed.
7. Add Kafka only when asynchronous event consumers/replay are useful.
8. Add dashboard, authentication, metrics, and deployment controls after the core engine is tested.

## 14. Disclaimer
This project is a local educational proof of concept. Do not connect it to live trading or use it to make financial decisions without substantial redesign, independent validation, security review, operational controls, and regulatory/compliance review.
