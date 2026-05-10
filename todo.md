# Project TODO

## COMPLETED (DONE) ✅

### 1. Runtime Algorithm Configuration [DONE]
Status: Algorithm selection is configurable at runtime via `ALGORITHM` environment variable.
- Controller reads ALGORITHM and validates at startup.
- Client auto-syncs with active algorithm via `/stats` endpoint.

### 2. Real Workload Integration: DB Query [DONE]
Status: Fully implemented and benchmarked.
- Nodes execute SQLite-backed query workloads (point, range, aggregate).
- Three workload profiles defined: `db_point_light`, `db_range_heavy`, `db_aggregate_mid`.
- Controller forwards query parameters to nodes; client sends workload-profile-driven requests.

### 3. Metrics Expansion [DONE]
Status: Nodes push rich metrics to controller.
- Metrics include: latency_ms, task_type, db_query_ms, queue_depth, timestamp.
- Controller uses metrics for adaptive routing and stale-metric detection (15-second threshold).

### 4. Adaptive Routing: Metric-Aware Strategy [DONE]
Status: Fully implemented and benchmarked.
- Weighted scoring: active_connections, latency_ms, queue_depth, failure_history.
- Results: 5% improvement on light point queries; slightly worse on range queries.
- Trade-off: workload-specific tuning needed for production deployment.

### 5. Benchmark Automation Scripts [DONE]
Status: Fully implemented and tested.
- `scripts/benchmark_matrix.py`: Automates repeated runs (default 3x per algorithm/workload).
- `scripts/build_results_matrix.py`: Aggregates per-run CSVs into multi-run summary.
- `scripts/summarize_matrix.py`: Displays aggregated statistics for quick review.

### 6. Benchmark Results: Multi-Run Aggregation [DONE]
Status: 3 repeats per (algorithm, workload) completed; aggregated analysis in `results.md`.
- **Metric-Aware vs. Least Connections**: -4.7% avg latency on point queries (51.48 vs 54.03 ms).
- **UCB vs. Least Connections**: -1.78% avg latency across workloads.
- Consistent sub-100ms latencies in repeated runs (vs. 6+ second outliers in single-run mode).

### 7. Update Results Documentation [DONE]
Status: `results.md` updated with multi-run aggregated analysis.
- Aggregated results table (mean over 3 runs).
- Algorithm comparison summary and improvement analysis.
- Workload-specific insights and limitations.
- Recommendations for future optimization.

### 8. Update Submission Documentation [DONE]
Status: `submission.md` updated with complete status and design decisions.
- Marked all implemented components as "Implemented" or "Fully Implemented".
- Added design decisions and tradeoffs section.
- Listed achievements and limitations.
- Added future extension roadmap.

---

## NOT STARTED / DEFERRED

### Image Resize and Compression Workloads
Status: Deferred for future extension (not critical for research contribution).
Reason: DB-query workload sufficient for demonstrating load-balancer routing effectiveness; compute-bound tasks are natural extension.

### Inference Workload
Status: Deferred for future extension.
Reason: Complex to implement; compression is simpler next step if needed.

### Data Locality and Shard Awareness
Status: Not yet implemented (noted as future work).
Why: Requires workload parameter extensions (shard_key, range boundaries) and node-side shard metadata.

### Fault Injection Testing Matrix
Status: Infrastructure ready (CRASH_RATE, TIMEOUT_RATE env vars); comprehensive testing deferred.
Why: Current focus on baseline algorithm comparison; fault testing can be added as comparative benchmark dimension.

---

## FINAL PHASE

- [x] Re-run clean matrix with fixed repeat labels
- [x] Execute benchmark script with preserved repeats
- [x] Rebuild final aggregated CSV
- [x] Run build_results_matrix.py
- [x] Update results.md with aggregated stats
- [x] Mark TODO progress
- [ ] Tune metric_aware once (DECISION: Current tuning introduces timeouts; original weights are better)
- [ ] Docker compose default algorithm: confirm `metric_aware`
- [ ] Verify scripts run end-to-end from clean start
- [ ] Create final commit

---

## Summary

**Project Status**: Research prototype complete with three routing algorithms (Least Connections, UCB, Metric-Aware) benchmarked across DB-query workloads. Multi-run aggregated results show metric-aware strategy outperforms baseline on light queries; trade-off with range query performance indicates workload-aware algorithm selection is necessary.

**Deliverables**:
- Adaptive load balancer controller (FastAPI)
- Three comparison algorithms with runtime selection
- SQLite-backed DB workload simulator
- Automated benchmark suite with reproducible results
- Comprehensive documentation (submission.md, results.md)
- Clean architecture for future extension (compression, inference, data locality)

**Recommendations for Future**:
1. Implement compression workload as proof-of-concept
2. Add data-locality awareness for range queries
3. Comprehensive fault-injection testing
4. Production hardening and performance tuning

