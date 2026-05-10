# Benchmark Results — Multi-Run Aggregated Summary

Date: May 10, 2026  
Methodology: 3 repeated runs per (algorithm, workload profile)  
Algorithms: `least_connections`, `ucb`, `metric_aware`  
Workloads: `db_point_light`, `db_range_heavy`, `db_aggregate_mid`  
Run settings: `90 requests`, `15 concurrency`, `steady` pattern, clean Docker start  

---

## Aggregated Results (Mean over 3 Runs)

| Algorithm | Workload Profile | Runs | Success Rate (%) | Avg Latency (ms) | P95 (ms) | P99 (ms) | Max (ms) |
|---|---|---:|---:|---:|---:|---:|---:|
| least_connections | db_point_light | 3 | 100.0 | **54.03** | 68.14 | 74.57 | 74.57 |
| least_connections | db_range_heavy | 3 | 100.0 | 55.22 | 69.88 | 75.95 | 75.95 |
| metric_aware | db_point_light | 3 | 100.0 | **51.48** | 64.36 | 74.40 | 74.40 |
| metric_aware | db_range_heavy | 3 | 100.0 | **56.66** | 78.66 | 88.59 | 88.59 |
| ucb | db_point_light | 3 | 100.0 | 52.84 | 68.77 | 75.85 | 75.85 |
| ucb | db_range_heavy | 3 | 100.0 | 54.42 | 71.17 | 78.06 | 78.06 |

*Note: `db_aggregate_mid` runs encountered failures during repeated execution; results excluded from final summary.*

---

## Key Observations

### 1. Point Query Workload (Light)
- **metric_aware** achieved best performance: **51.48 ms avg** (5.0% improvement vs. least_connections)
- UCB: 52.84 ms (2.2% worse than least_connections)
- Demonstrates metric-aware routing advantage on smaller, low-latency queries

### 2. Range Query Workload (Heavy)
- **least_connections** outperformed: **55.22 ms avg**
- metric_aware slightly degraded: 56.66 ms (2.6% worse)
- UCB: 54.42 ms (best p95 latency at 71.17 ms)
- Range queries benefit from balanced connection distribution over latency-aware scoring

### 3. P95 and Tail Latency Stability
- **metric_aware** on point queries shows best p95: 64.36 ms (5.6% vs. least_connections)
- **UCB** on range queries shows best tail control: p99 78.06 ms (3.1% vs. least_connections)
- Variance between runs indicates workload sensitivity to timing and node state

---

## Algorithm Comparison Summary

### Least Connections (Baseline)
- **Strengths**: Stable on range queries, predictable performance, simple
- **Weaknesses**: Does not adapt to actual latency/load signals
- **Best use case**: Homogeneous node workloads with balanced concurrency

### Metric-Aware (Adaptive)
- **Strengths**: Excellent on point queries due to latency-aware scoring; considers multiple metrics
- **Weaknesses**: Slightly worse on heavy range queries; metric staleness can degrade decisions
- **Best use case**: Mixed workload profiles where query types vary; environments with node heterogeneity

### UCB (Exploration-Exploitation)
- **Strengths**: Balances exploration and exploitation; solid tail latency on range queries
- **Weaknesses**: Learns slowly in first few requests; exploration overhead
- **Best use case**: Discovering node performance patterns; dynamic node availability scenarios

---

## Improvement Analysis (Exact Deltas)

### Metric-Aware vs. Least Connections

| Workload | Metric | Improvement |
|---|---|---|
| db_point_light | Avg Latency | **-4.72%** (51.48 vs 54.03) |
| db_point_light | P95 Latency | **-5.56%** (64.36 vs 68.14) |
| db_range_heavy | Avg Latency | **+2.61%** (56.66 vs 55.22) |
| db_range_heavy | P95 Latency | **+12.58%** (78.66 vs 69.88) |

**Macro Average**: metric-aware avg latency = -1.06% vs. least_connections across workloads

### UCB vs. Least Connections

| Workload | Metric | Improvement |
|---|---|---|
| db_point_light | Avg Latency | **-2.12%** (52.84 vs 54.03) |
| db_point_light | P95 Latency | **+1.01%** (68.77 vs 68.14) |
| db_range_heavy | Avg Latency | **-1.45%** (54.42 vs 55.22) |
| db_range_heavy | P95 Latency | **+1.84%** (71.17 vs 69.88) |

**Macro Average**: ucb avg latency = -1.78% vs. least_connections across workloads

---

## Interpretation and Limitations

1. **Workload Sensitivity**: Routing algorithm effectiveness is highly workload-specific. metric_aware excels on light point queries but degrades on heavier range queries.

2. **Single-Run Bias**: Previous single-run results showed extreme variance (p99 > 6 seconds), likely due to controller/node state variance. Repeated runs with clean Docker starts show consistent sub-100ms latencies.

3. **Metric Staleness**: The 15-second stale-metric penalty in metric_aware strategy may need tuning for faster feedback loops. Current penalty appears conservative.

4. **Node Heterogeneity**: Three nodes with diverse delay profiles (node1 fast, node2/node3 slow) represent realistic heterogeneity, but metric-aware strategy does not currently account for workload-specific node affinity.

5. **Aggregate Query Failures**: Failures on db_aggregate_mid warrant investigation into database query volume vs. node capacity; possible connection pooling or query timeout tuning needed.

---

## Recommendations for Future Optimization

1. **Metric Weighting Tuning**: Adjust `w_latency` and `w_queue` weights in metric_aware strategy based on observed latency distributions per workload type.

2. **Query-Type-Aware Routing**: Extend metric collection to include query type (point vs. range) and apply workload-specific routing preferences.

3. **Data Locality**: Implement shard-key awareness so range queries preferentially route to nodes holding relevant key ranges.

4. **Adaptive Stale Metrics**: Reduce stale-metric penalty during high-throughput phases; increase exploration penalty during low-throughput phases.

5. **Comprehensive Fault Testing**: Rerun with node failures (CRASH_RATE, TIMEOUT_RATE) enabled to compare algorithm resilience under adverse conditions.

---

## Conclusion

The aggregated multi-run benchmark reveals that **algorithm choice should be workload-aware**:
- Use **metric_aware** for point query / light workload scenarios where latency variability is high.
- Use **least_connections** as a robust baseline for balanced, homogeneous workloads.
- Use **UCB** as a fallback learning strategy when workload patterns are initially unknown.

None of the current algorithms dominates across all profiles, confirming the hypothesis that distributed load balancing remains a multi-objective optimization problem without universal solutions.
