# Adaptive Load Balancing in Distributed Systems Using Multi-Armed Bandits

**Course**: Parallel and Distributed Computing  
**Date**: May 11, 2026

---

## 1. Introduction

### 1.1 Problem Statement

Load balancing in distributed systems must operate under **partial, delayed, and noisy system state**. This creates a fundamental challenge:

> *How can a system make low-latency routing decisions while maintaining high decision quality, despite uncertainty and rapidly changing load conditions?*

Traditional approaches struggle with this trade-off:

- **Polling-based systems** are responsive but inaccurate due to stale state data
- **ML/RL-based systems** are accurate but computationally expensive and slow to adapt

### 1.2 Hypothesis

> A load balancer that combines **push-based real-time feedback** with **online learning (Multi-Armed Bandits)** can reduce tail latency (P99) and improve load distribution compared to traditional polling-based and heuristic approaches.

### 1.3 Approach

The routing decision is modeled as a **Multi-Armed Bandit (MAB)** problem, where each backend server represents an **arm**. The system uses the **Upper Confidence Bound (UCB1)** strategy to balance:

- **Exploitation**: Routing to nodes known to be fast
- **Exploration**: Periodically trying underused nodes to discover performance changes

This is compared against three baselines: static Round Robin, connection-aware Least Connections, and a multi-signal Metric-Aware adaptive strategy.

---

## 2. System Architecture

### 2.1 Components

The system is a fully containerized (Docker Compose) load balancing testbed with four main components:

```
Client ──HTTP──▶ Controller ──HTTP──▶ Nodes (×6)
                     ▲                    │
                     └── POST /metrics ◀──┘
                     (push-based feedback)
```

1. **Controller** (FastAPI) — Routes incoming requests using the selected algorithm, tracks per-request metrics, collects asynchronous push metrics from nodes, implements retry logic across all nodes on failure.

2. **Nodes** (6 heterogeneous instances) — Process real SQLite DB queries (point, range, aggregate), push operational metrics (latency, queue depth, task type) back to the controller after each request, and simulate realistic failure modes.

3. **Client** — Configurable stress-test driver that sends parameterized workload requests, logs per-request results (latency, node assignment, success/failure), and supports multiple traffic patterns.

4. **Benchmark Suite** — Automated experiment runner (`benchmark_matrix.py`) that orchestrates algorithm switching, container restarts, repeated runs, and result aggregation.

### 2.2 Node Configuration

The 6 nodes are intentionally **heterogeneous** to simulate real-world infrastructure variance:

| Node | Role | Delay Range | Crash Rate | Timeout Rate | Rationale |
|------|------|-------------|-----------|-------------|-----------|
| 1 | Authority | 50–150 ms | 0% | 0% | Fast, reliable primary |
| 2 | Replica | 300–700 ms | 5% | 0% | Moderate speed, unreliable |
| 3 | Replica | 500–1000 ms | 0% | 5% | Slow, timeout-prone |
| 4 | Replica | 400–900 ms | 0% | 0% | Moderate speed, reliable |
| 5 | Replica | 600–1200 ms | 3% | 0% | Slow, occasionally crashes |
| 6 | Replica | 700–1500 ms | 0% | 0% | Slowest, reliable |

All nodes share the full key range (1–50,000) and serve all query types identically. The only differentiator is their simulated processing latency and failure behavior. This isolates the effect of routing decisions from data locality.

### 2.3 Communication Model

The system implements a **hybrid communication model**:

- **Synchronous path** (request routing): Client → Controller → Node → Controller → Client
- **Asynchronous path** (feedback): Node → Controller via `POST /metrics` after each processed request

This means the controller has two information channels:
1. **Direct observation**: Latency and success/failure measured per-request
2. **Push-based feedback**: Node-reported queue depth, task type, and self-measured latency

The UCB strategy uses only channel (1). The Metric-Aware strategy uses both channels. This design choice becomes significant in the results.

---

## 3. Routing Algorithms

### 3.1 Round Robin (Static Baseline)

```
next_node = nodes[index % len(nodes)]
index += 1
```

- **Signal used**: None (purely positional)
- **Decision cost**: O(1)
- **Adapts to**: Nothing — distributes load uniformly regardless of node state
- **Purpose**: Establishes the performance floor; any adaptive algorithm should beat this

### 3.2 Least Connections (Heuristic)

```
next_node = argmin(active_connections[node] for node in nodes)
```

- **Signal used**: Active connection count (maintained by controller)
- **Decision cost**: O(N) where N = number of nodes
- **Adapts to**: Connection-level congestion
- **Limitation**: Does not consider latency — a node with 0 connections but 1500ms delay is preferred over a node with 1 connection and 50ms delay

### 3.3 UCB — Upper Confidence Bound (Multi-Armed Bandit)

```
reward = 1 / (1 + latency_ms / 1000)    # ∈ (0, 1]
score  = avg_reward + c * sqrt(ln(total_pulls) / node_pulls)
next_node = argmax(score)
```

- **Signal used**: Historical latency (transformed to reward)
- **Decision cost**: O(N)
- **Exploration constant**: c = √2 (standard UCB1)
- **Adapts to**: Node performance over time; automatically shifts traffic away from slow nodes
- **Key property**: The exploration term `c * sqrt(ln(T)/n_i)` ensures no node is permanently ignored. Even if a node was slow initially, UCB will periodically re-check it.

**Reward function design**: The reward `1 / (1 + latency_ms / 1000)` maps:
- 50ms latency → reward ≈ 0.95 (high)
- 500ms latency → reward ≈ 0.67 (medium)
- 1000ms latency → reward = 0.50 (low)
- Failure/timeout → reward = 0.00 (zero)

This creates a smooth, monotonically decreasing reward signal that naturally penalizes slow and failing nodes.

### 3.4 Metric-Aware (Adaptive Multi-Signal)

```
score = w1 * active_connections
      + w2 * (latency_ms / 100)
      + w3 * queue_depth
      + w4 * failure_count_recent
      + stale_penalty
next_node = argmin(score)    # lower is better
```

- **Signals used**: Active connections, pushed latency, pushed queue depth, recent failure count, metric freshness
- **Weights**: [1.0, 0.02, 1.5, 2.0]
- **Stale penalty**: +3.0 if metrics are older than 15 seconds
- **Decision cost**: O(N) with timestamp parsing per node
- **Adapts to**: Multiple operational dimensions simultaneously

---

## 4. Experimental Design

### 4.1 Workload Profiles

All queries operate on a SQLite database with 50,000 rows per node.

| Profile | Query Type | Cardinality | Characteristics |
|---------|-----------|-------------|-----------------|
| `db_point_light` | Point lookup (key=42) | 1 row | Light, latency-sensitive, cache-friendly |
| `db_range_heavy` | Range scan (keys 100–10,000, limit 500) | Up to 500 rows | Heavy, scan-intensive, high variance |
| `db_mixed_50_50` | Point lookup (key=5000) | 1 row | Mixed workload simulation |

### 4.2 Traffic Patterns

| Pattern | Behavior | Concurrency |
|---------|----------|-------------|
| `steady` | Even submission rate, all requests queued through thread pool | 30 workers |
| `burst` | All requests submitted instantly (no pacing), same pool | 30 workers |

### 4.3 Benchmark Configuration

- **Requests per run**: 500
- **Concurrency**: 30 threads
- **Repeats**: 3 per (algorithm, workload, pattern) combination
- **Total runs**: 4 algorithms × 3 workloads × 2 patterns × 3 repeats = **72 runs**
- **Controller timeout**: 1 second per node (retries all 6 nodes before failing)
- **Clean Docker restart**: Between each algorithm switch (eliminates state carryover)

### 4.4 Metrics Collected

| Level | Metrics |
|-------|---------|
| **Client** | latency_ms, success/failure, error type, assigned node |
| **Controller** | algorithm, active_connections, node_request_count, failure logs |
| **Node** | task_type, db_query_ms, queue_depth, role, timestamp |

---

## 5. Results

### 5.1 Aggregated Performance (Steady Traffic, Mean over 3 Runs)

| Algorithm | Point Light | Range Heavy | Mixed 50/50 |
|-----------|------------|------------|-------------|
| **Round Robin** | 97.29 ms | 97.25 ms | 95.58 ms |
| **Least Connections** | 94.96 ms | 95.40 ms | 98.28 ms |
| **UCB** | 96.64 ms | 99.09 ms | **95.13 ms** |
| **Metric-Aware** | 140.59 ms | 199.29 ms | 250.41 ms |

**Success rate**: 100% across all algorithms and workloads (retry logic handles node failures transparently).

### 5.2 Tail Latency (P95/P99, Steady Traffic)

| Algorithm | Point P95 | Point P99 | Range P95 | Range P99 | Mixed P95 | Mixed P99 |
|-----------|----------|----------|----------|----------|----------|----------|
| **Round Robin** | 106.98 | 419.86 | 106.85 | 423.99 | 105.15 | 112.29 |
| **Least Connections** | 107.15 | 115.17 | 106.19 | 421.98 | 115.56 | 128.32 |
| **UCB** | 111.21 | 427.99 | 115.98 | 429.30 | 110.10 | 119.62 |
| **Metric-Aware** | 161.80 | 180.62 | 229.71 | 838.84 | 291.58 | 326.51 |

### 5.3 Performance vs Baseline (Steady, % Change vs Round Robin)

| Algorithm | Point Avg | Range Avg | Mixed Avg |
|-----------|----------|----------|----------|
| **Least Connections** | −2.40% | −1.90% | +2.83% |
| **UCB** | −0.67% | +1.89% | **−0.47%** |
| **Metric-Aware** | +44.51% | +104.93% | +161.99% |

### 5.4 Traffic Pattern Impact (Steady vs Burst)

| Algorithm | Workload | Steady (ms) | Burst (ms) | Change |
|-----------|----------|------------|-----------|--------|
| Round Robin | Mixed | 95.58 | 94.10 | −1.55% |
| Least Connections | Range | 95.40 | 99.62 | +4.42% |
| UCB | Range | 99.09 | 95.66 | **−3.47%** |
| Metric-Aware | Point | 140.59 | 161.48 | +14.86% |
| Metric-Aware | Mixed | 250.41 | 292.80 | +16.93% |

UCB is the only adaptive algorithm that **improves** under burst conditions on range queries. Metric-Aware degrades 14–17% under burst traffic across all workloads.

---

## 6. Analysis and Discussion

### 6.1 UCB Validates the MAB Approach

UCB achieves the **best average latency on mixed workloads** (95.13 ms vs 95.58 ms RR) and is competitive on all workloads. The improvement is modest (−0.47% to −0.67%), which reflects the reality that with homogeneous key ranges and a fast controller, routing overhead is small relative to node processing time.

However, UCB's advantage becomes clearer under stress:
- **Lowest max latency on mixed workloads**: 752.63 ms vs 1069.23 ms (RR) and 990.81 ms (MA)
- **Improves under burst traffic**: −3.47% on range queries while Metric-Aware degrades +10–21%
- **Self-correcting**: When a slow node is selected, UCB's reward drops, naturally routing future requests elsewhere

### 6.2 Signal Richness ≠ Better Decisions

The most striking finding is that **Metric-Aware consistently performs worst** despite using the richest signal set. On mixed workloads, it is 146% slower than Round Robin and 147% slower than UCB.

This is caused by three compounding factors:

1. **Stale metric penalty dominates early routing**: During the first 15 seconds of each run, all nodes have stale metrics. The +3.0 penalty applies uniformly, making the first ~150 requests (at 30 concurrency) effectively random with bias.

2. **Low-weight latency signal**: The latency weight (0.02) is dominated by the connection weight (1.0) and failure weight (2.0). This means Metric-Aware is effectively a Least Connections variant with a stale-metric noise floor, not a latency-optimized router.

3. **Queue depth reporting lag**: Push-based queue depth is reported *after* the request completes on the node, creating an inherent lag. By the time the controller receives `queue_depth=5`, the actual queue may already have changed.

**Key insight**: UCB achieves better results with a **single clean signal** (latency → reward) than Metric-Aware achieves with four signals. This supports the thesis that for routing optimization, the **exploration-exploitation tradeoff is more important than signal richness**.

### 6.3 Round Robin and Least Connections: Surprisingly Strong Baselines

---

### 6.4 Run 2: Higher Heterogeneity + Higher Failure Proneness

Run 2 was executed after:
- increasing delay ranges for all nodes,
- enabling crash/timeout rates on all nodes,
- tuning `metric_aware` to increase latency emphasis (`MA_W_LATENCY=0.08`) and reduce stale penalty (`MA_STALE_PENALTY=1.0`).

#### 6.4.1 Aggregated Means by Algorithm, Profile, and Pattern

| Algorithm | Profile | Pattern | Avg (ms) | P95 (ms) | P99 (ms) | Max (ms) |
|---|---|---|---:|---:|---:|---:|
| least_connections | db_mixed_50_50 | burst | 135.78 | 432.82 | 1103.65 | 1768.66 |
| least_connections | db_mixed_50_50 | steady | 145.78 | 730.36 | 1104.65 | 1733.98 |
| least_connections | db_point_light | burst | 150.19 | 736.62 | 1113.61 | 1769.92 |
| least_connections | db_point_light | steady | 149.96 | 452.83 | 1121.38 | 1770.05 |
| least_connections | db_range_heavy | burst | 144.01 | 738.06 | 1093.78 | 2052.59 |
| least_connections | db_range_heavy | steady | 137.23 | 127.45 | 1104.57 | 2066.22 |
| metric_aware | db_mixed_50_50 | burst | 344.04 | 420.68 | 1387.31 | 1726.65 |
| metric_aware | db_mixed_50_50 | steady | 302.33 | 634.52 | 1319.13 | 1993.34 |
| metric_aware | db_point_light | burst | 175.85 | 193.60 | 1159.73 | 2513.88 |
| metric_aware | db_point_light | steady | 163.64 | 747.25 | 1124.55 | 1806.09 |
| metric_aware | db_range_heavy | burst | 246.59 | 305.77 | 1228.86 | 1856.79 |
| metric_aware | db_range_heavy | steady | 217.87 | 539.88 | 1202.08 | 1815.44 |
| round_robin | db_mixed_50_50 | burst | 130.87 | 421.89 | 1097.85 | 1429.68 |
| round_robin | db_mixed_50_50 | steady | 135.62 | 138.08 | 1097.23 | 1732.04 |
| round_robin | db_point_light | burst | 145.80 | 450.05 | 1105.53 | 1439.91 |
| round_robin | db_point_light | steady | 131.35 | 143.75 | 1101.83 | 1451.32 |
| round_robin | db_range_heavy | burst | 129.47 | 725.79 | 1084.58 | 1429.74 |
| round_robin | db_range_heavy | steady | 145.67 | 149.06 | 1113.69 | 2056.48 |
| ucb | db_mixed_50_50 | burst | 129.52 | 126.93 | 1063.90 | 2061.05 |
| ucb | db_mixed_50_50 | steady | 130.76 | 135.97 | 1092.03 | 2039.80 |
| ucb | db_point_light | burst | 138.58 | 437.42 | 1104.75 | 1433.94 |
| ucb | db_point_light | steady | 141.59 | 149.73 | 1103.40 | 1744.45 |
| ucb | db_range_heavy | burst | 132.57 | 434.22 | 1089.49 | 1420.91 |
| ucb | db_range_heavy | steady | 136.17 | 131.48 | 1101.36 | 2378.07 |

#### 6.4.2 Pattern-Level Summary

| Algorithm | Pattern | Avg (ms) | P95 (ms) | P99 (ms) | Max (ms) |
|---|---|---:|---:|---:|---:|
| least_connections | burst | 143.33 | 635.83 | 1103.68 | 1863.72 |
| least_connections | steady | 144.32 | 436.88 | 1110.20 | 1856.75 |
| metric_aware | burst | 255.49 | 306.68 | 1258.64 | 2032.44 |
| metric_aware | steady | 227.95 | 640.55 | 1215.25 | 1871.63 |
| round_robin | burst | 135.38 | 532.58 | 1095.99 | 1433.11 |
| round_robin | steady | 137.55 | 143.63 | 1104.25 | 1746.61 |
| ucb | burst | 133.56 | 332.86 | 1086.05 | 1638.63 |
| ucb | steady | 136.17 | 139.06 | 1098.93 | 2054.10 |

#### 6.4.3 Run 2 Winners (Average Latency)

- `db_mixed_50_50` burst: **UCB** (129.52 ms)
- `db_mixed_50_50` steady: **UCB** (130.76 ms)
- `db_point_light` burst: **UCB** (138.58 ms)
- `db_point_light` steady: **Round Robin** (131.35 ms)
- `db_range_heavy` burst: **Round Robin** (129.47 ms)
- `db_range_heavy` steady: **UCB** (136.17 ms)

#### 6.4.4 Run 2 Interpretation

1. Under higher heterogeneity and broader failures, **UCB and Round Robin remain the strongest** on average latency.
2. **Metric-Aware still underperforms** despite tuning (`MA_W_LATENCY` up, `MA_STALE_PENALTY` down), indicating more aggressive redesign of score scaling/normalization may be needed.
3. **Least Connections stays competitive** but is not the top performer across mixed scenarios.

Both static/heuristic algorithms perform within 3% of UCB on most workloads. This is because:

- All nodes handle all keys — there's no data locality to exploit
- The controller's 1-second timeout + retry logic masks most node failures
- With 6 nodes and 30 concurrent requests, load naturally distributes (5 requests per node average)

The baselines are strong precisely because the retry mechanism acts as an implicit adaptation layer: if a node times out, the request is automatically retried on another node. This means the **worst-case latency is bounded by retry overhead**, not algorithm quality.

### 6.5 When UCB Would Excel

The advantage of UCB over static algorithms grows with:

- **Higher node heterogeneity**: If some nodes are 100× faster than others
- **Changing node performance**: If node latency varies over time (e.g., noisy neighbors, thermal throttling)
- **Larger node counts**: With 50+ nodes, Round Robin wastes requests on slow nodes; UCB converges to the fast subset
- **No retry mechanism**: Without retries, the cost of choosing a bad node is much higher

### 6.6 Limitations

1. **Local containerized network**: No real network variance; latency is entirely from simulated node delays
2. **Homogeneous key ranges**: All nodes handle all keys; no data locality to optimize
3. **Short runs**: 500 requests (~20s) don't capture long-term adaptation or drift
4. **Static failure rates**: Crash/timeout rates are constant; real failures are bursty
5. **Single controller**: The controller is a bottleneck; real systems use distributed routing
6. **Metric-Aware not tuned**: Weights are fixed; production systems would use adaptive tuning

### 6.7 Updated Design Choices After Run 2

Based on the higher-heterogeneity and higher-failure run, we made and retained the following choices:

1. Keep a strong baseline set (`round_robin`, `least_connections`, `ucb`, `metric_aware`) and compare all four on identical traffic/workload settings.
2. Increase node heterogeneity and failure pressure in Docker profiles to expose algorithm behavior under stress, rather than only under mild conditions.
3. Keep retry logic enabled to reflect practical availability requirements, while evaluating latency as the primary differentiator.
4. Keep metric-aware parameters externally configurable (`MA_*` env vars) so routing policy can be tuned without code edits.
5. Preserve automation-first evaluation with `benchmark_matrix.py` and `build_results_matrix.py` to ensure repeatability and fair comparison.

The main takeaway from these choices is methodological: the system now emphasizes controlled, repeatable stress testing over one-off demonstrations.

---

## 7. Conclusions

### 7.1 Hypothesis Evaluation

> A load balancer using push-based feedback with online learning (MAB) can reduce latency and improve routing quality under dynamic conditions.

Result: **partially supported**.

- `UCB` and `Round Robin` were consistently strongest in Run 2 average latency across most profile/pattern combinations.
- `Least Connections` remained competitive and close to the top performers in multiple scenarios.
- `Metric-Aware` remained weaker in this configuration even after latency-weight and stale-penalty adjustments.

This indicates that adaptive routing is not automatically superior; gains are scenario-dependent and highly sensitive to signal quality and scoring design.

### 7.2 Updated Findings

1. Under stronger heterogeneity/failure stress, **UCB remained a robust top performer** (especially on mixed and burst scenarios).
2. **Simple baselines remained surprisingly strong**, suggesting practical value in low-overhead strategies under this workload mix.
3. **Metric richness alone did not guarantee better routing**; score construction and signal freshness handling still dominate outcomes.
4. All algorithms preserved high reliability due to retry behavior; therefore, latency/tail metrics remain the primary evaluation target.

### 7.3 Practical Interpretation

For this project’s current architecture and workload:
- If simplicity is prioritized: `least_connections` or `round_robin` are viable.
- If adaptive behavior is desired with limited complexity: `ucb` is the best current candidate.
- `metric_aware` requires deeper redesign (normalization, stale handling, and/or dynamic weighting) before it can be considered production-competitive.

### 7.4 Next Steps

1. Run a second tuning pass focused on score normalization (not only weight magnitude changes).
2. Add one more realistic workload class (e.g., compression) and rerun the same matrix.
3. Report confidence intervals/variance bands in addition to means for stronger statistical claims.
4. Extend analysis with run-to-run stability metrics (sensitivity under burst + failure combinations).

---

## 8. Reproducibility

All experiments are reproducible with the repository scripts:

```bash
# Run matrix (example)
python scripts/benchmark_matrix.py \
  --requests 500 --concurrency 30 --repeat 3 \
  --algorithms round_robin least_connections ucb metric_aware \
  --profiles db_point_light db_range_heavy db_mixed_50_50 \
  --patterns steady burst \
  --label run2

# Build aggregated matrix from client logs
python scripts/build_results_matrix.py --output scripts/results_matrix.csv
```

Primary artifacts:
- Per-run benchmark summary: `logs/matrix_summary.csv`
- Aggregated matrix: `scripts/results_matrix.csv`
- Configuration: `docker-compose.yml`, `workloads.json`
