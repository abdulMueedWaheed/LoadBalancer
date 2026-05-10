# Project TODO

## 1. Runtime Algorithm Configuration [DONE]
Why:
The controller algorithm is currently hardcoded, which makes comparative testing slow and error-prone.
Expected outcome:
Algorithm selection becomes configurable at runtime (e.g., env var or API), so experiments can switch between Round Robin, Least Connections, and UCB without code edits.
Status:
Completed on May 10, 2026. Controller now reads `ALGORITHM` from environment with startup validation, and client labels auto-sync with controller algorithm via `/stats`.

## 2. Introduce Real Workload Endpoints
Why:
Simulated delay alone cannot represent compute-bound or data-dependent workloads.
Expected outcome:
Nodes process at least two real workload types (e.g., compression and image transform), enabling evaluation beyond synthetic sleep delays.

## 3. Refresh and Expand Metrics Schema
Why:
Current metrics are good for latency/failure basics but not enough for realistic distributed scheduling decisions.
Expected outcome:
Controller and node logs capture richer signals (queue depth, task type, retries, node utilization proxies, and locality fields) for deeper analysis.

## 4. Add Node Capability Advertisement
Why:
Realistic routing requires knowing which nodes are best suited for specific workloads.
Expected outcome:
Each node publishes capability metadata (e.g., CPU class, memory tier, supported task types), and the controller stores this state for routing.

## 5. Implement Data Locality Metadata and Routing Hooks
Why:
Distributed systems performance depends heavily on data placement and partition ownership.
Expected outcome:
Requests include dataset/shard identifiers, nodes expose shard ownership, and the controller can prioritize locality-aware routes.

## 6. Extend UCB Reward Function
Why:
Latency-only reward does not reflect true system cost under heterogeneous workloads.
Expected outcome:
UCB reward incorporates multiple factors (latency, success/failure, queue pressure, and locality), improving adaptive routing quality.

## 7. Build Fault Scenarios Matrix
Why:
Ad-hoc failure testing makes it hard to compare algorithm resilience consistently.
Expected outcome:
A repeatable matrix of fault scenarios (timeouts, crash bursts, permanent node failure, mixed faults) is defined and executable.

## 8. Standardize Experiment Protocol
Why:
Inconsistent run settings can invalidate algorithm comparisons.
Expected outcome:
A documented experiment protocol defines workload mix, concurrency levels, run count, warm-up, and summary statistics for fair benchmarking.

## 9. Dashboard Enhancements for Comparative Analysis
Why:
Current visualizations are useful but not yet focused on scheduler-level tradeoffs.
Expected outcome:
Dashboard adds per-algorithm comparisons by workload type, failure mode, and locality hit rate with clearer side-by-side views.

## 10. Final Performance Report
Why:
The project needs a clear narrative linking design choices to measured outcomes.
Expected outcome:
A concise report summarizes setup, methodology, results, tradeoffs, and recommendations for future production-grade extensions.

## 11. Real Workload Implementation: DB Query (Start Here)
Why:
Database-like access patterns are fundamental in distributed systems and introduce realistic latency and data-access behavior beyond synthetic delays.
Expected outcome:
Nodes execute SQLite-backed query workloads (point, range, aggregate) with configurable request parameters, and the load balancer routes these requests end-to-end.

## 12. Real Workload Implementation: Image Resize
Why:
Image processing introduces compute-heavy tasks with input-size-dependent cost, which helps evaluate scheduling under heterogeneous workloads.
Expected outcome:
Nodes process image-resize workloads (initially simulated or library-backed) with tunable dimensions and iterations to produce measurable compute variance.

## 13. Real Workload Implementation: Compression
Why:
Compression is a practical CPU-bound workload and easy to parameterize for repeatable experiments.
Expected outcome:
Nodes run configurable compression tasks (payload size, compression level, iterations) and return timing/results for comparative routing experiments.

## 14. Real Workload Implementation: Inference
Why:
Inference-style workloads represent modern distributed systems where compute cost and batch size strongly affect latency.
Expected outcome:
Nodes execute inference-like workloads (surrogate compute first, model-backed later) with controllable batch/size parameters for realistic load testing.
