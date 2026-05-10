# Distributed Load Balancer

An adaptive load balancer that compares multiple routing strategies (Least Connections, UCB, Metric-Aware) on heterogeneous backend nodes using real workloads (DB queries).

## What It Does

- Routes HTTP requests across three backend nodes running different load profiles
- Implements three routing algorithms: baseline (Least Connections), exploration-based (UCB), and adaptive (Metric-Aware)
- Executes real SQLite-backed DB query workloads (point, range, aggregate)
- Collects metrics (latency, queue depth, failures) and makes adaptive routing decisions
- Benchmarks algorithm performance across workload types with repeatable, automated experiments

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.10+

### Run

```bash
# Start services (metric_aware algorithm by default)
docker compose up -d

# Run a benchmark (90 requests, 15 concurrent)
python3 client/main.py --workload-profile db_point_light --requests 90 --concurrency 15

# View results
cat logs/client_*.csv | head -20
```

### Change Algorithm

Edit `docker-compose.yml`:
```yaml
environment:
  ALGORITHM: "least_connections"  # or "ucb", "metric_aware"
```
Then restart: `docker compose restart controller`

## Architecture

- **Controller** (FastAPI): Routes requests, tracks metrics, applies algorithm
- **Nodes** (3 instances): Process DB queries with varying latency profiles
- **Client**: Sends parameterized workload requests, logs results
- **Benchmark Suite**: Automates repeated runs and aggregates results

## Available Workloads

- `db_point_light`: Single-key lookups (light)
- `db_range_heavy`: Range queries over large key spaces (heavy)
- `db_aggregate_mid`: Aggregate statistics queries (medium)

Define custom workloads in `workloads.json`.

## Results

See [results.md](results.md) for multi-run benchmarking analysis:
- **Metric-aware** outperforms baseline by **5%** on point queries
- **UCB** achieves best tail-latency control on range queries
- Full improvement breakdown and limitations documented

## Documentation

- [results.md](results.md) — Aggregated benchmark results and analysis
- [submission.md](submission.md) — Project design, implementation status, tradeoffs
- [todo.md](todo.md) — Completed work and future extensions

## License

MIT
