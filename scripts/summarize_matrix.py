#!/usr/bin/env python3
"""Summarize repeated-run benchmark matrix with aggregated statistics."""

import csv
from statistics import mean
from pathlib import Path
from typing import Dict, List, Tuple
from io import StringIO

def summarize_matrix(matrix_csv: str = "scripts/results_matrix.csv") -> None:
    path = Path(matrix_csv)
    
    with path.open(newline='') as f:
        lines = f.readlines()
    
    # Skip "section,per_run" metadata row
    if lines[0].strip() == 'section,per_run':
        data_start = 1
    else:
        data_start = 0
    
    # Use DictReader on the remaining lines
    reader = csv.DictReader(StringIO(''.join(lines[data_start:])))
    rows = [r for r in reader if r.get('algorithm')]
    
    # Group by (algorithm, profile)
    parsed: Dict[Tuple[str, str], List[dict]] = {}
    for rec in rows:
        label = rec.get('label') or ''
        if label.startswith('matrixrep'):
            key = (rec['algorithm'], rec['profile'])
            parsed.setdefault(key, []).append(rec)
    
    # Compute and print aggregates
    print("\n" + "=" * 90)
    print("AGGREGATED REPEATED-RUN BENCHMARK RESULTS")
    print("=" * 90)
    print(f"{'Algorithm':<20} {'Profile':<20} {'Runs':<6} {'Avg (ms)':<12} {'P95 (ms)':<12} {'P99 (ms)':<12} {'Max (ms)':<12}")
    print("-" * 90)
    
    for key in sorted(parsed.keys()):
        algo, profile = key
        items = parsed[key]
        
        avg = mean(float(r['avg_ms']) for r in items)
        p95 = mean(float(r['p95_ms']) for r in items)
        p99 = mean(float(r['p99_ms']) for r in items)
        maxs = mean(float(r['max_ms']) for r in items)
        
        print(f"{algo:<20} {profile:<20} {len(items):<6} {avg:>11.3f} {p95:>11.3f} {p99:>11.3f} {maxs:>11.3f}")
    
    print("=" * 90 + "\n")


if __name__ == "__main__":
    summarize_matrix()
