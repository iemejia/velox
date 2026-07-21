#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Summarizes randomized, paired parallel Parquet writer trials."""

import argparse
import json
import math
import random
import statistics
from collections import defaultdict
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", type=Path, nargs="+")
    parser.add_argument("--metric", default=None)
    parser.add_argument("--baseline", default="serial")
    parser.add_argument("--bootstrap-samples", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def percentile(values: list[float], probability: float) -> float:
    ordered = sorted(values)
    position = probability * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def bootstrap_median_ci(
    values: list[float], samples: int, random_generator: random.Random
) -> tuple[float, float]:
    estimates = []
    for _ in range(samples):
        resample = [random_generator.choice(values) for _ in values]
        estimates.append(statistics.median(resample))
    return percentile(estimates, 0.025), percentile(estimates, 0.975)


def main() -> int:
    args = parse_args()
    records = []
    study_identity = None
    identity_keys = (
        "mode",
        "workload",
        "codec",
        "columns",
        "rows",
        "batches",
        "instrumented",
        "hash_output",
    )
    for path in args.inputs:
        with path.open(encoding="utf-8") as source:
            for line in source:
                if not line.strip():
                    continue
                record = json.loads(line)
                identity = tuple(record.get(key) for key in identity_keys)
                if study_identity is None:
                    study_identity = identity
                elif identity != study_identity:
                    raise RuntimeError("Input files contain mixed study parameters")
                record["source"] = str(path)
                records.append(record)
    if not records:
        raise RuntimeError("No records found")

    metric = args.metric
    if metric is None:
        metric = (
            "steady_batch_avg_ns"
            if records[0]["mode"] in ("warm", "arrow_warm")
            else "total_ns"
        )

    groups = defaultdict(dict)
    for record in records:
        block_key = (record["source"], record["block"])
        configuration = record["configuration"]
        if configuration in groups[block_key]:
            raise RuntimeError(
                f"Duplicate block/configuration: {block_key}, {configuration}"
            )
        groups[block_key][configuration] = record[metric]
    configurations = sorted(
        {record["configuration"] for record in records},
        key=lambda value: (value != "serial", value != "threaded1", value),
    )
    if args.baseline not in configurations:
        raise RuntimeError(
            f"Paired speedup requires baseline configuration {args.baseline}"
        )
    expected = set(configurations)
    for block_key, values in groups.items():
        if set(values) != expected:
            raise RuntimeError(
                f"Incomplete block {block_key}: expected {expected}, got {set(values)}"
            )

    random_generator = random.Random(args.seed)
    summary = {
        "metric": metric,
        "baseline": args.baseline,
        "blocks": len(groups),
        "configurations": {},
    }
    for configuration in configurations:
        ratios = []
        latencies = []
        for values in groups.values():
            if args.baseline not in values or configuration not in values:
                continue
            latencies.append(values[configuration])
            ratios.append(values[args.baseline] / values[configuration])
        lower, upper = bootstrap_median_ci(
            ratios, args.bootstrap_samples, random_generator
        )
        summary["configurations"][configuration] = {
            "paired_samples": len(ratios),
            "median_latency_ns": statistics.median(latencies),
            "median_speedup": statistics.median(ratios),
            "speedup_ci_95": [lower, upper],
            "latency_iqr_ns": [
                percentile(latencies, 0.25),
                percentile(latencies, 0.75),
            ],
        }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
