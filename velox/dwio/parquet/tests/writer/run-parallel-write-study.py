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

"""Runs randomized, process-isolated parallel Parquet writer trials."""

import argparse
import json
import os
import random
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--mode",
        choices=("cold", "warm", "arrow_cold", "arrow_warm"),
        default="arrow_cold",
    )
    parser.add_argument("--workload", required=True)
    parser.add_argument("--codec", default="zstd")
    parser.add_argument(
        "--workers",
        default="serial,deferred,deferred_contexts,threaded1,2,4",
    )
    parser.add_argument("--blocks", type=int, default=30)
    parser.add_argument("--columns", type=int, default=20)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--batches", type=int, default=8)
    parser.add_argument(
        "--warmups",
        type=int,
        default=5,
        help="Sacrificial writers per process. Five are needed for stable local results.",
    )
    parser.add_argument(
        "--cpus",
        default="2,4,6,1",
        help="One logical CPU per physical core, ordered by preference.",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--instrument", action="store_true")
    parser.add_argument(
        "--fixed-cpuset",
        action="store_true",
        help="Give every configuration the full cpuset (diagnostic controls only).",
    )
    return parser.parse_args()


def worker_flags(configuration: str) -> list[str]:
    if configuration == "serial":
        return [
            "--workers=1",
            "--threaded_one_worker=false",
            "--deferred_serial=false",
            "--per_field_contexts=false",
        ]
    if configuration == "deferred":
        return [
            "--workers=1",
            "--threaded_one_worker=false",
            "--deferred_serial=true",
            "--per_field_contexts=false",
        ]
    if configuration == "deferred_contexts":
        return [
            "--workers=1",
            "--threaded_one_worker=false",
            "--deferred_serial=true",
            "--per_field_contexts=true",
        ]
    if configuration == "threaded1":
        return [
            "--workers=1",
            "--threaded_one_worker=true",
            "--deferred_serial=false",
            "--per_field_contexts=false",
        ]
    workers = int(configuration)
    return [
        f"--workers={workers}",
        "--threaded_one_worker=false",
        "--deferred_serial=false",
        "--per_field_contexts=false",
    ]


def cpu_list(configuration: str, available_cpus: list[str]) -> str:
    workers = (
        1
        if configuration in ("serial", "deferred", "deferred_contexts", "threaded1")
        else int(configuration)
    )
    if workers > len(available_cpus):
        raise ValueError(
            f"Configuration {configuration} requires {workers} CPUs, "
            f"but only {len(available_cpus)} were provided"
        )
    return ",".join(available_cpus[:workers])


def main() -> int:
    args = parse_args()
    configurations = args.workers.split(",")
    available_cpus = args.cpus.split(",")
    random_generator = random.Random(args.seed)
    args.output.parent.mkdir(parents=True, exist_ok=True)

    environment = os.environ.copy()
    environment["OMP_NUM_THREADS"] = "1"
    with args.output.open("w", encoding="utf-8") as output:
        for block in range(args.blocks):
            order = configurations.copy()
            random_generator.shuffle(order)
            for order_index, configuration in enumerate(order):
                configuration_cpus = (
                    args.cpus
                    if args.fixed_cpuset
                    else cpu_list(configuration, available_cpus)
                )
                command = [
                    "taskset",
                    "--cpu-list",
                    configuration_cpus,
                    str(args.binary),
                    f"--mode={args.mode}",
                    f"--workload={args.workload}",
                    f"--codec={args.codec}",
                    f"--columns={args.columns}",
                    f"--rows={args.rows}",
                    f"--batches={args.batches}",
                    f"--warmups={args.warmups}",
                    "--trials=1",
                    f"--seed={args.seed}",
                    f"--instrument={str(args.instrument).lower()}",
                    "--hash_output=false",
                    *worker_flags(configuration),
                ]
                completed = subprocess.run(
                    command,
                    check=True,
                    capture_output=True,
                    text=True,
                    env=environment,
                )
                records = [
                    json.loads(line)
                    for line in completed.stdout.splitlines()
                    if line.startswith("{")
                ]
                if len(records) != 1:
                    print(completed.stdout, file=sys.stderr)
                    print(completed.stderr, file=sys.stderr)
                    raise RuntimeError("Expected exactly one JSON result")
                record = records[0]
                record.update(
                    block=block,
                    order_index=order_index,
                    configuration=configuration,
                    cpu_list=configuration_cpus,
                )
                output.write(json.dumps(record, sort_keys=True) + "\n")
                output.flush()
                print(
                    f"block={block + 1}/{args.blocks} configuration={configuration}",
                    file=sys.stderr,
                )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
