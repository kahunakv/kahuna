#!/usr/bin/env python3
"""Summarizes one scripts/gc-bench.sh run directory.

Reads counters.csv (dotnet-counters, System.Runtime meter, one sample per second), run.env
(the benchmark window) and bench.json (kahuna-bench output), and prints: the GC pause share of
wall time inside the benchmark window, the longest one-second pause, collections per generation,
the gen1 growth after each gen0 collection (the promoted volume), heap sizes, the pinned object
heap size and fragmentation, the working set, the allocation rate, and the benchmark latencies.
Pass --timeline to also print one line per second.

Usage: gc-summary.py <run-dir> [--timeline]
"""

from __future__ import annotations

import csv
import json
import pathlib
import sys
import time
from collections import defaultdict
from datetime import datetime

MB = 1024 * 1024


def parse_timestamp(text: str) -> float:
    for fmt in ("%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"):
        try:
            return time.mktime(datetime.strptime(text.strip(), fmt).timetuple())
        except ValueError:
            continue
    raise ValueError(f"unrecognized timestamp: {text!r}")


def counter_key(name: str) -> tuple[str, str]:
    """'dotnet.gc.collections ({collection} / 1 sec)[gc.heap.generation=gen0]' -> ('dotnet.gc.collections', 'gen0')."""
    tag = ""
    if "[" in name and name.endswith("]"):
        name, tag_text = name[:-1].split("[", 1)
        tag = tag_text.split("=", 1)[1] if "=" in tag_text else tag_text
    base = name.split(" (", 1)[0].strip()
    return base, tag


def load_counters(path: pathlib.Path) -> dict[float, dict[tuple[str, str], float]]:
    samples: dict[float, dict[tuple[str, str], float]] = defaultdict(dict)
    with path.open(newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header is None:
            return samples
        for row in reader:
            if len(row) < 5:
                continue
            try:
                stamp = parse_timestamp(row[0])
                value = float(row[4])
            except ValueError:
                continue
            samples[stamp][counter_key(row[2])] = value
    return samples


def load_env(path: pathlib.Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for line in path.read_text().splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip()
    return env


def fmt_mb(value: float | None) -> str:
    return "n/a" if value is None else f"{value / MB:,.0f} MB"


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2

    run_dir = pathlib.Path(sys.argv[1])
    timeline = "--timeline" in sys.argv[2:]
    env = load_env(run_dir / "run.env")
    samples = load_counters(run_dir / "counters.csv")
    if not samples:
        print(f"no counter samples in {run_dir / 'counters.csv'}")
        return 1

    stamps = sorted(samples)
    start = float(env.get("bench_start_epoch", stamps[0]))
    end = float(env.get("bench_end_epoch", stamps[-1]))
    window = [s for s in stamps if start <= s <= end]
    if len(window) < 2:
        window = stamps

    def series(base: str, tag: str = "") -> list[tuple[float, float]]:
        return [(s, samples[s][(base, tag)]) for s in window if (base, tag) in samples[s]]

    pause = series("dotnet.gc.pause.time")
    wall = (window[-1] - window[0]) + 1 if len(window) > 1 else 1
    total_pause = sum(v for _, v in pause)
    longest_second = max((v for _, v in pause), default=0.0)
    seconds_over_100ms = sum(1 for _, v in pause if v >= 0.1)

    counts = {gen: sum(v for _, v in series("dotnet.gc.collections", gen)) for gen in ("gen0", "gen1", "gen2")}

    gen1_size = dict(series("dotnet.gc.last_collection.heap.size", "gen1"))
    gen0_counts = dict(series("dotnet.gc.collections", "gen0"))
    gen1_counts = dict(series("dotnet.gc.collections", "gen1"))
    gen2_counts = dict(series("dotnet.gc.collections", "gen2"))
    gen1_growth: list[float] = []
    previous_gen1 = None
    for stamp in window:
        current = gen1_size.get(stamp)
        if current is None:
            continue
        if previous_gen1 is not None and gen0_counts.get(stamp, 0) > 0 \
                and gen1_counts.get(stamp, 0) == 0 and gen2_counts.get(stamp, 0) == 0:
            gen1_growth.append(current - previous_gen1)
        previous_gen1 = current

    def peak(base: str, tag: str = "") -> float | None:
        values = [v for _, v in series(base, tag)]
        return max(values) if values else None

    def mean(base: str, tag: str = "") -> float | None:
        values = [v for _, v in series(base, tag)]
        return sum(values) / len(values) if values else None

    print(f"GC summary: {run_dir}")
    for key in ("git_head", "storage", "workload", "key_space", "host"):
        if key in env:
            print(f"  {key}: {env[key]}")
    print(f"  window: {len(window)} samples over {wall:.0f} s "
          f"({datetime.fromtimestamp(window[0]):%H:%M:%S} .. {datetime.fromtimestamp(window[-1]):%H:%M:%S})")
    print()
    print("GC pauses")
    print(f"  total pause            : {total_pause:.3f} s = {100.0 * total_pause / wall:.1f}% of wall")
    print(f"  longest 1-s pause value: {longest_second:.3f} s")
    print(f"  seconds with >= 100 ms : {seconds_over_100ms}")
    print(f"  collections            : gen0 {counts['gen0']:.0f}, gen1 {counts['gen1']:.0f}, gen2 {counts['gen2']:.0f}")
    if gen1_growth:
        growth_text = ", ".join(f"{g / MB:+,.0f}" for g in gen1_growth)
        print(f"  gen1 growth per gen0   : mean {sum(gen1_growth) / len(gen1_growth) / MB:,.0f} MB [{growth_text}] MB")
    else:
        print("  gen1 growth per gen0   : no gen0-only second in the window")
    print()
    print("Heap (peak in window)")
    for gen in ("gen0", "gen1", "gen2", "loh", "poh"):
        print(f"  {gen:<5} size             : {fmt_mb(peak('dotnet.gc.last_collection.heap.size', gen))}"
              f"   fragmentation {fmt_mb(peak('dotnet.gc.last_collection.heap.fragmentation.size', gen))}")
    print(f"  committed              : {fmt_mb(peak('dotnet.gc.last_collection.memory.committed_size'))}")
    print(f"  working set            : {fmt_mb(peak('dotnet.process.memory.working_set'))}")
    alloc = mean("dotnet.gc.heap.total_allocated")
    print(f"  allocation rate (mean) : {fmt_mb(alloc)}/s")
    cpu_sys = mean("dotnet.process.cpu.time", "system")
    cpu_user = mean("dotnet.process.cpu.time", "user")
    if cpu_sys is not None and cpu_user is not None:
        print(f"  cpu (mean cores)       : user {cpu_user:.2f}, system {cpu_sys:.2f}")

    bench_path = run_dir / "bench.json"
    if bench_path.exists():
        try:
            bench = json.loads(bench_path.read_text())
        except json.JSONDecodeError:
            bench = None
        if bench:
            print()
            print("Benchmark")
            rows = list(bench.get("operations", []))
            if "aggregate" in bench:
                rows.append(bench["aggregate"])
            print(f"  {'op':<8}{'rps':>12}{'p50 ms':>10}{'p99 ms':>10}{'p99.9 ms':>10}{'max ms':>10}{'errors':>8}")
            for row in rows:
                print(f"  {row.get('operation', ''):<8}{row.get('rps', 0):>12,.0f}{row.get('p50Ms', 0):>10.3f}"
                      f"{row.get('p99Ms', 0):>10.3f}{row.get('p999Ms', 0):>10.3f}{row.get('maxMs', 0):>10.3f}"
                      f"{row.get('errors', 0):>8}")

    slow_path = run_dir / "slow-dispatch.log"
    if slow_path.exists():
        lines = slow_path.read_text().splitlines()
        if lines:
            print()
            print(f"Slow-dispatch log lines: {len(lines)} (first 3)")
            for line in lines[:3]:
                print(f"  {line[:160]}")

    report_path = run_dir / "heap-report.txt"
    if report_path.exists():
        lines = [l for l in report_path.read_text().splitlines() if l.strip()]
        print()
        print("Heap dump (top 20 lines of dotnet-gcdump report)")
        for line in lines[:22]:
            print(f"  {line[:160]}")

    if timeline:
        print()
        print(f"  {'t':>4}{'pause s':>9}{'g0':>4}{'g1':>4}{'g2':>4}{'gen1 MB':>9}{'gen2 MB':>9}{'poh MB':>8}{'ws MB':>8}{'alloc MB/s':>11}")
        for stamp in window:
            row = samples[stamp]

            def v(base: str, tag: str = "") -> float | None:
                return row.get((base, tag))

            def mb(value: float | None) -> str:
                return "" if value is None else f"{value / MB:,.0f}"

            print(f"  {stamp - window[0]:>4.0f}{(v('dotnet.gc.pause.time') or 0):>9.3f}"
                  f"{(v('dotnet.gc.collections', 'gen0') or 0):>4.0f}{(v('dotnet.gc.collections', 'gen1') or 0):>4.0f}"
                  f"{(v('dotnet.gc.collections', 'gen2') or 0):>4.0f}"
                  f"{mb(v('dotnet.gc.last_collection.heap.size', 'gen1')):>9}{mb(v('dotnet.gc.last_collection.heap.size', 'gen2')):>9}"
                  f"{mb(v('dotnet.gc.last_collection.heap.size', 'poh')):>8}{mb(v('dotnet.process.memory.working_set')):>8}"
                  f"{mb(v('dotnet.gc.heap.total_allocated')):>11}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
