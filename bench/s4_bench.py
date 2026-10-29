"""Spike S4 benchmark: the servers of the spike against grpc.aio, with fastgrpc3's harness.

    ../fastgrpc/.venv/bin/python bench/s4_bench.py [--servers raw fastgrpc3 tonic-echo ...]

Each server runs alone, pinned to one CPU with all its threads; `ghz` loads it from other
CPUs with the requests of fastgrpc3's benchmark (`benches/contract.py`, pre-encoded).
"""

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
FASTGRPC = HERE.parents[1] / "fastgrpc"
sys.path.insert(0, str(FASTGRPC / "benches"))

import e2e_bench  # noqa: E402
from e2e_bench import free_port, pinned, run_ghz, wait_for_port  # noqa: E402

PYTHON = sys.executable
E2E = str(FASTGRPC / "benches" / "e2e_servers.py")
SERVERS: dict[str, list[str]] = {
    "raw": [PYTHON, E2E, "raw", "{port}"],
    "fastgrpc3": [PYTHON, E2E, "fastgrpc3", "{port}"],
    "tonic-echo": [str(HERE.parent / "target" / "release" / "echo"), "{port}", "1"],
    "tonic-A": [PYTHON, str(HERE / "python_server.py"), "direct", "{port}"],
    "tonic-B": [PYTHON, str(HERE / "python_server.py"), "queue", "{port}"],
    "tonic-B-2threads": [PYTHON, str(HERE / "python_server.py"), "queue", "{port}", "2"],
    "fastgrpc3-2workers": [
        PYTHON, "-m", "fastgrpc3", "run", "bench.fg3_app:app",
        "--address", "127.0.0.1:{port}", "--workers", "2",
    ],
}


def bench(command: list[str], cpus: str, data: Path, sizes: list[str], concurrencies: list[int], duration: float) -> dict[str, Any]:
    port = free_port()
    args = [part.replace("{port}", str(port)) for part in command]
    server = subprocess.Popen(pinned(cpus, args), cwd=HERE.parent, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    try:
        wait_for_port(port)
        results = {}
        for size in sizes:
            run_ghz(data, port, size, 4, 1.0)  # warm up
            for concurrency in concurrencies:
                results[f"{size} c={concurrency}"] = run_ghz(data, port, size, concurrency, duration)
        return results
    finally:
        server.terminate()
        server.wait(timeout=10)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--servers", nargs="+", default=list(SERVERS))
    parser.add_argument("--sizes", nargs="+", default=["small", "medium", "large"])
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 16, 64])
    parser.add_argument("--duration", type=float, default=5.0)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--server-cpus", default="0", help="taskset list for the server")
    parser.add_argument("--client-cpus", default="2-7", help="taskset list for ghz")
    args = parser.parse_args()
    e2e_bench.CLIENT_CPUS = args.client_cpus
    with tempfile.TemporaryDirectory() as directory:
        data = Path(directory)
        subprocess.run([PYTHON, E2E, "fastgrpc3", "--prepare", str(data)], check=True)
        results = {}
        for name in args.servers:
            print(f"benchmarking {name}...", file=sys.stderr)
            results[name] = bench(SERVERS[name], args.server_cpus, data, args.sizes, args.concurrency, args.duration)
    columns = list(next(iter(results.values())))
    for measure in ("rps", "p50 ms", "p99 ms"):
        print(f"\n### {measure}\n")
        print("| | " + " | ".join(columns) + " |")
        print("|---|" + "---:|" * len(columns))
        for name, points in results.items():
            cells = [f"{points[c][measure]:,.0f}" if measure == "rps" else f"{points[c][measure]:.2f}" for c in columns]
            print(f"| {name} | " + " | ".join(cells) + " |")
    if args.out:
        args.out.write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
