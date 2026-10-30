"""Lot 9 benchmark campaign: one core, several cores (workers), dependencies, middlewares.

    ../fastgrpc/.venv/bin/python bench/campaign.py OUT.json [--quick]
    python bench/campaign_report.py OUT.json        # the markdown tables

Needs fastgrpc next to this repository (its benches and .venv, with this transport and
fastgrpc2 installed), `ghz` and `taskset`. The server runs on CPU 0 (CPUs 0,2[,4,6] for
the workers), `ghz` on CPUs 2-7 (4-15 or 8-15).

Each point: ghz load, rps, p50/p95/p99, failed calls (refused if > concurrency, the calls
cut at the end), server CPU per call (every process of the server, every thread) and RSS.
Repetitions run the servers in alternating order.
"""

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

TRANSPORT = Path(__file__).resolve().parents[1]
FASTGRPC = TRANSPORT.parent / "fastgrpc"
PYTHON = str(FASTGRPC / ".venv/bin/python")
SERVERS = str(FASTGRPC / "benches/e2e_servers.py")
GHZ = shutil.which("ghz") or str(Path.home() / "go/bin/ghz")
TICK = os.sysconf("SC_CLK_TCK")
QUICK = "--quick" in sys.argv
DURATION = 1.0 if QUICK else 4.0


def free_port() -> int:
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def tree(pid: int) -> list[int]:
    """The process and its descendants."""
    pids, todo = [], [pid]
    while todo:
        p = todo.pop()
        pids.append(p)
        try:
            for task in Path(f"/proc/{p}/task").iterdir():
                todo += [int(c) for c in (task / "children").read_text().split()]
        except OSError:
            pass
    return pids


def cpu_and_rss(pid: int) -> tuple[float, float]:
    cpu = rss = 0.0
    for p in tree(pid):
        try:
            stat = Path(f"/proc/{p}/stat").read_text()
            fields = stat[stat.rindex(")") + 2 :].split()
            cpu += (int(fields[11]) + int(fields[12])) / TICK
            rss += int(fields[21]) * os.sysconf("SC_PAGE_SIZE") / 2**20
        except OSError:
            pass
    return cpu, rss


def ghz(
    data: Path, port: int, size: str, concurrency: int, seconds: float, cpus: str
) -> dict[str, Any]:
    command = ["taskset", "-c", cpus, GHZ, "--insecure",
               "--protoset", str(data / "bench.protoset"),
               "--call", f"bench.v1.Bench/{size.capitalize()}",
               "--binary-file", str(data / f"{size}.bin"),
               "--concurrency", str(concurrency), "--connections", str(min(concurrency, 4)),
               "--duration", f"{seconds}s", "--format", "json", f"127.0.0.1:{port}"]  # fmt: skip
    report = json.loads(subprocess.run(command, capture_output=True, text=True, check=True).stdout)
    failed = sum(n for code, n in report["statusCodeDistribution"].items() if code != "OK")
    if failed > concurrency:
        raise RuntimeError(f"{failed} failed calls: {report['statusCodeDistribution']}")
    latency = {d["percentage"]: d["latency"] / 1e6 for d in report["latencyDistribution"]}
    ok = report["statusCodeDistribution"].get("OK", 0)
    return {"rps": report["rps"], "ok": ok, "failed": failed,
            "p50": latency[50], "p95": latency[95], "p99": latency[99]}  # fmt: skip


def wait(port: int, proc: subprocess.Popen[bytes]) -> None:
    import socket

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("the server exited")
        with socket.socket() as sock:
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                time.sleep(0.5)  # every worker up
                return
        time.sleep(0.1)
    raise RuntimeError("the server did not start")


def server_command(name: str, port: int) -> tuple[list[str], dict[str, str], str]:
    """`name` is `MODE@TRANSPORT` or `workersN@TRANSPORT`; returns (command, env, cwd)."""
    mode, _, transport = name.partition("@")
    env = {**os.environ, "FASTGRPC_TRANSPORT": transport or "grpc_aio"}
    if mode.startswith("workers"):
        workers = mode[len("workers") :]
        command = [PYTHON, "-m", "fastgrpc3", "run", "bench.fg3_app:app", "--grace", "0.5",
                   "--address", f"127.0.0.1:{port}", "--workers", workers]  # fmt: skip
        return command, env, str(TRANSPORT)
    return [PYTHON, SERVERS, mode, str(port)], env, str(FASTGRPC)


def run_server(
    name: str, cpus: str, client_cpus: str, data: Path, points: list[tuple[str, int]]
) -> dict[str, Any]:
    port = free_port()
    command, env, cwd = server_command(name, port)
    proc = subprocess.Popen(["taskset", "-c", cpus, *command], env=env, cwd=cwd,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)  # fmt: skip
    results: dict[str, Any] = {}
    try:
        wait(port, proc)
        for size in sorted({s for s, _ in points}):
            ghz(data, port, size, 4, 1.0, client_cpus)  # warm up
        for size, concurrency in points:
            before, start = cpu_and_rss(proc.pid)[0], time.perf_counter()
            point = ghz(data, port, size, concurrency, DURATION, client_cpus)
            cpu, rss = cpu_and_rss(proc.pid)
            calls = point["ok"] + point["failed"]
            point["cpu_us"] = (cpu - before) / max(calls, 1) * 1e6
            point["rss_mib"] = rss
            point["wall"] = time.perf_counter() - start
            results[f"{size} c={concurrency}"] = point
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            proc.kill()
    return results


def campaign() -> dict[str, Any]:
    sizes = ["small", "medium", "large"]
    one_core = [(s, c) for s in sizes for c in ([16] if QUICK else [1, 16, 64, 256])]
    servers = ["raw", "fastgrpc2", "fastgrpc3@grpc_aio", "fastgrpc3-novalidate@grpc_aio",
               "fastgrpc3@tonic", "fastgrpc3-novalidate@tonic"]  # fmt: skip
    reps = 1 if QUICK else 3
    out: dict[str, Any] = {"one_core": {}, "workers": {}, "deps": {}, "middlewares": {}}
    with tempfile.TemporaryDirectory() as d:
        data = {}
        for kind in ("fastgrpc3", "fastgrpc2"):
            data[kind] = Path(d) / kind
            subprocess.run([PYTHON, SERVERS, kind, "--prepare", str(data[kind])], check=True,
                           capture_output=True)  # fmt: skip

        def data_of(name: str) -> Path:
            return data["fastgrpc2" if name.startswith("fastgrpc2") else "fastgrpc3"]

        for rep in range(reps):
            order = servers if rep % 2 == 0 else servers[::-1]
            for name in order:
                print(f"[one core] rep {rep + 1}/{reps} {name}", flush=True)
                res = run_server(name, "0", "2-7", data_of(name), one_core)
                out["one_core"].setdefault(name, []).append(res)

        multi = [(s, c) for s in ("small", "medium") for c in ([64] if QUICK else [64, 256])]
        layouts = {2: ("0,2", "4-15"), 4: ("0,2,4,6", "8-15")}
        for rep in range(1 if QUICK else 2):
            for n, (cpus, client) in layouts.items():
                names = [f"workers{n}@grpc_aio", f"workers{n}@tonic"]
                for name in names if rep % 2 == 0 else names[::-1]:
                    print(f"[workers] rep {rep + 1} {name}", flush=True)
                    res = run_server(name, cpus, client, data["fastgrpc3"], multi)
                    out["workers"].setdefault(name, []).append(res)

        point = [("medium", 64)]
        for rep in range(1 if QUICK else 3):
            for transport in ("grpc_aio", "tonic") if rep % 2 == 0 else ("tonic", "grpc_aio"):
                for n in (0, 1, 3, 10):
                    name = f"fastgrpc3-deps-{n}-async@{transport}"
                    print(f"[deps] rep {rep + 1} {name}", flush=True)
                    res = run_server(name, "0", "2-7", data["fastgrpc3"], point)
                    out["deps"].setdefault(name, []).append(res)
                for n in (0, 1, 3):
                    name = f"fastgrpc3-mw-{n}@{transport}"
                    print(f"[middlewares] rep {rep + 1} {name}", flush=True)
                    res = run_server(name, "0", "2-7", data["fastgrpc3"], point)
                    out["middlewares"].setdefault(name, []).append(res)
    return out


if __name__ == "__main__":
    started = time.time()
    result = campaign()
    result["seconds"] = time.time() - started
    Path(sys.argv[1]).write_text(json.dumps(result, indent=1))
    print(f"done in {result['seconds'] / 60:.1f} min", flush=True)
