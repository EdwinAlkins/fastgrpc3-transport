"""Markdown tables from campaign.py's JSON: medians over the repetitions, and their spread."""

import json
import statistics
import sys

data = json.load(open(sys.argv[1]))

LABELS = {
    "raw": "`grpc.aio` + upb, no framework",
    "fastgrpc2": "fastgrpc2",
    "fastgrpc3@grpc_aio": "fastgrpc3 on `grpc.aio`",
    "fastgrpc3-novalidate@grpc_aio": "fastgrpc3 on `grpc.aio`, no validation",
    "fastgrpc3@tonic": "**fastgrpc3 on tonic**",
    "fastgrpc3-novalidate@tonic": "fastgrpc3 on tonic, no validation",
}


def med(reps: list[dict], point: str, key: str) -> float:
    return statistics.median(r[point][key] for r in reps)


def spread(reps: list[dict], point: str) -> float:
    values = [r[point]["rps"] for r in reps]
    return (max(values) - min(values)) / statistics.median(values) * 100


def table(
    part: dict, names: list[str], points: list[str], key: str, fmt: str, label=lambda n: n
) -> str:
    lines = ["| Server | " + " | ".join(points) + " |", "|---|" + "--:|" * len(points)]
    for name in names:
        if name in part:
            cells = [format(med(part[name], p, key), fmt) for p in points]
            lines.append(f"| {label(name)} | " + " | ".join(cells) + " |")
    return "\n".join(lines)


one = data["one_core"]
names = [n for n in LABELS if n in one]
points = list(next(iter(one.values()))[0])
label = lambda n: LABELS.get(n, n)  # noqa: E731
print("## One core\n\n### Requests per second (median of 3)\n")
print(table(one, names, points, "rps", ",.0f", label))
print("\n### Spread of the 3 repetitions (max − min) / median, %\n")
lines = ["| Server | " + " | ".join(points) + " |", "|---|" + "--:|" * len(points)]
for name in names:
    lines.append(
        f"| {label(name)} | " + " | ".join(f"{spread(one[name], p):.0f}" for p in points) + " |"
    )
print("\n".join(lines))
for key, title, fmt in (
    ("cpu_us", "Server CPU per call, µs", ".1f"),
    ("p50", "p50, ms", ".2f"),
    ("p95", "p95, ms", ".2f"),
    ("p99", "p99, ms", ".2f"),
    ("rss_mib", "Server RSS at the end, MiB", ".0f"),
):
    print(f"\n### {title}\n")
    print(table(one, names, points, key, fmt, label))

for part, title in (
    ("workers", "Several cores: `fastgrpc3 run --workers N`"),
    ("deps", "Dependencies (medium, c=64)"),
    ("middlewares", "Middlewares (medium, c=64)"),
):
    section = data[part]
    pts = list(next(iter(section.values()))[0])
    print(f"\n## {title}\n")
    for key, sub, fmt in (
        ("rps", "requests/s", ",.0f"),
        ("cpu_us", "CPU per call, µs", ".1f"),
        ("p99", "p99, ms", ".2f"),
    ):
        print(f"\n### {sub}\n")
        print(table(section, list(section), pts, key, fmt))
print(f"\nCampaign: {data['seconds'] / 60:.0f} min.")
