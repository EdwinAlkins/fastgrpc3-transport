"""The bench's echo service as a `fastgrpc3 run` target (`bench.fg3_app:app`)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "fastgrpc" / "benches"))

from e2e_servers import fastgrpc3_app  # noqa: E402

app, _ = fastgrpc3_app(0)
