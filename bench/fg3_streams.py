"""`fastgrpc/benches/stream_bench.py`'s streaming service on `grpc.aio`: `python fg3_streams.py PORT`."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "fastgrpc" / "benches"))

from stream_bench import app  # noqa: E402

if __name__ == "__main__":
    application, _, _ = app(int(sys.argv[1]))
    asyncio.run(application.run())
