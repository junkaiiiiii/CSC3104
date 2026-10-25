"""Line sources. Today: a file or stdin. A Kubernetes log source plugs in later."""
from __future__ import annotations

import select


def iter_lines(stream, idle_tick_s=None):
    """Yield lines from stream. With idle_tick_s, also yield None after that many
    idle seconds so a live pipe still advances the watermark. Stops at EOF."""
    if idle_tick_s is None:
        for line in stream:
            yield line
        return
    while True:
        ready, _, _ = select.select([stream], [], [], idle_tick_s)
        if not ready:
            yield None
            continue
        line = stream.readline()
        if line == "":
            return
        yield line
