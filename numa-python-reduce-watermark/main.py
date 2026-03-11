import asyncio
import logging
import os
import random
from collections.abc import AsyncIterable

from pynumaflow.reducestreamer import (
    ReduceStreamer,
    ReduceStreamAsyncServer,
    Datum as ReduceDatum,
    Metadata,
    Message as ReduceMessage,
)
from pynumaflow.mapper import MapServer, Datum as MapDatum, Messages, Message as MapMessage
from pynumaflow.shared.asynciter import NonBlockingIterator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# -- Map: passthrough ---------------------------------------------------------

KEYS = [f"key-{i}" for i in range(1, 11)]

def passthrough(keys: list[str], datum: MapDatum) -> Messages:
    keys = keys or [random.choice(KEYS)]
    logger.info(
        "passthrough: event_time=%s, watermark=%s, keys=%s",
        datum.event_time, datum.watermark, ','.join(keys),
    )
    return Messages(MapMessage(datum.value, keys=keys))


# -- Reduce: counter ----------------------------------------------------------


class Counter(ReduceStreamer):
    async def handler(
        self,
        keys: list[str],
        datums: AsyncIterable[ReduceDatum],
        output: NonBlockingIterator,
        md: Metadata,
    ):
        logger.info(
            "Window opened: start=%s end=%s keys=%s",
            md.interval_window.start,
            md.interval_window.end,
            keys,
        )
        count = 0
        async for datum in datums:
            count += 1
            logger.info(
                "datum #%d: event_time=%s, watermark=%s",
                count, datum.event_time, datum.watermark,
            )
        logger.info("Window closed, total count=%d", count)
        await output.put(ReduceMessage(str(count).encode(), keys=keys))


# -- Entrypoint ---------------------------------------------------------------

if __name__ == "__main__":
    mode = os.environ.get("UDF_MODE", "reduce")
    logger.info("Starting in %s mode", mode)
    if mode == "map":
        server = MapServer(passthrough)
        server.start()
    else:
        server = ReduceStreamAsyncServer(Counter)
        server.start()

