import logging
from collections.abc import AsyncIterable

from pynumaflow.reducestreamer import (
    Message,
    Datum,
    Metadata,
    ReduceStreamAsyncServer,
    ReduceStreamer,
)
from pynumaflow.shared.asynciter import NonBlockingIterator
from simulate_error import maybe_raise

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StreamingCounter(ReduceStreamer):
    """Counts messages, streams intermediate results every 5 messages."""

    def __init__(self, counter):
        self.counter = counter

    async def handler(
        self,
        keys: list[str],
        datums: AsyncIterable[Datum],
        output: NonBlockingIterator,
        md: Metadata,
    ):
        maybe_raise()
        async for datum in datums:
            self.counter += 1
            logger.info("reduce_stream got: %s", datum.value.decode("utf-8"))
            if self.counter % 5 == 0:
                msg = f"intermediate-count:{self.counter}"
                await output.put(Message(msg.encode(), keys=keys))
        msg = f"final-count:{self.counter}"
        await output.put(Message(msg.encode(), keys=keys))


def start():
    grpc_server = ReduceStreamAsyncServer(StreamingCounter, init_args=(0,))
    logger.info("Starting reduce_stream")
    grpc_server.start()
