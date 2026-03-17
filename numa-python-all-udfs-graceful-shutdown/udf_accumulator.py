import logging
from collections.abc import AsyncIterable
from datetime import datetime

from pynumaflow.accumulator import Accumulator, AccumulatorAsyncServer, Message, Datum
from pynumaflow.shared.asynciter import NonBlockingIterator
from simulate_error import maybe_raise

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SimpleAccumulator(Accumulator):
    """Buffers messages and flushes all on watermark advance or timeout."""

    def __init__(self):
        self.latest_wm = datetime.fromtimestamp(-1)
        self.buffer: list[Datum] = []

    async def handler(
        self,
        datums: AsyncIterable[Datum],
        output: NonBlockingIterator,
    ):
        maybe_raise()
        async for datum in datums:
            # If watermark has advanced, flush the buffer
            if datum.watermark and datum.watermark > self.latest_wm:
                self.latest_wm = datum.watermark
                await self._flush(output)
            self.buffer.append(datum)

        # Timeout reached — flush everything
        await self._flush(output)

    async def _flush(self, output: NonBlockingIterator):
        for datum in self.buffer:
            await output.put(Message.from_datum(datum))
        logger.info("Flushed %d messages", len(self.buffer))
        self.buffer.clear()


def start():
    grpc_server = AccumulatorAsyncServer(SimpleAccumulator)
    logger.info("Starting accumulator")
    grpc_server.start()
