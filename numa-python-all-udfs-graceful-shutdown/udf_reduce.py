import logging
from collections.abc import AsyncIterable

from pynumaflow.reducer import Messages, Message, Datum, Metadata, ReduceAsyncServer
from simulate_error import maybe_raise

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def reduce_handler(keys: list[str], datums: AsyncIterable[Datum], md: Metadata) -> Messages:
    """Counts messages in the window, emits count with window boundaries."""
    maybe_raise()
    interval_window = md.interval_window
    counter = 0
    async for _ in datums:
        counter += 1
    msg = (
        f"count:{counter} window:{interval_window.start}-{interval_window.end}"
    )
    return Messages(Message(msg.encode(), keys=keys))


def start():
    grpc_server = ReduceAsyncServer(reduce_handler)
    logger.info("Starting reduce")
    grpc_server.start()
