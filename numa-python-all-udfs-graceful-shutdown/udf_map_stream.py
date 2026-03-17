import logging
from collections.abc import AsyncIterable

from pynumaflow.mapstreamer import Message, Datum, MapStreamAsyncServer
from simulate_error import maybe_raise

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def map_stream_handler(keys: list[str], datum: Datum) -> AsyncIterable[Message]:
    """Yields single message with [map-stream] tag appended."""
    maybe_raise()
    val = datum.value.decode("utf-8")
    logger.info("map_stream got: %s", val)
    result = f"{val}[map-stream]"
    yield Message(result.encode(), keys=keys)


def start():
    grpc_server = MapStreamAsyncServer(map_stream_handler)
    logger.info("Starting map_stream")
    grpc_server.start()
