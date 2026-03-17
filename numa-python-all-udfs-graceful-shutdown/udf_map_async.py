import logging

from pynumaflow.mapper import Messages, Message, Datum, MapAsyncServer
from simulate_error import maybe_raise

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def map_async_handler(keys: list[str], datum: Datum) -> Messages:
    """Appends [async-map] tag to the message."""
    maybe_raise()
    val = datum.value.decode("utf-8")
    result = f"{val}[async-map]"
    return Messages(Message(value=result.encode(), keys=keys))


def start():
    grpc_server = MapAsyncServer(map_async_handler)
    logger.info("Starting map_async")
    grpc_server.start()
