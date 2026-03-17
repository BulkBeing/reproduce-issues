import logging
from collections.abc import AsyncIterable

from pynumaflow.sinker import Datum, Responses, Response, SinkAsyncServer
from simulate_error import maybe_raise

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def sink_handler(datums: AsyncIterable[Datum]) -> Responses:
    """Logs each message and returns success."""
    maybe_raise()
    responses = Responses()
    async for msg in datums:
        logger.info("Sink received: %s", msg.value.decode("utf-8"))
        responses.append(Response.as_success(msg.id))
    return responses


def start():
    grpc_server = SinkAsyncServer(sink_handler)
    logger.info("Starting sink")
    grpc_server.start()
