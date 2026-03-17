import logging
from datetime import datetime

from pynumaflow.sourcetransformer import Messages, Message, Datum, SourceTransformAsyncServer
from simulate_error import maybe_raise

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def source_transform_handler(keys: list[str], datum: Datum) -> Messages:
    """Pass through with event_time set to now."""
    maybe_raise()
    val = datum.value
    return Messages(Message(value=val, event_time=datetime.now(), keys=keys))


def start():
    grpc_server = SourceTransformAsyncServer(source_transform_handler)
    logger.info("Starting source_transform")
    grpc_server.start()
