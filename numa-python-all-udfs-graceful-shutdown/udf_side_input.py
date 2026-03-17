import datetime
import logging

from pynumaflow.sideinput import Response, SideInputServer, SideInput
from simulate_error import maybe_raise

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TickerSideInput(SideInput):
    """Returns a timestamp string as side-input data every invocation."""

    def retrieve_handler(self) -> Response:
        maybe_raise()
        val = f"ticker:{datetime.datetime.now().isoformat()}"
        logger.info("Broadcasting side-input: %s", val)
        return Response.broadcast_message(val.encode("utf-8"))


def start():
    grpc_server = SideInputServer(TickerSideInput())
    logger.info("Starting side_input")
    grpc_server.start()
