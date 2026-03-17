import logging
import os

from pynumaflow.mapper import Messages, Message, Datum, Mapper, MapMultiprocServer
from simulate_error import maybe_raise

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MultiProcMapper(Mapper):
    """Appends [multiproc] tag to the message."""

    def handler(self, keys: list[str], datum: Datum) -> Messages:
        maybe_raise()
        val = datum.value.decode("utf-8")
        result = f"{val}[multiproc]"
        return Messages(Message(value=result.encode(), keys=keys))


def start():
    server_count = int(os.getenv("NUM_CPU_MULTIPROC", "2"))
    grpc_server = MapMultiprocServer(MultiProcMapper(), server_count=server_count)
    logger.info("Starting map_multiproc with %d processes", server_count)
    grpc_server.start()
