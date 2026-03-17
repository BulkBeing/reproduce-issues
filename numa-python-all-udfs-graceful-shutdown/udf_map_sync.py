import logging
import os

from pynumaflow.mapper import Messages, Message, Datum, MapServer
from simulate_error import maybe_raise

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Path where Numaflow writes side-input data
SIDE_INPUT_DIR = "/var/numaflow/side-inputs"
SIDE_INPUT_NAME = "ticker"


def _read_side_input() -> str:
    """Read the side-input file if available."""
    path = os.path.join(SIDE_INPUT_DIR, SIDE_INPUT_NAME)
    try:
        with open(path, "rb") as f:
            return f.read().decode("utf-8")
    except FileNotFoundError:
        return ""


def map_sync_handler(keys: list[str], datum: Datum) -> Messages:
    """Appends [sync-map] and side-input value to the message."""
    maybe_raise()
    val = datum.value.decode("utf-8")
    side_input = _read_side_input()
    suffix = f"[sync-map][si={side_input}]" if side_input else "[sync-map]"
    result = f"{val}{suffix}"
    return Messages(Message(value=result.encode(), keys=keys))


def start():
    grpc_server = MapServer(map_sync_handler)
    logger.info("Starting map_sync")
    grpc_server.start()
