import os
import time

_START_TIME = time.monotonic()
_RAISE = os.getenv("RAISE_UDF_EXCEPTION", "").lower() == "true"
_DELAY_SECS = 60


def maybe_raise():
    """Raise after 60s of process uptime if RAISE_UDF_EXCEPTION=true."""
    if _RAISE and (time.monotonic() - _START_TIME) >= _DELAY_SECS:
        raise RuntimeError("Simulated exception from UDF after 60s")
