import logging
from collections.abc import AsyncIterable

from simulate_error import maybe_raise
from pynumaflow.batchmapper import (
    Message,
    Datum,
    BatchMapper,
    BatchMapAsyncServer,
    BatchResponses,
    BatchResponse,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TagBatchMap(BatchMapper):
    """Appends [batch-map] tag to each message in the batch."""

    async def handler(self, datums: AsyncIterable[Datum]) -> BatchResponses:
        maybe_raise()
        batch_responses = BatchResponses()
        async for datum in datums:
            val = datum.value.decode("utf-8")
            logger.info("batch_map got: %s", val)
            result = f"{val}[batch-map]"
            batch_response = BatchResponse.from_id(datum.id)
            batch_response.append(Message(result.encode()))
            batch_responses.append(batch_response)
        return batch_responses


def start():
    grpc_server = BatchMapAsyncServer(TagBatchMap())
    logger.info("Starting batch_map")
    grpc_server.start()
