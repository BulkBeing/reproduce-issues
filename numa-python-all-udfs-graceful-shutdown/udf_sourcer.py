import uuid
import logging
from datetime import datetime

from pynumaflow.shared.asynciter import NonBlockingIterator
from pynumaflow.sourcer import (
    ReadRequest,
    Message,
    AckRequest,
    PendingResponse,
    Offset,
    PartitionsResponse,
    get_default_partitions,
    Sourcer,
    SourceAsyncServer,
    NackRequest,
)

from simulate_error import maybe_raise

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SimpleSource(Sourcer):
    """Generates sequential string messages for the testing-all pipeline."""

    def __init__(self):
        self.read_idx: int = 0
        self.to_ack_set: set[int] = set()
        self.nacked: set[int] = set()

    async def read_handler(self, datum: ReadRequest, output: NonBlockingIterator):
        maybe_raise()
        if self.to_ack_set:
            return

        for _ in range(datum.num_records):
            if self.nacked:
                idx = self.nacked.pop()
            else:
                idx = self.read_idx
                self.read_idx += 1
            payload = f"msg-{idx}-{uuid.uuid4().hex[:8]}"
            await output.put(
                Message(
                    payload=payload.encode(),
                    offset=Offset.offset_with_default_partition_id(str(idx).encode()),
                    event_time=datetime.now(),
                    keys=["all"],
                )
            )
            self.to_ack_set.add(idx)

    async def ack_handler(self, ack_request: AckRequest):
        for req in ack_request.offsets:
            self.to_ack_set.discard(int(req.offset))

    async def nack_handler(self, ack_request: NackRequest):
        for req in ack_request.offsets:
            offset = int(req.offset)
            self.to_ack_set.discard(offset)
            self.nacked.add(offset)
        logger.info("Nacked offsets: %s", self.nacked)

    async def pending_handler(self) -> PendingResponse:
        return PendingResponse(count=0)

    async def partitions_handler(self) -> PartitionsResponse:
        return PartitionsResponse(partitions=get_default_partitions())


def start():
    grpc_server = SourceAsyncServer(SimpleSource())
    logger.info("Starting sourcer")
    grpc_server.start()
