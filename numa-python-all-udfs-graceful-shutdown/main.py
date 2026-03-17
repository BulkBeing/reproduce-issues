import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

udf_type = os.getenv("NUMAFLOW_UDF_TYPE", "")
logger.info("Starting UDF type: %s", udf_type)

if udf_type == "sourcer":
    from udf_sourcer import start
elif udf_type == "source_transform":
    from udf_source_transform import start
elif udf_type == "map_async":
    from udf_map_async import start
elif udf_type == "batch_map":
    from udf_batch_map import start
elif udf_type == "map_stream":
    from udf_map_stream import start
elif udf_type == "reduce":
    from udf_reduce import start
elif udf_type == "reduce_stream":
    from udf_reduce_stream import start
elif udf_type == "accumulator":
    from udf_accumulator import start
elif udf_type == "map_multiproc":
    from udf_map_multiproc import start
elif udf_type == "map_sync":
    from udf_map_sync import start
elif udf_type == "side_input":
    from udf_side_input import start
elif udf_type == "sink":
    from udf_sink import start
else:
    raise ValueError(f"Unknown NUMAFLOW_UDF_TYPE: {udf_type!r}")

start()
