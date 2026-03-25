import asyncio
import logging
import os

from datetime import datetime

from orchastrator import Orchestrator, Task, TaskResult
from common.db import create_conn_pool

import handlers
from recovery import run_recovery_loop, run_stale_recovery_once

# Create logs dir and timestamped file
os.makedirs("/logs", exist_ok=True)
_log_filename = "order-" + datetime.now().strftime("%y%m%d-%H%M%S") + ".log"
_log_path = os.path.join("/logs", _log_filename)

# Root config: write to both stdout and file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(),           # keeps docker compose logs -f working
        logging.FileHandler(_log_path),    # writes to /logs/YYMMDD-HHMMSS.log
    ]
)

# Silence noisy kafka loggers
for _noisy in ("kafka", "kafka.conn", "kafka.client", "kafka.consumer", "kafka.producer"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)
logger = logging.getLogger(__name__)



# --- Configuration ---
KAFKA_SERVER = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/data/checkpoints")

# Initialize Orchestrator
orch = Orchestrator(
    consumer_config={"bootstrap.servers": KAFKA_SERVER, "group.id": "payment-service"},
    producer_config={"bootstrap.servers": KAFKA_SERVER},
    checkpoint_dir=CHECKPOINT_DIR,
    dlq_topic="payment.dlq",
    max_concurrency=50
)

# Shared Resources
db_pool = create_conn_pool("PAYMENT")

# --- Handlers ---

@orch.handler("gateway.payment")
async def handle_gateway(task: Task) -> TaskResult:
    """Handles HTTP-proxy requests from the Gateway."""
    # We wrap blocking DB calls in a thread to avoid freezing the orchestrator
    loop = asyncio.get_event_loop()
    status_code, body = await loop.run_in_executor(
        None, handlers.handle_gateway_logic, task.value, db_pool, task.key
    )
    
    # Gateway expects correlation_id in the body
    if isinstance(body, dict):
        body["correlation_id"] = task.key
        
    return TaskResult.ok(output_topic="gateway.responses", output_value=body)

@orch.handler("internal.payment.tpc")
async def handle_tpc(task: Task) -> TaskResult:
    """Handles 2PC (Prepare/Commit/Rollback)."""
    loop = asyncio.get_event_loop()
    result_payload = await loop.run_in_executor(
        None, handlers.handle_tpc_logic, task.value, db_pool
    )
    return TaskResult.ok(output_topic="internal.responses", output_value=result_payload)

@orch.handler("internal.payment.saga")
async def handle_saga(task: Task) -> TaskResult:
    """Handles SAGA (Execute/Rollback)."""
    loop = asyncio.get_event_loop()
    result_payload = await loop.run_in_executor(
        None, handlers.handle_saga_logic, task.value, db_pool
    )
    return TaskResult.ok(output_topic="internal.responses", output_value=result_payload)

@orch.handler("payment.dlq")
async def handle_dfq(task: Task) -> None:
    logger.warning(f"Received msg in dlq: {task}, dict format: {task.to_checkpoint()}")

async def main():
    # Run recovery once, on startup and await it before continuing setting up
    await run_stale_recovery_once(db_pool, orch)

    # Start the recovery background task (2PC cleanup)
    asyncio.create_task(run_recovery_loop(db_pool, orch))
    
    logger.info("Payment service starting Orchestrator loop...")
    await orch.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass