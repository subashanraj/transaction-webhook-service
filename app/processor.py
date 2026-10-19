import asyncio
import random

import asyncpg


class SimulatedExternalError(Exception):
    pass


async def simulate_external_call(row: asyncpg.Record, delay_seconds: float, failure_rate: float) -> None:
    """Stand-in for the real downstream calls (ledger, fraud check, ...).

    asyncio.sleep yields the event loop, so thousands of these can be in flight
    without slowing the webhook endpoint. A real implementation would pass
    row["transaction_id"] as the downstream idempotency key, because a crash
    after this call but before the final UPDATE makes it run again.
    """
    await asyncio.sleep(delay_seconds)
    if failure_rate and random.random() < failure_rate:
        raise SimulatedExternalError("simulated external API failure")
