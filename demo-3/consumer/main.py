import asyncio
import contextlib
import json
from contextlib import asynccontextmanager
from pathlib import Path

import aio_pika
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, PlainTextResponse

# Lives in the container filesystem: survives `docker stop`/`start` and
# `docker compose restart`, wiped when the container is recreated. Which jobs
# got processed is tracked by RabbitMQ's consumer acks, not by this file.
JOBS_LOG = Path("/app/jobs.log")

RABBITMQ_URL = "amqp://guest:guest@rabbitmq/"
QUEUE_NAME = "jobs"


async def consume_jobs():
    # The broker can still be settling right after its healthcheck passes,
    # so keep retrying until we connect.
    while True:
        try:
            connection = await aio_pika.connect_robust(RABBITMQ_URL)
        except Exception as e:
            print(f"RabbitMQ not ready ({e}), retrying in 2s...")
            await asyncio.sleep(2)
            continue

        async with connection:
            channel = await connection.channel()

            # prefetch_count=1 means RabbitMQ won't send a new message until
            # the current one is acked — equivalent to at-least-once processing.
            await channel.set_qos(prefetch_count=1)

            queue = await channel.declare_queue(QUEUE_NAME, durable=True)

            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        # process() auto-acks on success, nacks on exception —
                        # equivalent to the manual commit pattern in Kafka.
                        job = json.loads(message.body)
                        output = round(job["data"] ** 2, 4)
                        with JOBS_LOG.open("a") as f:
                            f.write(
                                f"Received Job {job['id']} ({job['data']}) ........ {output}\n"
                            )


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(consume_jobs())
    yield
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


app = FastAPI(lifespan=lifespan)


@app.get("/log", response_class=PlainTextResponse)
def job_log():
    return JOBS_LOG.read_text() if JOBS_LOG.exists() else ""


# Updates the <pre> in place instead of reloading the page, so the scroll
# position is preserved while new jobs stream in.
@app.get("/", response_class=HTMLResponse)
def show_jobs():
    return """<html><body><pre id="log"></pre><script>
async function refresh() {
  const res = await fetch("/log");
  document.getElementById("log").textContent = await res.text();
}
refresh();
setInterval(refresh, 1000);
</script></body></html>"""
