import asyncio
import contextlib
import json
from contextlib import asynccontextmanager
from pathlib import Path

from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, PlainTextResponse

# Lives in the container filesystem: survives `docker stop`/`start` and
# `docker compose restart`, wiped when the container is recreated. Which jobs
# got processed is tracked by the consumer group's committed offsets in Kafka,
# not by this file.
JOBS_LOG = Path("/app/jobs.log")

KAFKA_BROKER = "kafka:9092"
TOPIC = "jobs"
GROUP_ID = "job-processors"


async def consume_jobs():
    # The broker can still be settling right after its healthcheck passes,
    # so keep retrying until we connect.
    while True:
        consumer = AIOKafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        try:
            await consumer.start()
        except Exception as e:
            print(f"Kafka not ready ({e}), retrying in 2s...")
            await asyncio.sleep(2)
            continue
        try:
            async for msg in consumer:
                job = json.loads(msg.value)
                output = round(job["data"] ** 2, 4)
                with JOBS_LOG.open("a") as f:
                    f.write(
                        f"Received Job {job['id']} ({job['data']}) ........ {output}\n"
                    )
                # Commit only after processing: at-least-once, so a crash
                # mid-stream can never lose a job.
                await consumer.commit()
        finally:
            await consumer.stop()


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
