from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, PlainTextResponse
from pydantic import BaseModel

# Lives in the container filesystem: survives `docker stop`/`start` and
# `docker compose restart`, wiped when the container is recreated
# (`docker compose down` + `up`).
JOBS_LOG = Path("/app/jobs.log")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class Job(BaseModel):
    id: int
    data: float


@app.post("/jobs")
def receive_job(job: Job):
    output = round(job.data**2, 4)
    with JOBS_LOG.open("a") as f:
        f.write(f"Received Job {job.id} ({job.data}) ........ {output}\n")
    return {"id": job.id, "output": output}


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
