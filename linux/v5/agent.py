#!/usr/bin/env python3
import os
import uuid
import subprocess
import time
from datetime import datetime
from typing import Optional, Dict

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------

BASE_DIR = os.environ.get("AIRFLOW_AGENT_HOME", "/opt/airflow_agent")
JOB_DIR = os.path.join(BASE_DIR, "jobs")
os.makedirs(JOB_DIR, exist_ok=True)

TOKEN_FILE = os.path.join(BASE_DIR, "agent_token")
AGENT_TOKEN = os.environ.get("AIRFLOW_AGENT_TOKEN")
if not AGENT_TOKEN and os.path.exists(TOKEN_FILE):
    AGENT_TOKEN = open(TOKEN_FILE).read().strip()

TMUX_BIN = "/usr/bin/tmux"  # explicit path to tmux on RHEL8/9

app = FastAPI(title="Airflow Root Agent")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------

def write_file(path: str, content: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(str(content))


def read_file(path: str) -> Optional[str]:
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return f.read()


async def validate_token(request: Request):
    """
    Simple header-based token auth.

    Uses header X-Agent-Token and a shared secret from:
      - env AIRFLOW_AGENT_TOKEN  OR
      - file BASE_DIR/agent_token

    If AGENT_TOKEN is unset → token check is disabled (lab only).
    """
    if AGENT_TOKEN is None:
        return
    header = request.headers.get("X-Agent-Token")
    if not header or header != AGENT_TOKEN:
        raise Exception("Invalid token")


# -----------------------------------------------------------------------------
# REQUEST MODEL
# -----------------------------------------------------------------------------

class JobRequest(BaseModel):
    job_type: str = "shell"
    command: str
    run_as_user: Optional[str] = None
    sync: bool = True
    use_tmux: bool = False
    env: Dict[str, str] = {}
    timeout_seconds: Optional[int] = None

    # NEW FIELDS for unified TMUX dedup behaviour
    job_id: Optional[str] = None
    skip_if_running: bool = True
    fire_and_forget: bool = True


# -----------------------------------------------------------------------------
# SYNC EXECUTION (no tmux, wait for completion) - still available but
# you will only be using tmux mode from Airflow.
# -----------------------------------------------------------------------------

def execute_sync(payload: JobRequest) -> Dict:
    if payload.run_as_user:
        cmd_escaped = payload.command.replace("'", "'\"'\"'")
        shell_cmd = f"su - {payload.run_as_user} -c '{cmd_escaped}'"
    else:
        shell_cmd = payload.command

    env = os.environ.copy()
    env.update(payload.env or {})

    result = subprocess.run(
        shell_cmd,
        shell=True,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=payload.timeout_seconds or 999999,
        text=True,
    )

    return {
        "status": "finished",
        "return_code": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


# -----------------------------------------------------------------------------
# ASYNC / TMUX EXECUTION (file-based tracking)
# -----------------------------------------------------------------------------

def build_job_script(job_id: str, payload: JobRequest) -> str:
    job_path = os.path.join(JOB_DIR, job_id)
    os.makedirs(job_path, exist_ok=True)

    log_out = os.path.join(job_path, "stdout.log")
    log_err = os.path.join(job_path, "stderr.log")
    exit_file = os.path.join(job_path, "exit")
    status_file = os.path.join(job_path, "status")

    write_file(status_file, "running")

    if payload.run_as_user:
        cmd_escaped = payload.command.replace("'", "'\"'\"'")
        run_line = (
            f"su - {payload.run_as_user} -c '{cmd_escaped}' "
            f">> '{log_out}' 2>> '{log_err}'"
        )
    else:
        run_line = f"{payload.command} >> '{log_out}' 2>> '{log_err}'"

    script_lines = [
        "#!/bin/bash",
        "set -o pipefail",
        run_line,
        "RC=$?",
        f"echo $RC > '{exit_file}'",
        f"echo finished > '{status_file}'",
    ]

    script_path = os.path.join(job_path, "run.sh")
    with open(script_path, "w", encoding="utf-8") as f:
        f.write("\n".join(script_lines) + "\n")

    os.chmod(script_path, 0o755)
    return script_path


def execute_job(job_id: str, payload: JobRequest):
    job_path = os.path.join(JOB_DIR, job_id)
    os.makedirs(job_path, exist_ok=True)

    script_path = build_job_script(job_id, payload)

    env = os.environ.copy()
    env.update(payload.env or {})

    if payload.use_tmux:
        tmux_session = f"agent_{job_id}"
        cmd = [TMUX_BIN, "new-session", "-d", "-s", tmux_session, script_path]
    else:
        cmd = ["/bin/bash", script_path]

    subprocess.Popen(cmd, env=env)
    # status tracked by files under job_path


# -----------------------------------------------------------------------------
# API ENDPOINTS
# -----------------------------------------------------------------------------

@app.post("/ping")
async def ping(request: Request):
    await validate_token(request)
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.post("/run")
async def run_job(request: Request, payload: JobRequest):
    """
    Unified entry point.

    Our TMUX operator will call with:
      sync=False, use_tmux=True,
      job_id=<deterministic>,
      skip_if_running=True,
      fire_and_forget=False (operator polls /status).
    """
    await validate_token(request)

    # Use provided job_id if present; else generate new
    job_id = payload.job_id or str(uuid.uuid4())
    job_path = os.path.join(JOB_DIR, job_id)

    status_file = os.path.join(job_path, "status")

    # Dedup: if skip_if_running and status == running → don't start new tmux
    if payload.skip_if_running and os.path.exists(status_file):
        existing_status = (read_file(status_file) or "").strip()
        if existing_status == "running":
            return {"job_id": job_id, "status": "already_running"}

    os.makedirs(job_path, exist_ok=True)

    # SYNC MODE (no tmux) – not used by your TMUX operator, but kept for completeness
    if payload.sync and not payload.use_tmux:
        res = execute_sync(payload)
        res["job_id"] = job_id
        return JSONResponse(res)

    # ASYNC / TMUX MODE
    execute_job(job_id, payload)
    return {"job_id": job_id, "status": "submitted"}


@app.get("/status/{job_id}")
async def job_status(request: Request, job_id: str):
    await validate_token(request)

    job_path = os.path.join(JOB_DIR, job_id)
    if not os.path.exists(job_path):
        return {"job_id": job_id, "status": "unknown"}

    status_file = os.path.join(job_path, "status")
    exit_file = os.path.join(job_path, "exit")
    out_file = os.path.join(job_path, "stdout.log")
    err_file = os.path.join(job_path, "stderr.log")

    status = read_file(status_file) or "running"
    exit_code = read_file(exit_file)
    stdout = read_file(out_file) or ""
    stderr = read_file(err_file) or ""

    return {
        "job_id": job_id,
        "status": status,
        "return_code": int(exit_code) if exit_code is not None else None,
        "stdout": stdout,
        "stderr": stderr,
    }


@app.post("/cancel/{job_id}")
async def cancel_job(request: Request, job_id: str):
    await validate_token(request)

    job_path = os.path.join(JOB_DIR, job_id)
    os.makedirs(job_path, exist_ok=True)
    status_file = os.path.join(job_path, "status")

    tmux_session = f"agent_{job_id}"
    try:
        subprocess.call([TMUX_BIN, "kill-session", "-t", tmux_session])
    except Exception:
        pass

    write_file(status_file, "cancelled")
    return {"job_id": job_id, "status": "cancelled"}


@app.on_event("startup")
def startup():
    print(f"[agent] Started. BASE_DIR={BASE_DIR}, JOB_DIR={JOB_DIR}, TMUX={TMUX_BIN}")
