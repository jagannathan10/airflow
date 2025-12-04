#!/usr/bin/env python3
# -------------------------------------------------------------------------
# Unified Airflow Agent (TMUX-only with parallel-safe dedup)
# -------------------------------------------------------------------------

import os
import subprocess
import time
from datetime import datetime
from typing import Optional, Dict

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# -------------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------------

BASE_DIR = "/opt/airflow_agent"
JOB_DIR = f"{BASE_DIR}/jobs"
TMUX_BIN = "/usr/bin/tmux"

os.makedirs(JOB_DIR, exist_ok=True)

TOKEN_FILE = f"{BASE_DIR}/agent_token"
AGENT_TOKEN = None
if os.path.exists(TOKEN_FILE):
    AGENT_TOKEN = open(TOKEN_FILE).read().strip()

# -------------------------------------------------------------------------
# FASTAPI APP
# -------------------------------------------------------------------------

app = FastAPI(title="Unified Airflow Agent")


# -------------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------------

def write(path, content):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(str(content))


def read(path, default=""):
    if not os.path.exists(path):
        return default
    return open(path).read()


async def validate_token(req: Request):
    if AGENT_TOKEN is None:
        return
    provided = req.headers.get("X-Agent-Token")
    if provided != AGENT_TOKEN:
        raise Exception("Invalid token")


def tmux_session_name(job_id):
    return f"agent_{job_id}"


def is_tmux_alive(job_id):
    session = tmux_session_name(job_id)
    return subprocess.call(
        [TMUX_BIN, "has-session", "-t", session],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    ) == 0


# -------------------------------------------------------------------------
# REQUEST MODEL
# -------------------------------------------------------------------------

class JobRequest(BaseModel):
    command: str
    run_as_user: Optional[str] = None
    job_id: str
    skip_if_running: bool = True
    fire_and_forget: bool = False
    env: Dict[str, str] = {}
    timeout_seconds: Optional[int] = None


# -------------------------------------------------------------------------
# JOB EXECUTION
# -------------------------------------------------------------------------

def build_script(job_id: str, payload: JobRequest):

    job_path = f"{JOB_DIR}/{job_id}"
    os.makedirs(job_path, exist_ok=True)

    stdout = f"{job_path}/stdout.log"
    stderr = f"{job_path}/stderr.log"
    exit_file = f"{job_path}/exit"
    status = f"{job_path}/status"

    write(status, "starting")

    if payload.run_as_user:
        cmd_escaped = payload.command.replace("'", "'\"'\"'")
        run_line = f"su - {payload.run_as_user} -c '{cmd_escaped}'"
    else:
        run_line = payload.command

    script = f"{job_path}/run.sh"
    with open(script, "w") as f:
        f.write(f"""#!/bin/bash
set -o pipefail
{run_line} >> "{stdout}" 2>> "{stderr}"
RC=$?
echo $RC > "{exit_file}"
echo finished > "{status}"
""")

    os.chmod(script, 0o755)
    return script


def run_job(job_id: str, payload: JobRequest):
    """Launch job inside TMUX session."""
    script = build_script(job_id, payload)

    session = tmux_session_name(job_id)

    # Start TMUX session
    subprocess.call([
        TMUX_BIN, "new-session", "-d",
        "-s", session,
        "bash", "-lc", f"'{script}'"
    ])

    # Update status to running *after* tmux starts
    write(f"{JOB_DIR}/{job_id}/status", "running")


# -------------------------------------------------------------------------
# API ENDPOINTS
# -------------------------------------------------------------------------

@app.post("/ping")
async def ping(req: Request):
    await validate_token(req)
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.post("/run")
async def run(req: Request, payload: JobRequest):
    await validate_token(req)
    job_id = payload.job_id

    # Parallel-safe dedup check
    if payload.skip_if_running and is_tmux_alive(job_id):
        return {"job_id": job_id, "status": "already_running"}

    # Start new job
    run_job(job_id, payload)
    return {"job_id": job_id, "status": "submitted"}


@app.get("/status/{job_id}")
async def status(req: Request, job_id: str):
    await validate_token(req)

    job_path = f"{JOB_DIR}/{job_id}"
    status_file = f"{job_path}/status"
    exit_file = f"{job_path}/exit"
    stdout_file = f"{job_path}/stdout.log"
    stderr_file = f"{job_path}/stderr.log"

    status = read(status_file, "unknown").strip()
    exit_code = read(exit_file).strip() if os.path.exists(exit_file) else None

    if exit_code:
        try:
            exit_code = int(exit_code)
        except:
            exit_code = None

    return {
        "job_id": job_id,
        "status": status,
        "return_code": exit_code,
        "stdout": read(stdout_file, ""),
        "stderr": read(stderr_file, ""),
    }


@app.post("/cancel/{job_id}")
async def cancel(req: Request, job_id: str):
    await validate_token(req)
    session = tmux_session_name(job_id)

    subprocess.call([TMUX_BIN, "kill-session", "-t", session])
    write(f"{JOB_DIR}/{job_id}/status", "cancelled")

    return {"job_id": job_id, "status": "cancelled"}


@app.on_event("startup")
def startup():
    print(f"[AGENT] Started. Jobs={JOB_DIR}, tmux={TMUX_BIN}")
