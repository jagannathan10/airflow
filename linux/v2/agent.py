#!/usr/bin/env python3
# -------------------------------------------------------------------------
# Unified Airflow Root Agent (TMUX Only)
# Supports:
#   ✔ TMUX Long-Running Jobs
#   ✔ skip_if_running (default=True)
#   ✔ Deterministic Job IDs (for DAG re-runs)
#   ✔ 60-Day Retention Cleanup
#   ✔ OS-safe su - <user> execution
#   ✔ Accurate return_codes + stdout/stderr capture
#   ✔ Fire-and-forget or Poll Mode
# -------------------------------------------------------------------------

import os
import json
import uuid
import time
import subprocess
import threading
from datetime import datetime, timedelta
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ===========================
# CONFIGURATION
# ===========================

BASE_DIR = "/opt/airflow_agent"
JOB_DIR = f"{BASE_DIR}/jobs"
TOKEN_FILE = f"{BASE_DIR}/agent.token"

RETENTION_DAYS = 60
CLEANUP_INTERVAL_SECONDS = 24 * 3600   # once per day

os.makedirs(JOB_DIR, exist_ok=True)

app = FastAPI()


# ===========================
# HELPERS
# ===========================

def read_file(path, default=""):
    try:
        with open(path, "r") as f:
            return f.read()
    except:
        return default

def write_file(path, data):
    with open(path, "w") as f:
        f.write(str(data))

async def validate_token(request: Request):
    if not os.path.exists(TOKEN_FILE):
        return
    expected = read_file(TOKEN_FILE).strip()
    provided = request.headers.get("X-Agent-Token")
    if provided != expected:
        return JSONResponse({"error": "Unauthorized"}, status_code=403)


# ===========================
# PAYLOAD MODEL
# ===========================

class JobRequest(BaseModel):
    job_id: str                # Deterministic from Airflow
    command: str
    run_as_user: str = None
    fire_and_forget: bool = True
    timeout_seconds: int = 86400
    skip_if_running: bool = True   # Default TRUE
    env: dict = {}


# ===========================
# TMUX JOB EXECUTION
# ===========================

def run_tmux_job(job_id, payload: JobRequest):

    job_path = os.path.join(JOB_DIR, job_id)
    os.makedirs(job_path, exist_ok=True)

    status_file = f"{job_path}/status"
    exit_file = f"{job_path}/exit"
    stdout_file = f"{job_path}/stdout.log"
    stderr_file = f"{job_path}/stderr.log"
    session_file = f"{job_path}/tmux_session"

    write_file(status_file, "running")

    # ---------------------
    # Construct command
    # ---------------------
    if payload.run_as_user:
        base = f"su - {payload.run_as_user} -c '{payload.command}'"
    else:
        base = payload.command

    tmux_session = f"agent_{job_id.replace('-', '')}"
    write_file(session_file, tmux_session)

    full_cmd = (
        f"tmux new-session -d -s {tmux_session} "
        f"\"bash -c '{base} > {stdout_file} 2> {stderr_file}; "
        f"echo $? > {exit_file}'\""
    )

    subprocess.call(full_cmd, shell=True)

    # ---------------------
    # Monitor exit_code
    # ---------------------
    while True:
        if os.path.exists(exit_file):
            write_file(status_file, "finished")
            return
        time.sleep(2)


# ===========================
# STATUS CHECK
# ===========================

def get_status(job_id):
    job_path = os.path.join(JOB_DIR, job_id)
    status_file = f"{job_path}/status"
    exit_file = f"{job_path}/exit"
    stdout_file = f"{job_path}/stdout.log"
    stderr_file = f"{job_path}/stderr.log"

    if not os.path.exists(job_path):
        return {"error": "job_not_found"}

    status = read_file(status_file, "unknown").strip()
    rc = None
    if os.path.exists(exit_file):
        try:
            rc = int(read_file(exit_file).strip())
        except:
            rc = None

    return {
        "job_id": job_id,
        "status": status,
        "return_code": rc,
        "stdout": read_file(stdout_file, ""),
        "stderr": read_file(stderr_file, "")
    }


# ===========================
# CLEANUP ENGINE (Retention)
# ===========================

def cleanup_old_jobs():
    now = datetime.utcnow()
    cutoff = now - timedelta(days=RETENTION_DAYS)

    print(f"[CLEANUP] Running retention cleanup (> {RETENTION_DAYS} days)")

    for job_id in os.listdir(JOB_DIR):
        job_path = f"{JOB_DIR}/{job_id}"

        if not os.path.isdir(job_path):
            continue

        try:
            ctime = datetime.utcfromtimestamp(os.path.getctime(job_path))
        except:
            continue

        if ctime > cutoff:
            continue

        status = read_file(f"{job_path}/status", "running").strip()

        if status == "running":
            print(f"[CLEANUP] Skipping active job {job_id}")
            continue

        # Kill tmux session if exists
        tmux_name = read_file(f"{job_path}/tmux_session", "").strip()
        if tmux_name:
            os.system(f"tmux kill-session -t {tmux_name} 2>/dev/null")

        os.system(f"rm -rf {job_path}")
        print(f"[CLEANUP] Deleted old job: {job_id}")


def cleanup_scheduler():
    while True:
        try:
            cleanup_old_jobs()
        except Exception as e:
            print(f"[CLEANUP ERROR] {e}")
        time.sleep(CLEANUP_INTERVAL_SECONDS)


threading.Thread(target=cleanup_scheduler, daemon=True).start()


# ===========================
# API ENDPOINTS
# ===========================

@app.post("/ping")
async def ping(request: Request):
    await validate_token(request)
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.post("/run")
async def run_job(request: Request, payload: JobRequest):
    await validate_token(request)

    job_id = payload.job_id
    job_path = f"{JOB_DIR}/{job_id}"

    # ---------------------------
    # skip_if_running logic
    # ---------------------------
    if payload.skip_if_running and os.path.exists(f"{job_path}/status"):
        existing_status = read_file(f"{job_path}/status").strip()
        if existing_status == "running":
            return JSONResponse({
                "job_id": job_id,
                "status": "already_running"
            })

    # overwrite job folder if old
    os.makedirs(job_path, exist_ok=True)

    # ---------------------------
    # Fire and forget
    # ---------------------------
    if payload.fire_and_forget:
        threading.Thread(
            target=run_tmux_job,
            args=(job_id, payload),
            daemon=True
        ).start()

        return JSONResponse({
            "job_id": job_id,
            "status": "submitted"
        })

    # ---------------------------
    # Poll mode (wait inside agent — optional)
    # ---------------------------
    run_tmux_job(job_id, payload)
    st = get_status(job_id)
    return JSONResponse(st)


@app.get("/status/{job_id}")
async def job_status(job_id: str):
    return get_status(job_id)
