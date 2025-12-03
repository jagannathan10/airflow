#!/usr/bin/env python3
# ============================================================================
#  AIRFLOW UNIFIED TMUX AGENT (FINAL VERSION)
#  - TMUX fixed using bash -lc (critical for SELinux + su - <user>)
#  - skip_if_running enabled by default
#  - deterministic job_id
#  - RHEL 8/9 compatible
#  - Works ONLY if run as root (so it can su - <user>)
# ============================================================================

import os
import json
import uuid
import subprocess
import time
import threading
from datetime import datetime, timedelta
from typing import Optional, Dict

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ============================================================================
# CONFIG LOADING
# ============================================================================

BASE_DIR = "/opt/airflow_agent"
CONFIG_FILE = f"{BASE_DIR}/config.xml"
JOB_DIR = f"{BASE_DIR}/jobs"
os.makedirs(JOB_DIR, exist_ok=True)

DEFAULT_CONFIG = {
    "token": None,
    "allowed_ips": [],
    "command_blacklist": [],
    "rate_limit": {"window_seconds": 60, "max_requests": 120},
    "tls": {
        "server_cert": f"{BASE_DIR}/certs/cert.pem",
        "server_key": f"{BASE_DIR}/certs/key.pem",
    },
    "server": {"host": "0.0.0.0", "port": 18443}
}

def load_config():
    if not os.path.exists(CONFIG_FILE):
        return DEFAULT_CONFIG
    try:
        with open(CONFIG_FILE) as f:
            return json.load(f)
    except:
        return DEFAULT_CONFIG

CONFIG = load_config()
AGENT_TOKEN = CONFIG.get("token")
ALLOWED_IPS = CONFIG.get("allowed_ips", [])
BLACKLIST = CONFIG.get("command_blacklist", [])
SERVER_CFG = CONFIG.get("server", {})

TMUX_BIN = "/usr/bin/tmux"     # RHEL safe path

# ============================================================================
# FASTAPI
# ============================================================================

app = FastAPI(title="Airflow Unified TMUX Agent")

# ============================================================================
# HELPERS
# ============================================================================

def write_file(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(str(data))

def read_file(path):
    try:
        with open(path) as f:
            return f.read()
    except:
        return None

def is_ip_allowed(ip):
    if not ALLOWED_IPS:  # if list empty -> allow all
        return True
    return ip in ALLOWED_IPS

async def validate_token(request: Request):
    """Token + IP allowlist security."""
    client_ip = request.client.host

    if not is_ip_allowed(client_ip):
        raise HTTPException(status_code=403, detail="IP not allowed")

    if not AGENT_TOKEN:
        return

    provided = request.headers.get("X-Agent-Token")
    if provided != AGENT_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token")


def command_blocked(cmd):
    for pattern in BLACKLIST:
        if pattern.lower() in cmd.lower():
            return True
    return False

# ============================================================================
# REQUEST PAYLOAD
# ============================================================================

class JobRequest(BaseModel):
    command: str
    run_as_user: Optional[str] = None
    job_id: Optional[str] = None
    skip_if_running: bool = True
    fire_and_forget: bool = True  # Operator polls
    use_tmux: bool = True         # ALWAYS TMUX
    env: Dict[str, str] = {}

# ============================================================================
# CORE JOB EXECUTION
# ============================================================================

def build_job_script(job_id: str, payload: JobRequest) -> str:
    """Creates run.sh which handles su, stdout, stderr, exit code."""
    job_path = f"{JOB_DIR}/{job_id}"
    os.makedirs(job_path, exist_ok=True)

    out_log = f"{job_path}/stdout.log"
    err_log = f"{job_path}/stderr.log"
    exit_file = f"{job_path}/exit"
    status_file = f"{job_path}/status"

    write_file(status_file, "running")

    if payload.run_as_user:
        escaped = payload.command.replace("'", "'\"'\"'")
        run_line = f"su - {payload.run_as_user} -c '{escaped}' >>'{out_log}' 2>>'{err_log}'"
    else:
        run_line = f"{payload.command} >>'{out_log}' 2>>'{err_log}'"

    script = f"""\n#!/bin/bash
set -o pipefail
{run_line}
RC=$?
echo $RC > '{exit_file}'
echo finished > '{status_file}'
"""
    script_path = f"{job_path}/run.sh"
    write_file(script_path, script)
    os.chmod(script_path, 0o755)

    return script_path


def execute_tmux(job_id: str, payload: JobRequest):
    """Runs job inside TMUX using bash -lc (critical fix!)."""

    script_path = build_job_script(job_id, payload)
    job_path = f"{JOB_DIR}/{job_id}"
    session_name = f"agent_{job_id}"

    # Create tmux session properly using bash login shell
    # (fixes SELinux + su - user behavior)
    cmd = [
        TMUX_BIN, "new-session", "-d",
        "-s", session_name,
        "bash", "-lc", f"'{script_path}'"
    ]

    subprocess.Popen(cmd)

# ============================================================================
# STATUS READER
# ============================================================================

def get_status(job_id: str):
    job_path = f"{JOB_DIR}/{job_id}"
    if not os.path.exists(job_path):
        return {"job_id": job_id, "status": "unknown"}

    status = read_file(f"{job_path}/status") or "running"
    exit_code = read_file(f"{job_path}/exit")
    out = read_file(f"{job_path}/stdout.log") or ""
    err = read_file(f"{job_path}/stderr.log") or ""

    return {
        "job_id": job_id,
        "status": status.strip(),
        "return_code": int(exit_code) if exit_code else None,
        "stdout": out,
        "stderr": err
    }

# ============================================================================
# RETENTION CLEANUP (60 DAYS)
# ============================================================================

RETENTION_DAYS = 60

def cleanup_old_jobs():
    cutoff = datetime.utcnow() - timedelta(days=RETENTION_DAYS)
    for jobid in os.listdir(JOB_DIR):
        job_path = f"{JOB_DIR}/{jobid}"
        try:
            ts = datetime.utcfromtimestamp(os.path.getctime(job_path))
        except:
            continue
        if ts < cutoff:
            os.system(f"rm -rf '{job_path}'")

def cleanup_thread():
    while True:
        cleanup_old_jobs()
        time.sleep(24 * 3600)

threading.Thread(target=cleanup_thread, daemon=True).start()

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.post("/ping")
async def ping(request: Request):
    await validate_token(request)
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.post("/run")
async def run_job(request: Request, payload: JobRequest):
    await validate_token(request)

    if command_blocked(payload.command):
        raise HTTPException(400, "Command blocked by blacklist")

    job_id = payload.job_id or str(uuid.uuid4())
    job_path = f"{JOB_DIR}/{job_id}"

    # Dedup logic
    status_file = f"{job_path}/status"
    if payload.skip_if_running and os.path.exists(status_file):
        if (read_file(status_file) or "").strip() == "running":
            return {"job_id": job_id, "status": "already_running"}

    os.makedirs(job_path, exist_ok=True)

    # Launch TMUX job
    execute_tmux(job_id, payload)

    return {"job_id": job_id, "status": "submitted"}


@app.get("/status/{job_id}")
async def job_status(job_id: str):
    return get_status(job_id)


@app.post("/cancel/{job_id}")
async def cancel_job(job_id: str):
    session = f"agent_{job_id}"
    os.system(f"{TMUX_BIN} kill-session -t {session} 2>/dev/null")
    write_file(f"{JOB_DIR}/{job_id}/status", "cancelled")
    return {"job_id": job_id, "status": "cancelled"}


@app.on_event("startup")
def startup():
    print(f"[agent] Running - BASE={BASE_DIR}, TMUX={TMUX_BIN}")
