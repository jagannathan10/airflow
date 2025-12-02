#!/usr/bin/env python3
# ============================================================================
# Unified Airflow Agent (Linux TMUX Only)
# - Reads config.xml
# - TMUX long-running jobs
# - skip_if_running = True (default)
# - 60-day retention cleanup
# - CIDR allowlist
# - Command blacklist
# - Rate limiting
# - Deterministic Job IDs from Airflow
# ============================================================================

import os
import re
import yaml
import json
import uuid
import time
import ipaddress
import subprocess
import threading
from datetime import datetime, timedelta

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ----------------------------------------------------------------------------
# Paths
# ----------------------------------------------------------------------------
BASE_DIR = "/opt/airflow_agent"
CONFIG_FILE = f"{BASE_DIR}/config.xml"
JOB_DIR = f"{BASE_DIR}/jobs"

os.makedirs(JOB_DIR, exist_ok=True)

# ----------------------------------------------------------------------------
# Read config.xml
# ----------------------------------------------------------------------------
def load_config():
    if not os.path.exists(CONFIG_FILE):
        raise SystemExit(f"[FATAL] config.xml not found at {CONFIG_FILE}")

    with open(CONFIG_FILE, "r") as f:
        return yaml.safe_load(f)

CONFIG = load_config()

TOKEN = CONFIG.get("token", "").strip()

ALLOWED_IPS = CONFIG.get("allowed_ips", []) or []
COMMAND_BLACKLIST = CONFIG.get("command_blacklist", []) or []

RATE_LIMIT = CONFIG.get("rate_limit", {
    "window_seconds": 60,
    "max_requests": 120
})

TLS_CERT = CONFIG.get("tls", {}).get("server_cert", None)
TLS_KEY = CONFIG.get("tls", {}).get("server_key", None)

SERVER_HOST = CONFIG.get("host", "0.0.0.0")
SERVER_PORT = CONFIG.get("port", 18443)

RETENTION_DAYS = CONFIG.get("retention_days", 60)

print(f"[CONFIG] Loaded config from {CONFIG_FILE}")

# ----------------------------------------------------------------------------
# FastAPI
# ----------------------------------------------------------------------------
app = FastAPI()

# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
def read_file(path, default=""):
    try:
        with open(path, "r") as f:
            return f.read()
    except:
        return default

def write_file(path, data):
    with open(path, "w") as f:
        f.write(str(data))

# ----------------------------------------------------------------------------
# Validate token + IP allowlist + Rate limiting + Command blacklist
# ----------------------------------------------------------------------------

# RATE LIMIT MEMORY
RATE_BUCKET = {}   # { ip: [timestamps...] }

async def validate_request(request: Request, payload=None):
    client_ip = request.client.host

    # ---------------------
    # 1. IP ALLOWLIST CHECK
    # ---------------------
    if ALLOWED_IPS:
        allowed = False
        for cidr in ALLOWED_IPS:
            try:
                if ipaddress.ip_address(client_ip) in ipaddress.ip_network(cidr, strict=False):
                    allowed = True
                    break
            except:
                continue

        if not allowed:
            return JSONResponse({"error": "IP_NOT_ALLOWED", "ip": client_ip}, status_code=403)

    # ---------------------
    # 2. TOKEN CHECK
    # ---------------------
    provided = request.headers.get("X-Agent-Token")
    if TOKEN and provided != TOKEN:
        return JSONResponse({"error": "INVALID_TOKEN"}, status_code=403)

    # ---------------------
    # 3. RATE LIMIT CHECK
    # ---------------------
    now = time.time()
    window = RATE_LIMIT["window_seconds"]
    max_req = RATE_LIMIT["max_requests"]

    RATE_BUCKET.setdefault(client_ip, [])
    RATE_BUCKET[client_ip] = [t for t in RATE_BUCKET[client_ip] if now - t < window]

    if len(RATE_BUCKET[client_ip]) >= max_req:
        return JSONResponse({"error": "RATE_LIMIT_EXCEEDED"}, status_code=429)

    RATE_BUCKET[client_ip].append(now)

    # ---------------------
    # 4. COMMAND BLACKLIST
    # ---------------------
    if payload:
        cmd = payload.command.lower()
        for bad in COMMAND_BLACKLIST:
            if bad.lower() in cmd:
                return JSONResponse({"error": "COMMAND_BLOCKED", "blocked": bad}, status_code=400)

    return None


# ----------------------------------------------------------------------------
# Payload Model
# ----------------------------------------------------------------------------
class JobRequest(BaseModel):
    job_id: str
    command: str
    run_as_user: str = None
    fire_and_forget: bool = True
    skip_if_running: bool = True
    timeout_seconds: int = 86400
    env: dict = {}

# ----------------------------------------------------------------------------
# TMUX Job Execution
# ----------------------------------------------------------------------------
def run_tmux_job(job_id, payload: JobRequest):

    job_path = f"{JOB_DIR}/{job_id}"
    os.makedirs(job_path, exist_ok=True)

    status_file = f"{job_path}/status"
    exit_file = f"{job_path}/exit"
    stdout_file = f"{job_path}/stdout.log"
    stderr_file = f"{job_path}/stderr.log"
    session_file = f"{job_path}/tmux_session"

    write_file(status_file, "running")

    # Build base shell command
    if payload.run_as_user:
        base = f"su - {payload.run_as_user} -c '{payload.command}''"
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

    # Monitor until exit file appears
    while True:
        if os.path.exists(exit_file):
            write_file(status_file, "finished")
            return
        time.sleep(2)

# ----------------------------------------------------------------------------
# Status
# ----------------------------------------------------------------------------
def get_status(job_id):
    job_path = f"{JOB_DIR}/{job_id}"
    if not os.path.exists(job_path):
        return {"error": "job_not_found"}

    status = read_file(f"{job_path}/status", "unknown").strip()
    rc = None
    if os.path.exists(f"{job_path}/exit"):
        try:
            rc = int(read_file(f"{job_path}/exit").strip())
        except:
            rc = None

    return {
        "job_id": job_id,
        "status": status,
        "return_code": rc,
        "stdout": read_file(f"{job_path}/stdout.log", ""),
        "stderr": read_file(f"{job_path}/stderr.log", "")
    }

# ----------------------------------------------------------------------------
# Retention Cleanup
# ----------------------------------------------------------------------------
def cleanup_old_jobs():
    cutoff = datetime.utcnow() - timedelta(days=RETENTION_DAYS)
    print("[CLEANUP] Starting retention scan")

    for job_id in os.listdir(JOB_DIR):
        job_path = f"{JOB_DIR}/{job_id}"
        if not os.path.isdir(job_path):
            continue

        ctime = datetime.utcfromtimestamp(os.path.getctime(job_path))

        # new job
        if ctime > cutoff:
            continue

        status = read_file(f"{job_path}/status", "running").strip()
        if status == "running":
            continue

        # kill tmux if needed
        tmux_name = read_file(f"{job_path}/tmux_session", "").strip()
        if tmux_name:
            os.system(f"tmux kill-session -t {tmux_name} 2>/dev/null")

        os.system(f"rm -rf {job_path}")
        print(f"[CLEANUP] Deleted old job {job_id}")

def cleanup_scheduler():
    while True:
        try:
            cleanup_old_jobs()
        except Exception as e:
            print(f"[CLEANUP ERROR] {e}")
        time.sleep(86400)

threading.Thread(target=cleanup_scheduler, daemon=True).start()

# ----------------------------------------------------------------------------
# API
# ----------------------------------------------------------------------------
@app.post("/ping")
async def ping(request: Request):
    error = await validate_request(request)
    if error:
        return error
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.post("/run")
async def run_job_endpoint(request: Request, payload: JobRequest):
    error = await validate_request(request, payload)
    if error:
        return error

    job_id = payload.job_id
    job_path = f"{JOB_DIR}/{job_id}"

    # skip_if_running
    if payload.skip_if_running and os.path.exists(f"{job_path}/status"):
        if read_file(f"{job_path}/status").strip() == "running":
            return {"job_id": job_id, "status": "already_running"}

    os.makedirs(job_path, exist_ok=True)

    if payload.fire_and_forget:
        threading.Thread(
            target=run_tmux_job,
            args=(job_id, payload),
            daemon=True
        ).start()
        return {"job_id": job_id, "status": "submitted"}

    # wait mode
    run_tmux_job(job_id, payload)
    return get_status(job_id)


@app.get("/status/{job_id}")
async def status(job_id: str):
    return get_status(job_id)
