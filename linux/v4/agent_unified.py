#!/usr/bin/env python3
"""
Airflow Unified Agent (Linux + Windows)

- Single API: /ping (POST), /run (POST), /status/{job_id} (GET)
- Linux:
    * Uses tmux sessions for long-running jobs
    * root -> run_as_user via `su - user -c ...`
- Windows:
    * Uses detached cmd.exe background processes (no tmux)
- Security:
    * Token via X-Agent-Token (from config.xml)
    * allowed_ips (CIDR or single IP) from config.xml
    * command_blacklist from config.xml
    * Simple per-IP rate limiting from config.xml
- Jobs:
    * Deterministic job_id or forced_job_id allowed
    * skip_if_running=True by default
    * Stores status, exit code, stdout.log, stderr.log
    * 60-day cleanup (skips jobs still marked "running")
- TLS:
    * host, port, server_cert, server_key read from config.xml
    * uvicorn started programmatically in __main__
"""

import os
import platform
import uuid
import subprocess
import threading
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

import ipaddress
import yaml
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# --------------------------------------------------------------------
# OS & PATHS
# --------------------------------------------------------------------
IS_WINDOWS = platform.system().lower().startswith("windows")

if IS_WINDOWS:
    BASE_DIR = os.environ.get("AIRFLOW_AGENT_BASE", r"C:\airflow_agent")
else:
    BASE_DIR = os.environ.get("AIRFLOW_AGENT_BASE", "/opt/airflow_agent")

JOB_DIR = os.path.join(BASE_DIR, "jobs")
DEFAULT_CONFIG_PATH = os.path.join(BASE_DIR, "config.xml")
os.makedirs(JOB_DIR, exist_ok=True)

CONFIG_PATH = os.environ.get("AIRFLOW_AGENT_CONFIG", DEFAULT_CONFIG_PATH)

# --------------------------------------------------------------------
# GLOBAL CONFIG LOAD
# --------------------------------------------------------------------
def load_config(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise RuntimeError(f"Config file not found: {path}")
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return yaml.safe_load(f) or {}


config = load_config(CONFIG_PATH)

# SECURITY SETTINGS FROM CONFIG
CONFIG_TOKEN = config.get("token", "").strip() or None
ALLOWED_IPS_RAW = config.get("allowed_ips", []) or []
COMMAND_BLACKLIST = config.get("command_blacklist", []) or []

RL_CONF = config.get("rate_limit", {}) or {}
RATE_WINDOW_SECONDS = int(RL_CONF.get("window_seconds", 60))
RATE_MAX_REQUESTS = int(RL_CONF.get("max_requests", 120))

# NETWORK + TLS SETTINGS
NET_CONF = config.get("network", {}) or {}
TLS_CONF = config.get("tls", {}) or {}

HOST = NET_CONF.get("host", "0.0.0.0")
PORT = int(NET_CONF.get("port", 18443))

TLS_CERT = TLS_CONF.get(
    "server_cert",
    "/opt/airflow_agent/certs/cert.pem" if not IS_WINDOWS else r"C:\airflow_agent\certs\cert.pem",
)
TLS_KEY = TLS_CONF.get(
    "server_key",
    "/opt/airflow_agent/certs/key.pem" if not IS_WINDOWS else r"C:\airflow_agent\certs\key.pem",
)

# INTERNAL TOKEN (fallback if config has no token)
INTERNAL_AGENT_TOKEN = "scb-airflowagent-CHANGE-ME-TO-LONG-RANDOM-SECRET"

# TOKEN PRIORITY: config.xml > env > internal
AGENT_TOKEN = CONFIG_TOKEN or os.environ.get("AGENT_TOKEN") or INTERNAL_AGENT_TOKEN

# RETENTION / CLEANUP
RETENTION_DAYS = 60
CLEANUP_INTERVAL_SECONDS = 24 * 3600  # once per day

# --------------------------------------------------------------------
# IP & RATE LIMIT STATE
# --------------------------------------------------------------------
# Compile allowed IP networks
_ALLOWED_NETWORKS = []
for entry in ALLOWED_IPS_RAW:
    try:
        # supports "1.2.3.4" and "1.2.3.0/24"
        if "/" in entry:
            _ALLOWED_NETWORKS.append(ipaddress.ip_network(entry, strict=False))
        else:
            _ALLOWED_NETWORKS.append(ipaddress.ip_network(entry + "/32", strict=False))
    except Exception:
        # ignore malformed entries
        pass

# Simple in-memory rate limiter: ip -> (window_start_ts, count)
_rate_lock = threading.Lock()
_rate_state: Dict[str, Any] = {}

# --------------------------------------------------------------------
# FASTAPI APP
# --------------------------------------------------------------------
app = FastAPI(title="Airflow Unified Agent")


# --------------------------------------------------------------------
# UTILITIES
# --------------------------------------------------------------------
def read_file(path: str, default: str = "") -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()
    except Exception:
        return default


def write_file(path: str, data: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", errors="ignore") as f:
        f.write(str(data))


def job_paths(job_id: str) -> Dict[str, str]:
    job_path = os.path.join(JOB_DIR, job_id)
    return {
        "job": job_path,
        "status": os.path.join(job_path, "status"),
        "exit": os.path.join(job_path, "exit"),
        "stdout": os.path.join(job_path, "stdout.log"),
        "stderr": os.path.join(job_path, "stderr.log"),
        "tmux": os.path.join(job_path, "tmux_session"),  # used only on Linux
    }


def get_status(job_id: str) -> Dict[str, Any]:
    paths = job_paths(job_id)
    if not os.path.isdir(paths["job"]):
        return {"error": "job_not_found"}

    status = read_file(paths["status"], "unknown").strip()
    exit_val = read_file(paths["exit"], "").strip()
    stdout = read_file(paths["stdout"], "")
    stderr = read_file(paths["stderr"], "")

    rc: Optional[int] = None
    if exit_val.isdigit():
        rc = int(exit_val)

    return {
        "job_id": job_id,
        "status": status,
        "return_code": rc,
        "stdout": stdout,
        "stderr": stderr,
    }


# --------------------------------------------------------------------
# SECURITY HELPERS (IP + TOKEN + RATE LIMIT)
# --------------------------------------------------------------------
def _is_ip_allowed(ip: str) -> bool:
    # If no allowed_ips configured → allow all
    if not _ALLOWED_NETWORKS:
        return True

    try:
        ip_obj = ipaddress.ip_address(ip)
    except Exception:
        return False

    for net in _ALLOWED_NETWORKS:
        if ip_obj in net:
            return True
    return False


def _rate_limit_check(ip: str) -> None:
    if RATE_MAX_REQUESTS <= 0:
        return  # disabled

    now = time.time()
    with _rate_lock:
        window_start, count = _rate_state.get(ip, (now, 0))

        # If window has expired, reset
        if now - window_start > RATE_WINDOW_SECONDS:
            window_start, count = now, 0

        count += 1
        _rate_state[ip] = (window_start, count)

        if count > RATE_MAX_REQUESTS:
            raise HTTPException(
                status_code=429,
                detail="Rate limit exceeded",
            )


async def validate_security(request: Request) -> str:
    """
    Validates:
      - Source IP against allowed_ips
      - Rate limiting
      - Agent token (X-Agent-Token)
    Returns the client IP if all checks pass.
    """
    client_ip = request.client.host if request.client else "unknown"

    if not _is_ip_allowed(client_ip):
        raise HTTPException(status_code=403, detail="IP not allowed")

    _rate_limit_check(client_ip)

    # Token check
    if AGENT_TOKEN:
        provided = request.headers.get("X-Agent-Token")
        if not provided or provided != AGENT_TOKEN:
            raise HTTPException(status_code=403, detail="Invalid agent token")

    return client_ip


def check_command_blacklist(command: str) -> None:
    """
    Very simple substring-based blacklist.
    If any pattern appears in the command, reject.
    """
    for pattern in COMMAND_BLACKLIST:
        if pattern and pattern.lower() in command.lower():
            raise HTTPException(
                status_code=400,
                detail=f"Command contains forbidden pattern: {pattern}",
            )


# --------------------------------------------------------------------
# JOB MODEL
# --------------------------------------------------------------------
class JobRequest(BaseModel):
    job_id: Optional[str] = None
    forced_job_id: Optional[str] = None

    command: str
    run_as_user: Optional[str] = None  # used only on Linux
    fire_and_forget: bool = True
    skip_if_running: bool = True
    timeout_seconds: int = 86400
    env: Dict[str, str] = {}

    class Config:
        extra = "ignore"  # ignore older fields from previous versions


# --------------------------------------------------------------------
# LINUX EXECUTION (tmux)
# --------------------------------------------------------------------
def run_job_linux(job_id: str, payload: JobRequest) -> None:
    paths = job_paths(job_id)
    write_file(paths["status"], "running")

    stdout_file = paths["stdout"]
    stderr_file = paths["stderr"]
    exit_file = paths["exit"]
    tmux_file = paths["tmux"]

    # Build the inner shell logic
    inner_cmd = (
        f"{payload.command} > {stdout_file} 2> {stderr_file}; "
        f"echo $? > {exit_file}"
    )

    if payload.run_as_user:
        base_cmd = (
            f"su - {payload.run_as_user} -c "
            f"\"bash -lc '{inner_cmd}'\""
        )
    else:
        base_cmd = f"bash -lc '{inner_cmd}'"

    tmux_session = f"agent_{job_id.replace('-', '')}"
    write_file(tmux_file, tmux_session)

    tmux_cmd = f"tmux new-session -d -s {tmux_session} \"{base_cmd}\""
    subprocess.call(tmux_cmd, shell=True)

    # Poll for exit file
    while True:
        if os.path.exists(exit_file):
            write_file(paths["status"], "finished")
            return
        time.sleep(2)


# --------------------------------------------------------------------
# WINDOWS EXECUTION (detached cmd.exe)
# --------------------------------------------------------------------
def run_job_windows(job_id: str, payload: JobRequest) -> None:
    paths = job_paths(job_id)
    write_file(paths["status"], "running")

    stdout_file = paths["stdout"]
    stderr_file = paths["stderr"]
    exit_file = paths["exit"]

    # Note: run_as_user is not implemented on Windows in this agent
    if payload.run_as_user:
        warn = (
            f"run_as_user={payload.run_as_user} is not implemented "
            f"on Windows in this agent.\n"
        )
        write_file(stderr_file, warn)

    # Prepare the command for cmd.exe
    command = payload.command.replace('"', '\\"')
    cmd = (
        f'cmd.exe /c "{command} > \\"{stdout_file}\\" '
        f'2> \\"{stderr_file}\\" & echo %ERRORLEVEL% > \\"{exit_file}\\""'
    )

    DETACHED_PROCESS = 0x00000008
    CREATE_NEW_PROCESS_GROUP = 0x00000200

    subprocess.Popen(
        cmd,
        shell=True,
        creationflags=DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP,
    )

    while True:
        if os.path.exists(exit_file):
            write_file(paths["status"], "finished")
            return
        time.sleep(2)


# --------------------------------------------------------------------
# DISPATCHER
# --------------------------------------------------------------------
def run_job_background(job_id: str, payload: JobRequest) -> None:
    if IS_WINDOWS:
        run_job_windows(job_id, payload)
    else:
        run_job_linux(job_id, payload)


# --------------------------------------------------------------------
# CLEANUP (RETENTION)
# --------------------------------------------------------------------
def cleanup_old_jobs() -> None:
    now = datetime.utcnow()
    cutoff = now - timedelta(days=RETENTION_DAYS)
    print(f"[CLEANUP] cutoff={cutoff.isoformat()}")

    for job_id in os.listdir(JOB_DIR):
        paths = job_paths(job_id)
        job_path = paths["job"]

        if not os.path.isdir(job_path):
            continue

        try:
            ctime = datetime.utcfromtimestamp(os.path.getctime(job_path))
        except Exception:
            continue

        if ctime > cutoff:
            continue

        status = read_file(paths["status"], "running").strip()
        if status == "running":
            print(f"[CLEANUP] skip active job {job_id}")
            continue

        # Kill tmux session if exists (Linux)
        if not IS_WINDOWS:
            tmux_session = read_file(paths["tmux"], "").strip()
            if tmux_session:
                os.system(f"tmux kill-session -t {tmux_session} 2>/dev/null")

        # Remove job directory
        if IS_WINDOWS:
            os.system(f'rmdir /S /Q "{job_path}"')
        else:
            os.system(f'rm -rf "{job_path}"')

        print(f"[CLEANUP] deleted job {job_id}")


def cleanup_scheduler() -> None:
    while True:
        try:
            cleanup_old_jobs()
        except Exception as e:
            print(f"[CLEANUP] error: {e}")
        time.sleep(CLEANUP_INTERVAL_SECONDS)


# Start cleanup thread
threading.Thread(target=cleanup_scheduler, daemon=True).start()


# --------------------------------------------------------------------
# API ENDPOINTS
# --------------------------------------------------------------------
@app.post("/ping")
async def ping(request: Request):
    await validate_security(request)
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.post("/run")
async def run_job(request: Request, payload: JobRequest):
    await validate_security(request)

    # Blacklist check
    check_command_blacklist(payload.command)

    # Choose job_id priority: job_id > forced_job_id > new UUID
    job_id = payload.job_id or payload.forced_job_id or str(uuid.uuid4())
    paths = job_paths(job_id)

    # skip_if_running logic
    if payload.skip_if_running and os.path.exists(paths["status"]):
        current = read_file(paths["status"], "running").strip()
        if current == "running":
            return JSONResponse(
                {"job_id": job_id, "status": "already_running"},
                status_code=200,
            )

    # Ensure job directory exists
    os.makedirs(paths["job"], exist_ok=True)

    # fire-and-forget (most common mode with Airflow)
    if payload.fire_and_forget:
        t = threading.Thread(
            target=run_job_background, args=(job_id, payload), daemon=True
        )
        t.start()
        return JSONResponse(
            {"job_id": job_id, "status": "submitted"},
            status_code=200,
        )

    # Synchronous mode (less common)
    run_job_background(job_id, payload)
    return JSONResponse(get_status(job_id), status_code=200)


@app.get("/status/{job_id}")
async def job_status(request: Request, job_id: str):
    await validate_security(request)
    return JSONResponse(get_status(job_id), status_code=200)


# --------------------------------------------------------------------
# MAIN (TLS uvicorn)
# --------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "agent_unified:app",
        host=HOST,
        port=PORT,
        ssl_certfile=TLS_CERT,
        ssl_keyfile=TLS_KEY,
    )
