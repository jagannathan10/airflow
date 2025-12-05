#!/usr/bin/env python3
# ============================================================================
#    Universal Airflow Agent (TMUX-only, YAML config.xml enabled)
# ============================================================================

import os
import yaml
import time
import subprocess
import threading
from datetime import datetime, timedelta
from ipaddress import ip_address, ip_network

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ============================================================================
# PATHS
# ============================================================================
BASE_DIR = "/opt/airflow_agent"
CONFIG_FILE = f"{BASE_DIR}/config.xml"     # YAML-style config
JOB_DIR = f"{BASE_DIR}/jobs"
TMUX_BIN = "/usr/bin/tmux"

os.makedirs(JOB_DIR, exist_ok=True)

# ============================================================================
# GLOBAL CONFIG STATE (AUTO-RELOAD)
# ============================================================================
CONFIG = {}


def load_config():
    """Load YAML-style config.xml into CONFIG{} dictionary."""
    global CONFIG
    try:
        with open(CONFIG_FILE, "r") as f:
            CONFIG = yaml.safe_load(f)
        print(f"[AGENT] config.xml loaded")
    except Exception as e:
        print(f"[AGENT] ERROR: Failed to load config.xml → {e}")


# Initial load
load_config()


def config_reloader():
    """Reload config.xml every 30 seconds, automatically."""
    last_mtime = None
    while True:
        try:
            mtime = os.path.getmtime(CONFIG_FILE)
            if last_mtime is None or mtime != last_mtime:
                last_mtime = mtime
                load_config()
        except:
            pass
        time.sleep(30)


threading.Thread(target=config_reloader, daemon=True).start()

# ============================================================================
# FASTAPI
# ============================================================================
app = FastAPI(title="Universal Airflow Agent")

# ============================================================================
# HELPERS
# ============================================================================

def read_file(path, default=""):
    try:
        return open(path).read()
    except:
        return default


def write_file(path, content):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(str(content))


# ----------------------------------------------------------------------------
# IP Access Control
# ----------------------------------------------------------------------------
def ip_allowed(ip):
    allowed_list = CONFIG.get("allowed_ips", [])
    if not allowed_list:
        return True  # no restriction

    try:
        client_ip = ip_address(ip)
    except:
        return False

    for rule in allowed_list:
        try:
            net = ip_network(rule, strict=False)
            if client_ip in net:
                return True
        except:
            pass

    return False


# ----------------------------------------------------------------------------
# Command blacklist
# ----------------------------------------------------------------------------
def check_blacklist(cmd):
    for bad in CONFIG.get("command_blacklist", []):
        if bad in cmd:
            return False
    return True


# ----------------------------------------------------------------------------
# Token validation
# ----------------------------------------------------------------------------
async def validate_token(request: Request):
    expected = CONFIG.get("token")
    if not expected:
        return  # disabled

    provided = request.headers.get("X-Agent-Token")
    if provided != expected:
        return JSONResponse({"error": "invalid_token"}, status_code=403)

    return None


# ----------------------------------------------------------------------------
# Rate limiting
# ----------------------------------------------------------------------------
RATE_BUCKET = {}   # ip → list[timestamps]


def check_rate_limit(ip):
    rl = CONFIG.get("rate_limit", {})
    window = rl.get("window_seconds", 60)
    maxreq = rl.get("max_requests", 120)

    now = time.time()
    bucket = RATE_BUCKET.setdefault(ip, [])

    # purge old timestamps
    bucket[:] = [t for t in bucket if t > now - window]

    if len(bucket) >= maxreq:
        return False

    bucket.append(now)
    return True


# ============================================================================
# REQUEST MODEL
# ============================================================================
class JobRequest(BaseModel):
    command: str
    run_as_user: str | None = None
    job_id: str
    skip_if_running: bool = True
    fire_and_forget: bool = False
    env: dict = {}
    use_tmux: bool = True


# ============================================================================
# TMUX Helpers
# ============================================================================

def tmux_session(job_id):
    return f"agent_{job_id}"


def tmux_alive(job_id):
    return (
        subprocess.call(
            [TMUX_BIN, "has-session", "-t", tmux_session(job_id)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        == 0
    )


# ============================================================================
# JOB EXECUTION
# ============================================================================

def build_script(job_id, payload: JobRequest):
    job_path = f"{JOB_DIR}/{job_id}"
    os.makedirs(job_path, exist_ok=True)

    stdout = f"{job_path}/stdout.log"
    stderr = f"{job_path}/stderr.log"
    exit_file = f"{job_path}/exit"
    status_file = f"{job_path}/status"

    write_file(status_file, "starting")

    if payload.run_as_user:
        safe = payload.command.replace("'", "'\"'\"'")
        cmd = f"su - {payload.run_as_user} -c '{safe}'"
    else:
        cmd = payload.command

    script_file = f"{job_path}/run.sh"
    with open(script_file, "w") as f:
        f.write(
            f"""#!/bin/bash
set -o pipefail
{cmd} >> "{stdout}" 2>> "{stderr}"
echo $? > "{exit_file}"
echo finished > "{status_file}"
"""
        )
    os.chmod(script_file, 0o755)
    return script_file


def launch_tmux(job_id, payload):
    """Start TMUX job."""
    script = build_script(job_id, payload)
    session = tmux_session(job_id)

    subprocess.call([
        TMUX_BIN, "new-session", "-d",
        "-s", session,
        "bash", "-lc", script
    ])

    write_file(f"{JOB_DIR}/{job_id}/status", "running")


# ============================================================================
# ENDPOINTS
# ============================================================================

@app.post("/ping")
async def ping(request: Request):
    await validate_token(request)

    client_ip = request.client.host
    if not ip_allowed(client_ip):
        return JSONResponse({"error": "ip_not_allowed"}, status_code=403)

    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.post("/run")
async def run(request: Request, payload: JobRequest):
    await validate_token(request)

    client_ip = request.client.host

    if not ip_allowed(client_ip):
        return JSONResponse({"error": "ip_not_allowed"}, status_code=403)

    if not check_rate_limit(client_ip):
        return JSONResponse({"error": "rate_limited"}, status_code=429)

    if not check_blacklist(payload.command):
        return JSONResponse({"error": "command_blocked"}, status_code=400)

    job_id = payload.job_id

    # Dedup support
    if payload.skip_if_running and tmux_alive(job_id):
        return {"job_id": job_id, "status": "already_running"}

    launch_tmux(job_id, payload)
    return {"job_id": job_id, "status": "submitted"}


@app.get("/status/{job_id}")
async def status(request: Request, job_id: str):
    await validate_token(request)

    job_path = f"{JOB_DIR}/{job_id}"
    status_file = f"{job_path}/status"
    exit_file = f"{job_path}/exit"
    stdout_file = f"{job_path}/stdout.log"
    stderr_file = f"{job_path}/stderr.log"

    stat = read_file(status_file, "unknown").strip()
    exit_code = read_file(exit_file, "").strip()
    try:
        exit_code = int(exit_code) if exit_code else None
    except:
        exit_code = None

    return {
        "job_id": job_id,
        "status": stat,
        "return_code": exit_code,
        "stdout": read_file(stdout_file, ""),
        "stderr": read_file(stderr_file, ""),
    }


@app.post("/cancel/{job_id}")
async def cancel(request: Request, job_id: str):
    await validate_token(request)

    session = tmux_session(job_id)
    subprocess.call([TMUX_BIN, "kill-session", "-t", session])

    write_file(f"{JOB_DIR}/{job_id}/status", "cancelled")
    return {"job_id": job_id, "status": "cancelled"}


# ============================================================================
# RETENTION CLEANUP
# ============================================================================
def cleanup_loop():
    while True:
        try:
            retention = CONFIG.get("retention_days", 60)
            cutoff = datetime.utcnow() - timedelta(days=retention)

            for job_id in os.listdir(JOB_DIR):
                p = f"{JOB_DIR}/{job_id}"
                try:
                    ctime = datetime.utcfromtimestamp(os.path.getctime(p))
                except:
                    continue

                if ctime < cutoff:
                    subprocess.call([TMUX_BIN, "kill-session", "-t", tmux_session(job_id)])
                    subprocess.call(["rm", "-rf", p])
                    print(f"[CLEANUP] Removed {job_id}")
        except Exception as e:
            print(f"[CLEANUP] error: {e}")

        time.sleep(3600)  # run hourly


threading.Thread(target=cleanup_loop, daemon=True).start()


# ============================================================================
# STARTUP
# ============================================================================
@app.on_event("startup")
def startup():
    print("[AGENT] Started with TMUX-only unified mode")
    print(f"[AGENT] BASE_DIR={BASE_DIR}")
    print(f"[AGENT] Jobs stored in {JOB_DIR}")
